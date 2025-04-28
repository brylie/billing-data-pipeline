import os
import re
import urllib.parse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import s3fs
from dagster import ConfigurableResource, get_dagster_logger
from dagster_duckdb import DuckDBResource

from .utils import get_env


class S3HiveResource(ConfigurableResource):
    """Resource for working with Hive-partitioned S3 data."""

    def _parse_url(self, url: str) -> Tuple[str, str, str]:
        """
        Parse a URL into its components.

        Args:
            url: The URL to parse

        Returns:
            Tuple of (protocol, domain, path)
        """
        if not url:
            return ("https", "", "")

        # Handle URLs without protocol
        if not url.startswith(("http://", "https://", "s3://")):
            url = f"https://{url}"

        parsed = urllib.parse.urlparse(url)
        protocol = parsed.scheme or "https"
        domain = parsed.netloc
        path = parsed.path.lstrip("/")

        return (protocol, domain, path)

    def get_s3fs(self):
        """Get an S3 filesystem client."""
        # Using anonymous access as the bucket appears to be public
        # If authentication is required, add aws_access_key_id and aws_secret_access_key
        return s3fs.S3FileSystem(anon=True)

    def parse_hive_path(self, path: str) -> Dict[str, str]:
        """
        Parse a Hive-partitioned path into its components.

        Example:
            "year=2025/month=04/day=23/billing.csv" -> {'year': '2025', 'month': '04', 'day': '23'}
        """
        parts = {}
        # Match patterns like key=value in the path
        for part in path.split("/"):
            match = re.match(r"([^=]+)=([^=]+)", part)
            if match:
                key, value = match.groups()
                parts[key] = value
        return parts

    def list_hive_partitions(
        self, bucket_url: str, from_date: Optional[datetime] = None
    ) -> List[Dict]:
        """
        List available partitions in the Hive structure.

        Args:
            bucket_url: Base URL for the S3 bucket
            from_date: Optional start date for listing partitions (for backfilling)

        Returns:
            List of dictionaries with partition information
        """
        logger = get_dagster_logger()

        # Parse the URL into components
        protocol, domain, base_path = self._parse_url(bucket_url)

        # Convert the bucket URL to a format s3fs can understand
        # s3fs expects paths without protocol
        bucket_path = f"{domain}/{base_path}".rstrip("/")

        logger.info(f"Listing partitions in: {bucket_path}")

        fs = self.get_s3fs()

        try:
            # List all directories at the bucket path
            all_paths = []

            # If no from_date specified, list all partitions
            if from_date is None:
                # List all year directories
                year_dirs = fs.ls(bucket_path)

                for year_dir in year_dirs:
                    if "year=" in year_dir:
                        # List all month directories for this year
                        month_dirs = fs.ls(year_dir)

                        for month_dir in month_dirs:
                            if "month=" in month_dir:
                                # List all day directories for this month
                                day_dirs = fs.ls(month_dir)

                                for day_dir in day_dirs:
                                    if "day=" in day_dir:
                                        # Add the day directory to our list
                                        all_paths.append(day_dir)
            else:
                # For backfilling, start from the from_date and get all partitions
                current_date = from_date
                today = datetime.now()

                while current_date <= today:
                    year = current_date.strftime("%Y")
                    month = current_date.strftime("%m")
                    day = current_date.strftime("%d")

                    # Construct the path for this date
                    path = f"{bucket_path}/year={year}/month={month}/day={day}"

                    # Check if this partition exists
                    if fs.exists(path):
                        all_paths.append(path)

                    # Move to the next day
                    current_date += timedelta(days=1)

            partitions = []
            for path in all_paths:
                # Extract partition information
                partition_info = self.parse_hive_path(path)
                if (
                    partition_info
                    and "year" in partition_info
                    and "month" in partition_info
                    and "day" in partition_info
                ):
                    # Check if the partition has billing files
                    try:
                        files = fs.ls(path)
                        csv_files = [f for f in files if f.endswith(".csv")]
                        if csv_files:
                            partition_info["path"] = path
                            partition_info["files"] = csv_files
                            partitions.append(partition_info)
                    except Exception as e:
                        logger.error(f"Error listing files in partition {path}: {e}")

            return partitions

        except Exception as e:
            logger.error(f"Error listing hive partitions: {e}")
            return []

    def download_partition(self, partition: Dict, local_dir: str) -> List[str]:
        """
        Download files from a partition to a local directory.

        Args:
            partition: Partition dictionary with 'path' and 'files' keys
            local_dir: Local directory to download files to

        Returns:
            List of downloaded file paths
        """
        logger = get_dagster_logger()
        bucket_url = get_env("S3_BUCKET_URL")
        protocol, domain, base_path = self._parse_url(bucket_url)

        # Create the local directory if it doesn't exist
        os.makedirs(local_dir, exist_ok=True)

        downloaded_files = []

        # Download each file in the partition
        for s3_file_path in partition.get("files", []):
            try:
                # Extract filename from S3 path
                filename = os.path.basename(s3_file_path)

                # Create a more descriptive local filename that includes partition info
                year = partition.get("year", "unknown")
                month = partition.get("month", "unknown")
                day = partition.get("day", "unknown")
                local_filename = f"billing-{year}-{month}-{day}-{filename}"
                local_path = os.path.join(local_dir, local_filename)

                # Try to download with s3fs first
                try:
                    logger.info(
                        f"Downloading {s3_file_path} to {local_path} using s3fs"
                    )
                    fs = self.get_s3fs()
                    fs.get(s3_file_path, local_path)
                except Exception as s3_err:
                    # If s3fs fails, try direct HTTP download as fallback for public URLs
                    logger.warning(
                        f"S3fs download failed: {s3_err}. Trying direct HTTP download."
                    )

                    # Reconstruct the full HTTP URL using parsed components
                    if s3_file_path.startswith(("http://", "https://")):
                        full_url = s3_file_path
                    else:
                        # Handle both s3:// style paths and plain paths
                        clean_path = s3_file_path.replace("s3://", "").lstrip("/")

                        # Check if clean_path already includes the domain
                        if domain in clean_path:
                            # The path already includes the domain, just add the protocol
                            full_url = f"{protocol}://{clean_path}"
                        else:
                            # Extract just the relative path, removing any domain if present
                            parts = clean_path.split("/", 1)
                            path_part = (
                                parts[1]
                                if len(parts) > 1 and domain in parts[0]
                                else clean_path
                            )

                            # Construct the full URL using components from the bucket URL
                            full_url = f"{protocol}://{domain}/{path_part}"

                    logger.info(f"Attempting direct HTTP download from: {full_url}")

                    import requests

                    response = requests.get(full_url, stream=True)
                    response.raise_for_status()  # Raise an exception for HTTP errors

                    with open(local_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)

                    logger.info(f"Successfully downloaded from URL: {full_url}")

                downloaded_files.append(local_path)

            except Exception as e:
                logger.error(f"Error downloading file {s3_file_path}: {e}")

        return downloaded_files

    def get_latest_partition_date(
        self, bucket_url: Optional[str] = None
    ) -> Optional[datetime]:
        """
        Get the most recent partition date available in the S3 bucket.

        Args:
            bucket_url: URL for the S3 bucket. If None, get from environment variable.

        Returns:
            datetime object representing the latest partition date, or None if not found
        """
        if bucket_url is None:
            bucket_url = get_env("S3_BUCKET_URL")

        partitions = self.list_hive_partitions(bucket_url)

        if not partitions:
            return None

        # Find the most recent date from partitions
        latest_date = None
        for partition in partitions:
            try:
                year = int(partition.get("year", 0))
                month = int(partition.get("month", 0))
                day = int(partition.get("day", 0))

                date = datetime(year, month, day)

                if latest_date is None or date > latest_date:
                    latest_date = date
            except (ValueError, TypeError):
                # Skip invalid dates
                pass

        return latest_date

    def read_csv_from_s3(self, file_path: str, duckdb_resource: DuckDBResource = None):
        """
        Read a CSV file directly from S3 using DuckDB.

        Args:
            file_path: S3 path to the CSV file
            duckdb_resource: Optional DuckDBResource for the connection

        Returns:
            DuckDB result object or a list of dictionaries representing the data
        """
        logger = get_dagster_logger()
        fs = self.get_s3fs()

        try:
            # If a DuckDBResource is provided, use it
            if duckdb_resource:
                with duckdb_resource.get_connection() as conn:
                    # Read the CSV file directly from S3 using DuckDB's httpfs extension
                    conn.execute("INSTALL httpfs; LOAD httpfs;")
                    result = conn.execute(
                        f"SELECT * FROM read_csv_auto('s3://{file_path}')"
                    )
                    return result
            else:
                # Download the file to a temporary location and read it with DuckDB
                import tempfile

                import duckdb

                with tempfile.NamedTemporaryFile(suffix=".csv") as tmp:
                    # Download file from S3 to temp file
                    fs.get(file_path, tmp.name)

                    # Use DuckDB to read the file
                    con = duckdb.connect(":memory:")
                    result = con.execute(f"SELECT * FROM read_csv_auto('{tmp.name}')")
                    return result

        except Exception as e:
            logger.error(f"Error reading CSV from S3 ({file_path}): {e}")
            return None

    def generate_partition_paths(
        self,
        bucket_url: str,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
    ) -> List[Dict]:
        """
        Generate partition paths based on date range without listing the bucket.

        Args:
            bucket_url: Base URL for the S3 bucket
            from_date: Optional start date for partitions
            to_date: Optional end date for partitions, defaults to current date

        Returns:
            List of dictionaries with partition information
        """
        logger = get_dagster_logger()

        # Parse the URL into components
        protocol, domain, base_path = self._parse_url(bucket_url)

        # Set default dates if not provided
        if to_date is None:
            to_date = datetime.now()
            logger.info(
                f"No to_date provided, using current date: {to_date.strftime('%Y-%m-%d')}"
            )

        if from_date is None:
            # Default to yesterday if no from_date is provided
            from_date = to_date - timedelta(days=1)
            logger.info(
                f"No from_date provided, using yesterday: {from_date.strftime('%Y-%m-%d')}"
            )

        logger.info(
            f"Generating partition paths from {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}"
        )

        partitions = []
        current_date = from_date

        while current_date <= to_date:
            year = current_date.strftime("%Y")
            month = current_date.strftime("%m")
            day = current_date.strftime("%d")

            # Construct the relative path for this date using the base path if any
            relative_path = (
                f"{base_path.rstrip('/')}/year={year}/month={month}/day={day}".lstrip(
                    "/"
                )
            )

            # Full URL path for HTTP access
            full_url_path = f"{protocol}://{domain}/{relative_path}"

            # S3 path for s3fs access (without protocol)
            s3_path = f"{domain}/{relative_path}"

            # Construct the CSV file path
            csv_file_name = "billing.csv"
            csv_http_path = f"{full_url_path}/{csv_file_name}"
            csv_s3_path = f"{s3_path}/{csv_file_name}"

            # Create partition info - using s3 path for files for consistency with s3fs
            partition_info = {
                "year": year,
                "month": month,
                "day": day,
                "path": s3_path,
                "files": [csv_s3_path],
            }

            partitions.append(partition_info)
            logger.info(f"Generated partition path: {csv_http_path}")

            # Move to the next day
            current_date += timedelta(days=1)

        return partitions
