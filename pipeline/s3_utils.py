import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import s3fs
from dagster import ConfigurableResource, get_dagster_logger
from dagster_duckdb import DuckDBResource

from .utils import get_env


class S3HiveResource(ConfigurableResource):
    """Resource for working with Hive-partitioned S3 data."""

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

        fs = self.get_s3fs()
        bucket_path = bucket_url.replace("https://", "")

        logger.info(f"Listing partitions in: {bucket_path}")

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
        fs = self.get_s3fs()

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

                logger.info(f"Downloading {s3_file_path} to {local_path}")
                fs.get(s3_file_path, local_path)
                downloaded_files.append(local_path)

            except Exception as e:
                logger.error(f"Error downloading file {s3_file_path}: {e}")

        return downloaded_files

    def get_latest_partition_date(self) -> Optional[datetime]:
        """
        Get the most recent partition date available in the S3 bucket.

        Returns:
            datetime object representing the latest partition date, or None if not found
        """
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
        self, bucket_url: str, from_date: Optional[datetime] = None, to_date: Optional[datetime] = None
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
        
        # Set default dates if not provided
        if to_date is None:
            to_date = datetime.now()
            
        if from_date is None:
            # Default to yesterday if no from_date is provided
            from_date = to_date - timedelta(days=1)
        
        logger.info(f"Generating partition paths from {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
        
        partitions = []
        current_date = from_date
        
        while current_date <= to_date:
            year = current_date.strftime("%Y")
            month = current_date.strftime("%m")
            day = current_date.strftime("%d")
            
            # Construct the path for this date
            path = f"{bucket_url.rstrip('/')}/year={year}/month={month}/day={day}"
            
            # Construct the CSV file path
            csv_file_path = f"{path}/billing.csv"
            
            # Create partition info
            partition_info = {
                "year": year,
                "month": month,
                "day": day,
                "path": path.replace("https://", ""),
                "files": [csv_file_path.replace("https://", "")]
            }
            
            partitions.append(partition_info)
            logger.info(f"Generated partition path: {csv_file_path}")
            
            # Move to the next day
            current_date += timedelta(days=1)
            
        return partitions
