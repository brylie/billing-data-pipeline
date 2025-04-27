import glob
import hashlib
import os
from datetime import datetime

from dagster import AssetExecutionContext, MetadataValue, asset
from dagster_duckdb import DuckDBResource

from ..s3_utils import S3HiveResource
from ..utils import get_env, get_table_preview


@asset
def billing_files(context: AssetExecutionContext, s3_hive: S3HiveResource):
    """
    Asset that fetches billing CSV files from S3 with Hive partitioning.

    The S3 bucket follows the pattern:
    s3://bucket/year=YYYY/month=MM/day=DD/billing.csv
    """
    local_path = "data/raw/"
    os.makedirs(local_path, exist_ok=True)

    # Get S3 bucket URL from environment variables
    bucket_url = get_env("S3_BUCKET_URL")
    context.log.info(f"Fetching billing files from {bucket_url}")

    # Get the latest processed date from the run context
    # This enables backfilling if needed
    from_date_str = context.run_config.get("from_date")
    to_date_str = context.run_config.get("to_date")
    from_date = None
    to_date = None

    if from_date_str:
        try:
            from_date = datetime.strptime(from_date_str, "%Y-%m-%d")
            context.log.info(f"Backfilling data from {from_date}")
        except ValueError:
            context.log.error(
                f"Invalid from_date format: {from_date_str}, expected YYYY-MM-DD"
            )

    if to_date_str:
        try:
            to_date = datetime.strptime(to_date_str, "%Y-%m-%d")
            context.log.info(f"Backfilling data to {to_date}")
        except ValueError:
            context.log.error(
                f"Invalid to_date format: {to_date_str}, expected YYYY-MM-DD"
            )

    # Generate partition paths based on date range
    # This avoids listing S3 bucket contents which may have permission issues
    partitions = s3_hive.generate_partition_paths(bucket_url, from_date, to_date)
    context.log.info(f"Generated {len(partitions)} partition paths")

    # Download files from each partition
    all_files = []
    file_stats = {}
    total_size = 0
    successful_downloads = 0

    for partition in partitions:
        year = partition.get("year")
        month = partition.get("month")
        day = partition.get("day")

        context.log.info(f"Processing partition: year={year}, month={month}, day={day}")

        try:
            # Download files from this partition
            downloaded_files = s3_hive.download_partition(partition, local_path)

            if downloaded_files:
                all_files.extend(downloaded_files)
                successful_downloads += 1

                # Get file stats for metadata
                for file_path in downloaded_files:
                    if os.path.exists(file_path):
                        size = os.path.getsize(file_path)
                        total_size += size
                        file_stats[os.path.basename(file_path)] = size
            else:
                context.log.warning(
                    f"No files downloaded for partition year={year}, month={month}, day={day}"
                )
        except Exception as e:
            context.log.error(
                f"Error processing partition year={year}, month={month}, day={day}: {e}"
            )
            # Continue with next partition even if one fails

    context.log.info(
        f"Successfully downloaded {len(all_files)} billing files from {successful_downloads} partitions"
    )

    # Add metadata for Dagster UI
    context.add_output_metadata(
        {
            "num_files": len(all_files),
            "successful_partitions": successful_downloads,
            "total_partitions": len(partitions),
            "total_size_bytes": total_size,
            "files": MetadataValue.json(file_stats),
            "source": f"S3: {bucket_url}",
        }
    )

    return {"files": all_files, "path": local_path}


@asset(deps=["billing_files"])
def billing_db(context: AssetExecutionContext, duckdb: DuckDBResource):
    """Asset that creates a DuckDB database from the billing files downloaded from S3."""
    db_path = "data/billing.duckdb"

    with duckdb.get_connection() as conn:
        try:
            # First, create tables if they don't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_files (
                    filename VARCHAR,
                    file_hash VARCHAR,
                    processed_at TIMESTAMP,
                    record_count INTEGER,
                    PRIMARY KEY (filename)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_billing (
                    timestamp TIMESTAMP, 
                    resource_id VARCHAR,
                    user_id VARCHAR,
                    credit_usage DOUBLE,
                    region VARCHAR,
                    service_tier VARCHAR,
                    operation_type VARCHAR,
                    success BOOLEAN,
                    resource_type VARCHAR,
                    invoice_id VARCHAR,
                    currency VARCHAR,
                    year INTEGER,
                    month INTEGER,
                    day INTEGER,
                    UNIQUE (timestamp, resource_id, user_id, invoice_id)
                )
            """)

            # Get files downloaded from S3 by the billing_files asset
            local_path = "data/raw/"
            files = glob.glob(
                f"{local_path}/billing-*.csv"
            )  # Only process billing files from S3

            # Process each file idempotently
            total_new_records = 0
            new_files_processed = 0

            for file_path in files:
                filename = os.path.basename(file_path)

                # Calculate file hash to detect changes
                file_hash = ""
                with open(file_path, "rb") as f:
                    file_hash = hashlib.md5(f.read()).hexdigest()

                # Check if this file has been processed before
                result = conn.execute(
                    "SELECT file_hash FROM processed_files WHERE filename = ?",
                    [filename],
                ).fetchone()

                # Skip if file already processed and hash matches
                if result and result[0] == file_hash:
                    context.log.info(f"Skipping already processed file: {filename}")
                    continue

                # Process new or changed file
                context.log.info(f"Processing file: {filename}")

                # Use DuckDB to process the file directly
                conn.execute(f"""
                    CREATE TEMPORARY TABLE temp_raw_billing AS
                    SELECT * FROM read_csv_auto('{file_path}', header=true)
                """)

                # Get count of new records
                new_record_count = conn.execute(
                    "SELECT COUNT(*) FROM temp_raw_billing"
                ).fetchone()[0]

                # Insert data, avoiding duplicates
                conn.execute("""
                    INSERT INTO raw_billing
                    SELECT t.* FROM temp_raw_billing t
                    LEFT JOIN raw_billing r ON 
                        t.timestamp = r.timestamp AND
                        t.resource_id = r.resource_id AND
                        t.user_id = r.user_id AND
                        t.invoice_id = r.invoice_id
                    WHERE r.resource_id IS NULL
                """)

                # Record this file as processed
                conn.execute(
                    """
                    INSERT OR REPLACE INTO processed_files (filename, file_hash, processed_at, record_count)
                    VALUES (?, ?, ?, ?)
                """,
                    [filename, file_hash, datetime.now(), new_record_count],
                )

                # Drop temp table
                conn.execute("DROP TABLE temp_raw_billing")

                total_new_records += new_record_count
                new_files_processed += 1

            # Get total record count for metadata
            result = conn.execute("SELECT COUNT(*) FROM raw_billing").fetchone()
            total_record_count = result[0] if result else 0

            context.log.info(
                f"Processed {new_files_processed} new or changed files with {total_new_records} records"
            )
            context.log.info(f"Total records in database: {total_record_count}")

            # Add metadata
            context.add_output_metadata(
                {
                    "new_files_processed": new_files_processed,
                    "new_records_added": total_new_records,
                    "total_record_count": total_record_count,
                    "database_path": db_path,
                    "table_preview": MetadataValue.md(get_table_preview(conn)),
                }
            )

        except Exception as e:
            context.log.error(f"Error loading data into DuckDB: {e}")
            raise

    return {"db_path": db_path}
