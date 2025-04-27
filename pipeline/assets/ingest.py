import glob
import hashlib
import os
from datetime import datetime

from dagster import AssetExecutionContext, MetadataValue, asset
from dagster_duckdb import DuckDBResource

from ..utils import get_table_preview


@asset
def billing_files(context: AssetExecutionContext):
    """Asset that tracks available billing CSV files."""
    # In a real implementation, you'd connect to S3 here
    # For now, we'll use local files in the data/raw directory
    local_path = "data/raw/"
    files = glob.glob(f"{local_path}/*.csv")

    # Get file stats for metadata
    file_stats = {}
    total_size = 0
    for file in files:
        size = os.path.getsize(file)
        total_size += size
        file_stats[os.path.basename(file)] = size

    context.log.info(f"Found {len(files)} billing files")

    # Add metadata for Dagster UI
    context.add_output_metadata(
        {
            "num_files": len(files),
            "total_size_bytes": total_size,
            "files": MetadataValue.json(file_stats),
        }
    )

    return {"files": files, "path": local_path}


@asset(deps=["billing_files"])
def billing_db(context: AssetExecutionContext, duckdb: DuckDBResource):
    """Asset that creates a DuckDB database from the billing files, maintaining history."""
    db_path = "data/billing.duckdb"

    with duckdb.get_connection() as conn:
        try:
            # First, create tables if they don't exist

            # Create a table to track processed files
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_files (
                    filename VARCHAR,
                    file_hash VARCHAR,
                    processed_at TIMESTAMP,
                    record_count INTEGER,
                    PRIMARY KEY (filename)
                )
            """)

            # Create raw_billing table if it doesn't exist (based on actual CSV structure)
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
                    -- Create a unique constraint using multiple columns since there's no transaction_id
                    UNIQUE (timestamp, resource_id, user_id, invoice_id)
                )
            """)

            # Get all available files
            local_path = "data/raw/"
            files = glob.glob(f"{local_path}/*.csv")

            # Process each file idempotently
            total_new_records = 0
            new_files_processed = 0

            for file_path in files:
                filename = os.path.basename(file_path)

                # Calculate file hash to detect changes
                file_hash = ""
                with open(file_path, "rb") as f:
                    file_hash = hashlib.md5(f.read()).hexdigest()

                # Check if this file with this hash has been processed before
                result = conn.execute(
                    "SELECT file_hash FROM processed_files WHERE filename = ?",
                    [filename],
                ).fetchone()

                # Skip if file already processed and hash matches
                if result and result[0] == file_hash:
                    context.log.info(f"Skipping already processed file: {filename}")
                    continue

                # If file changed or is new, process it
                context.log.info(f"Processing file: {filename}")

                # Create a temporary table for the new data
                conn.execute(f"""
                    CREATE TEMPORARY TABLE temp_raw_billing AS
                    SELECT * FROM read_csv_auto('{file_path}', header=true)
                """)

                # Get count of new records
                new_record_count = conn.execute(
                    "SELECT COUNT(*) FROM temp_raw_billing"
                ).fetchone()[0]

                # Insert data, avoiding duplicates based on the composite key
                # Using a more robust approach that doesn't rely on specific column names
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
