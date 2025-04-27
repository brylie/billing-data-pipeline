import glob
import hashlib
import os
from datetime import datetime

from dagster import AssetExecutionContext, MetadataValue, asset
from dagster_duckdb import DuckDBResource


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


@asset(deps=["billing_db"])
def daily_aggregates(context: AssetExecutionContext, duckdb: DuckDBResource):
    """Asset that creates daily aggregated metrics."""
    # Use the DuckDBResource to get a connection
    with duckdb.get_connection() as conn:
        try:
            # Create daily aggregates table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_aggs (
                    year INTEGER,
                    month INTEGER,
                    day INTEGER,
                    transaction_count INTEGER,
                    total_credit_usage DOUBLE,
                    avg_credit_usage DOUBLE,
                    unique_users INTEGER,
                    unique_resources INTEGER,
                    successful_operations INTEGER,
                    failed_operations INTEGER,
                    PRIMARY KEY (year, month, day)
                )
            """)

            # First clear the existing aggregates as we'll recalculate them
            conn.execute("DELETE FROM daily_aggs")

            # Then recreate aggregates from raw data
            conn.execute("""
                INSERT INTO daily_aggs
                SELECT 
                    year, month, day,
                    COUNT(*) as transaction_count,
                    SUM(credit_usage) as total_credit_usage,
                    AVG(credit_usage) as avg_credit_usage,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(DISTINCT resource_id) as unique_resources,
                    SUM(CASE WHEN success = true THEN 1 ELSE 0 END) as successful_operations,
                    SUM(CASE WHEN success = false THEN 1 ELSE 0 END) as failed_operations
                FROM raw_billing
                GROUP BY year, month, day
                ORDER BY year, month, day
            """)

            # Get record count for metadata
            result = conn.execute("SELECT COUNT(*) FROM daily_aggs").fetchone()
            agg_count = result[0] if result else 0

            context.log.info(f"Created {agg_count} daily aggregates")

            # Add metadata with preview
            context.add_output_metadata(
                {
                    "aggregation_count": agg_count,
                    "aggregates_preview": MetadataValue.md(
                        get_table_preview(conn, "daily_aggs")
                    ),
                }
            )

        except Exception as e:
            context.log.error(f"Error creating daily aggregates: {e}")
            raise

    return {"aggregation": "daily", "count": agg_count}


@asset(deps=["billing_db"])
def user_aggregates(context: AssetExecutionContext, duckdb: DuckDBResource):
    """Asset that creates user-specific aggregated metrics."""
    # Use the DuckDBResource to get a connection
    with duckdb.get_connection() as conn:
        try:
            # Create user aggregates table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_aggs (
                    user_id VARCHAR PRIMARY KEY,
                    transaction_count INTEGER,
                    total_credit_usage DOUBLE,
                    avg_credit_usage DOUBLE,
                    resources_used INTEGER,
                    resource_types_used INTEGER,
                    operation_types INTEGER,
                    regions_used INTEGER,
                    first_activity TIMESTAMP,
                    last_activity TIMESTAMP
                )
            """)

            # Clear and regenerate user aggregates from raw data
            conn.execute("DELETE FROM user_aggs")

            # Create user aggregates based on actual schema
            conn.execute("""
                INSERT INTO user_aggs
                SELECT 
                    user_id,
                    COUNT(*) as transaction_count,
                    SUM(credit_usage) as total_credit_usage,
                    AVG(credit_usage) as avg_credit_usage,
                    COUNT(DISTINCT resource_id) as resources_used,
                    COUNT(DISTINCT resource_type) as resource_types_used,
                    COUNT(DISTINCT operation_type) as operation_types,
                    COUNT(DISTINCT region) as regions_used,
                    MIN(timestamp) as first_activity,
                    MAX(timestamp) as last_activity
                FROM raw_billing
                GROUP BY user_id
                ORDER BY total_credit_usage DESC
            """)

            # Get record count for metadata
            result = conn.execute("SELECT COUNT(*) FROM user_aggs").fetchone()
            user_count = result[0] if result else 0

            context.log.info(f"Created aggregates for {user_count} users")

            # Add metadata with preview
            context.add_output_metadata(
                {
                    "user_count": user_count,
                    "top_users_preview": MetadataValue.md(
                        get_table_preview(conn, "user_aggs", limit=10)
                    ),
                }
            )

        except Exception as e:
            context.log.error(f"Error creating user aggregates: {e}")
            raise

    return {"aggregation": "user", "count": user_count}


@asset(deps=["billing_db"])
def service_aggregates(context: AssetExecutionContext, duckdb: DuckDBResource):
    """Asset that creates service-specific aggregated metrics."""
    # Use the DuckDBResource to get a connection
    with duckdb.get_connection() as conn:
        try:
            # Create service_aggs table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS service_aggs (
                    service_tier VARCHAR,
                    resource_type VARCHAR,
                    operation_type VARCHAR,
                    operation_count INTEGER,
                    total_credit_usage DOUBLE,
                    avg_credit_usage DOUBLE,
                    unique_users INTEGER,
                    successful_operations INTEGER,
                    failed_operations INTEGER,
                    PRIMARY KEY (service_tier, resource_type, operation_type)
                )
            """)

            # Clear and regenerate service aggregates
            conn.execute("DELETE FROM service_aggs")

            # Create service tier and resource type aggregates
            conn.execute("""
                INSERT INTO service_aggs
                SELECT 
                    service_tier,
                    resource_type,
                    operation_type,
                    COUNT(*) as operation_count,
                    SUM(credit_usage) as total_credit_usage,
                    AVG(credit_usage) as avg_credit_usage,
                    COUNT(DISTINCT user_id) as unique_users,
                    SUM(CASE WHEN success = true THEN 1 ELSE 0 END) as successful_operations,
                    SUM(CASE WHEN success = false THEN 1 ELSE 0 END) as failed_operations
                FROM raw_billing
                GROUP BY service_tier, resource_type, operation_type
                ORDER BY service_tier, resource_type, operation_type
            """)

            # Get record count for metadata
            result = conn.execute("SELECT COUNT(*) FROM service_aggs").fetchone()
            service_count = result[0] if result else 0

            context.log.info(f"Created {service_count} service tier aggregates")

            # Add metadata with preview
            context.add_output_metadata(
                {
                    "service_count": service_count,
                    "service_preview": MetadataValue.md(
                        get_table_preview(conn, "service_aggs")
                    ),
                }
            )

        except Exception as e:
            context.log.error(f"Error creating service aggregates: {e}")
            raise

    return {"aggregation": "service", "count": service_count}


@asset(deps=["billing_db"])
def region_aggregates(context: AssetExecutionContext, duckdb: DuckDBResource):
    """Asset that creates region-specific aggregated metrics."""
    # Use the DuckDBResource to get a connection
    with duckdb.get_connection() as conn:
        try:
            # Create region_aggs table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS region_aggs (
                    region VARCHAR PRIMARY KEY,
                    operation_count INTEGER,
                    total_credit_usage DOUBLE,
                    avg_credit_usage DOUBLE,
                    unique_users INTEGER,
                    resource_types INTEGER,
                    operation_types INTEGER
                )
            """)

            # Clear and regenerate region aggregates
            conn.execute("DELETE FROM region_aggs")

            # Create region aggregates
            conn.execute("""
                INSERT INTO region_aggs
                SELECT 
                    region,
                    COUNT(*) as operation_count,
                    SUM(credit_usage) as total_credit_usage,
                    AVG(credit_usage) as avg_credit_usage,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(DISTINCT resource_type) as resource_types,
                    COUNT(DISTINCT operation_type) as operation_types
                FROM raw_billing
                GROUP BY region
                ORDER BY total_credit_usage DESC
            """)

            # Get record count for metadata
            result = conn.execute("SELECT COUNT(*) FROM region_aggs").fetchone()
            region_count = result[0] if result else 0

            context.log.info(f"Created aggregates for {region_count} regions")

            # Add metadata with preview
            context.add_output_metadata(
                {
                    "region_count": region_count,
                    "region_preview": MetadataValue.md(
                        get_table_preview(conn, "region_aggs")
                    ),
                }
            )

        except Exception as e:
            context.log.error(f"Error creating region aggregates: {e}")
            raise

    return {"aggregation": "region", "count": region_count}


@asset(
    deps=[
        "daily_aggregates",
        "user_aggregates",
        "service_aggregates",
        "region_aggregates",
    ]
)
def billing_insights(context: AssetExecutionContext, duckdb: DuckDBResource):
    """Asset that creates high-level insights from the aggregations."""
    # Use the DuckDBResource to get a connection
    with duckdb.get_connection() as conn:
        insights = {}

        try:
            # Total credit usage
            total = conn.execute(
                "SELECT SUM(credit_usage) FROM raw_billing"
            ).fetchone()[0]
            insights["total_credit_usage"] = total

            # Most active users
            most_active = conn.execute("""
                SELECT user_id, transaction_count 
                FROM user_aggs 
                ORDER BY transaction_count DESC 
                LIMIT 5
            """).fetchall()
            insights["most_active_users"] = most_active

            # Most expensive regions
            expensive_regions = conn.execute("""
                SELECT region, total_credit_usage 
                FROM region_aggs 
                ORDER BY total_credit_usage DESC
                LIMIT 5
            """).fetchall()
            insights["most_expensive_regions"] = expensive_regions

            # Most common operation types
            common_ops = conn.execute("""
                SELECT operation_type, COUNT(*) as count
                FROM raw_billing
                GROUP BY operation_type
                ORDER BY count DESC
            """).fetchall()
            insights["common_operations"] = common_ops

            # Success rate by service tier
            success_rates = conn.execute("""
                SELECT 
                    service_tier,
                    SUM(CASE WHEN success = true THEN 1 ELSE 0 END) as success_count,
                    COUNT(*) as total_count,
                    CAST(SUM(CASE WHEN success = true THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as success_rate
                FROM raw_billing
                GROUP BY service_tier
                ORDER BY success_rate DESC
            """).fetchall()
            insights["success_rates"] = success_rates

            # Create a markdown report
            report = "# Billing Insights\n\n"

            report += "## Total Credit Usage\n"
            report += f"Total credits used: {total:.2f}\n\n"

            report += "## Most Active Users\n"
            report += "| User ID | Transaction Count |\n"
            report += "|---------|------------------|\n"
            for user in most_active:
                report += f"| {user[0]} | {user[1]} |\n"
            report += "\n"

            report += "## Most Resource-Intensive Regions\n"
            report += "| Region | Credit Usage |\n"
            report += "|--------|-------------|\n"
            for region in expensive_regions:
                report += f"| {region[0]} | {region[1]:.2f} |\n"
            report += "\n"

            report += "## Common Operations\n"
            report += "| Operation Type | Count |\n"
            report += "|---------------|-------|\n"
            for op in common_ops:
                report += f"| {op[0]} | {op[1]} |\n"
            report += "\n"

            report += "## Success Rates by Service Tier\n"
            report += "| Service Tier | Success Rate |\n"
            report += "|-------------|-------------|\n"
            for rate in success_rates:
                report += f"| {rate[0]} | {rate[3]:.2%} |\n"

            # Add processed files summary
            processed_files = conn.execute("""
                SELECT COUNT(*) as file_count, SUM(record_count) as record_count 
                FROM processed_files
            """).fetchone()

            report += "\n## Data Processing Summary\n"
            report += f"Processed {processed_files[0]} files with {processed_files[1]} total records\n"

            # Save the report as metadata
            context.add_output_metadata({"insights_report": MetadataValue.md(report)})

        except Exception as e:
            context.log.error(f"Error creating billing insights: {e}")
            raise

    return {"insights": insights}


# Helper function to get table preview for metadata
def get_table_preview(conn, table_name="raw_billing", limit=5):
    """Generate a markdown table preview for metadata."""
    try:
        # Get column names
        cols = conn.execute(f"SELECT * FROM {table_name} LIMIT 0").description
        col_names = [col[0] for col in cols]

        # Get data
        data = conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchall()

        # Create markdown table
        md_table = "| " + " | ".join(col_names) + " |\n"
        md_table += "| " + " | ".join(["---"] * len(col_names)) + " |\n"

        for row in data:
            md_table += "| " + " | ".join([str(val) for val in row]) + " |\n"

        return md_table
    except Exception as e:
        return f"Error generating preview for {table_name}: {e}"
