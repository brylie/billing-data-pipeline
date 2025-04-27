import glob
import os

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
    """Asset that creates a DuckDB database from the billing files."""
    db_path = "data/billing.duckdb"

    # Use the DuckDBResource to get a connection
    with duckdb.get_connection() as conn:
        # Load raw files into DuckDB
        try:
            conn.execute("""
                CREATE OR REPLACE TABLE raw_billing AS 
                SELECT * FROM read_csv_auto('data/raw/*.csv', header=true);
            """)

            # Get record count for metadata
            result = conn.execute("SELECT COUNT(*) FROM raw_billing").fetchone()
            record_count = result[0] if result else 0

            context.log.info(f"Loaded {record_count} records into DuckDB")

            # Add metadata
            context.add_output_metadata(
                {
                    "record_count": record_count,
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
            # Create daily aggregates using the actual schema
            conn.execute("""
                CREATE OR REPLACE TABLE daily_aggs AS
                SELECT 
                    year, month, day,
                    COUNT(*) as transaction_count,
                    SUM(credit_usage) as total_credit_usage,
                    AVG(credit_usage) as avg_credit_usage,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(DISTINCT resource_id) as unique_resources,
                    SUM(CASE WHEN success = 'true' THEN 1 ELSE 0 END) as successful_operations,
                    SUM(CASE WHEN success = 'false' THEN 1 ELSE 0 END) as failed_operations
                FROM raw_billing
                GROUP BY year, month, day
                ORDER BY year, month, day;
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
            # Create user aggregates based on actual schema
            conn.execute("""
                CREATE OR REPLACE TABLE user_aggs AS
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
                ORDER BY total_credit_usage ASC;
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
            # Create service tier and resource type aggregates
            conn.execute("""
                CREATE OR REPLACE TABLE service_aggs AS
                SELECT 
                    service_tier,
                    resource_type,
                    operation_type,
                    COUNT(*) as operation_count,
                    SUM(credit_usage) as total_credit_usage,
                    AVG(credit_usage) as avg_credit_usage,
                    COUNT(DISTINCT user_id) as unique_users,
                    SUM(CASE WHEN success = 'true' THEN 1 ELSE 0 END) as successful_operations,
                    SUM(CASE WHEN success = 'false' THEN 1 ELSE 0 END) as failed_operations
                FROM raw_billing
                GROUP BY service_tier, resource_type, operation_type
                ORDER BY service_tier, resource_type, operation_type;
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
            # Create region aggregates
            conn.execute("""
                CREATE OR REPLACE TABLE region_aggs AS
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
                ORDER BY total_credit_usage ASC;
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
                ORDER BY total_credit_usage ASC
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
                    SUM(CASE WHEN success = 'true' THEN 1 ELSE 0 END) as success_count,
                    COUNT(*) as total_count,
                    CAST(SUM(CASE WHEN success = 'true' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as success_rate
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
