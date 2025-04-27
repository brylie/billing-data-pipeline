from dagster import AssetExecutionContext, MetadataValue, asset
from dagster_duckdb import DuckDBResource

from ..utils import get_table_preview


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
