from dagster import AssetExecutionContext, MetadataValue, asset
from dagster_duckdb import DuckDBResource


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
