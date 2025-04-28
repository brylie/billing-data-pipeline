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
            total_result = conn.execute(
                "SELECT SUM(credit_usage) FROM raw_billing"
            ).fetchone()
            total = (
                total_result[0] if total_result and total_result[0] is not None else 0.0
            )
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
            if most_active:
                for user in most_active:
                    user_id = user[0] if user[0] is not None else "Unknown"
                    count = user[1] if user[1] is not None else 0
                    report += f"| {user_id} | {count} |\n"
            else:
                report += "| No data available | - |\n"
            report += "\n"

            report += "## Most Resource-Intensive Regions\n"
            report += "| Region | Credit Usage |\n"
            report += "|--------|-------------|\n"
            if expensive_regions:
                for region in expensive_regions:
                    region_name = region[0] if region[0] is not None else "Unknown"
                    usage = region[1] if region[1] is not None else 0.0
                    report += f"| {region_name} | {usage:.2f} |\n"
            else:
                report += "| No data available | - |\n"
            report += "\n"

            report += "## Common Operations\n"
            report += "| Operation Type | Count |\n"
            report += "|---------------|-------|\n"
            if common_ops:
                for op in common_ops:
                    op_type = op[0] if op[0] is not None else "Unknown"
                    count = op[1] if op[1] is not None else 0
                    report += f"| {op_type} | {count} |\n"
            else:
                report += "| No data available | - |\n"
            report += "\n"

            report += "## Success Rates by Service Tier\n"
            report += "| Service Tier | Success Rate |\n"
            report += "|-------------|-------------|\n"
            if success_rates:
                for rate in success_rates:
                    tier = rate[0] if rate[0] is not None else "Unknown"
                    success_rate = rate[3] if rate[3] is not None else 0.0
                    report += f"| {tier} | {success_rate:.2%} |\n"
            else:
                report += "| No data available | - |\n"

            # Add processed files summary
            processed_files = conn.execute("""
                SELECT COUNT(*) as file_count, SUM(record_count) as record_count 
                FROM processed_files
            """).fetchone()

            report += "\n## Data Processing Summary\n"
            if processed_files and all(val is not None for val in processed_files):
                report += f"Processed {processed_files[0]} files with {processed_files[1]} total records\n"
            else:
                report += "No files processed yet\n"

            # Save the report as metadata
            context.add_output_metadata({"insights_report": MetadataValue.md(report)})

        except Exception as e:
            context.log.error(f"Error creating billing insights: {e}")
            raise

    return {"insights": insights}
