import glob
from datetime import datetime, timedelta

from dagster import (
    AssetSelection,
    Definitions,
    RunRequest,
    ScheduleDefinition,
    define_asset_job,
    sensor,
)
from dagster_duckdb import DuckDBResource

from .assets import (
    billing_db,
    billing_files,
    billing_insights,
    daily_aggregates,
    region_aggregates,
    service_aggregates,
    user_aggregates,
)
from .s3_utils import S3HiveResource
from .utils import get_env

# Define jobs for manual and scheduled runs
all_assets_job = define_asset_job(
    name="process_billing_data", selection=AssetSelection.all()
)

# Create a schedule to run daily at midnight to process the previous day's data
daily_schedule = ScheduleDefinition(
    job=all_assets_job,
    cron_schedule="0 0 * * *",  # Run at midnight every day
    execution_timezone="UTC",
)


# S3 sensor to detect new billing data by date partitions
@sensor(job=all_assets_job)
def s3_billing_partition_sensor(context, s3_hive: S3HiveResource):
    """
    Sensor that detects new billing data partitions in S3.

    This sensor checks for new Hive partitions (year/month/day) in the S3 bucket
    and triggers a run when new data is detected.
    """
    # Get S3 bucket URL
    bucket_url = get_env("S3_BUCKET_URL")

    # Get last processed date from sensor cursor
    last_processed_date_str = context.cursor or ""

    # Set a default date if this is the first run (30 days ago)
    if not last_processed_date_str:
        default_start_date = datetime.now() - timedelta(days=30)
        last_processed_date = default_start_date.strftime("%Y-%m-%d")
    else:
        last_processed_date = last_processed_date_str

    # Convert string date to datetime object for comparison
    last_date = datetime.strptime(last_processed_date, "%Y-%m-%d")

    # Find the latest available partition date
    latest_date = s3_hive.get_latest_partition_date()

    if not latest_date:
        context.log.info("No partition dates found in S3")
        return None

    # Only trigger if there's new data since the last processed date
    if latest_date > last_date:
        # Update cursor with latest processed date
        context.update_cursor(latest_date.strftime("%Y-%m-%d"))

        context.log.info(
            f"Detected new data: processing from {last_date} to {latest_date}"
        )

        # Trigger a run with the from_date parameter to enable backfilling
        return RunRequest(
            run_key=f"s3_billing_{latest_date.strftime('%Y%m%d')}",
            run_config={
                "ops": {
                    "billing_files": {
                        "config": {"from_date": last_date.strftime("%Y-%m-%d")}
                    }
                }
            },
        )

    context.log.info(
        f"No new data detected. Last processed: {last_date}, Latest available: {latest_date}"
    )
    return None


# Original file sensor (keeping for backward compatibility)
@sensor(job=all_assets_job)
def new_billing_file_sensor(context):
    """Sensor that detects new billing files and triggers the pipeline."""
    # Track the latest processed file timestamp
    last_run_files = set(context.cursor or "")

    # Check for new files
    current_files = set(glob.glob("data/raw/*.csv"))
    new_files = current_files - last_run_files

    if new_files:
        # Update cursor with current files for next run
        context.update_cursor(",".join(current_files))

        context.log.info(f"Detected {len(new_files)} new files: {new_files}")
        return RunRequest(
            run_key=f"new_files_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            run_config={},
        )

    return None  # No new files detected


# Define Dagster definitions
defs = Definitions(
    assets=[
        billing_files,
        billing_db,
        daily_aggregates,
        user_aggregates,
        service_aggregates,
        region_aggregates,
        billing_insights,
    ],
    schedules=[daily_schedule],  # Updated to daily schedule
    sensors=[s3_billing_partition_sensor, new_billing_file_sensor],  # Added S3 sensor
    jobs=[all_assets_job],
    resources={
        "duckdb": DuckDBResource(database="data/billing.duckdb"),
        "s3_hive": S3HiveResource(),  # Added S3 resource
    },
)
