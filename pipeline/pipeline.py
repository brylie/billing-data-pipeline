import glob
import os
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
@sensor(job=all_assets_job, minimum_interval_seconds=300)  # Run at most every 5 minutes
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

    # Find the latest available partition date, passing the bucket_url
    latest_date = s3_hive.get_latest_partition_date(bucket_url=bucket_url)

    if not latest_date:
        context.log.info(f"No partition dates found in S3 bucket: {bucket_url}")
        return None

    # Only trigger if there's new data since the last processed date
    if latest_date > last_date:
        # Update cursor with latest processed date
        context.update_cursor(latest_date.strftime("%Y-%m-%d"))

        context.log.info(
            f"Detected new data in {bucket_url}: processing from {last_date} to {latest_date}"
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
        f"No new data detected in {bucket_url}. Last processed: {last_date}, Latest available: {latest_date}"
    )
    return None


# Update the file sensor with proper file tracking and minimum interval
@sensor(
    job=all_assets_job, minimum_interval_seconds=3600
)  # Limit to run at most hourly
def new_billing_file_sensor(context):
    """Sensor that detects new billing files and triggers the pipeline."""
    # Track the latest processed file timestamp using a more reliable method
    last_run_files_str = context.cursor or ""
    last_run_files = set(last_run_files_str.split(",")) if last_run_files_str else set()

    # Get current files with their modification times to detect actual changes
    current_files_with_mtimes = {}
    for file_path in glob.glob(
        "data/raw/billing-*.csv"
    ):  # Only track billing files from S3
        try:
            mtime = os.path.getmtime(file_path)
            current_files_with_mtimes[file_path] = mtime
        except (FileNotFoundError, PermissionError):
            continue

    current_files = set(current_files_with_mtimes.keys())

    # Check for new files by comparing paths
    new_files = current_files - last_run_files

    # Also check if any existing files have been modified
    modified_files = set()
    if not new_files and last_run_files_str:
        last_run_mtimes = {}
        for file_record in [f for f in last_run_files_str.split(",") if ":" in f]:
            try:
                file_path, mtime_str = file_record.rsplit(":", 1)
                last_run_mtimes[file_path] = float(mtime_str)
            except (ValueError, IndexError):
                continue

        for file_path in current_files & set(last_run_mtimes.keys()):
            if (
                current_files_with_mtimes.get(file_path, 0)
                > last_run_mtimes.get(file_path, 0) + 1
            ):  # 1 second buffer
                modified_files.add(file_path)

    if new_files or modified_files:
        context.log.info(
            f"Detected {len(new_files)} new files and {len(modified_files)} modified files"
        )

        # Store files with their modification times in the cursor
        file_records = []
        for file_path in current_files:
            mtime = current_files_with_mtimes.get(file_path, 0)
            file_records.append(f"{file_path}:{mtime}")

        context.update_cursor(",".join(file_records))

        return RunRequest(
            run_key=f"files_changed_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            run_config={},
        )

    return None  # No new or modified files detected


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
