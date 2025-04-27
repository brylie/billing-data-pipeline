import glob
from datetime import datetime

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

# Define jobs for manual and scheduled runs
all_assets_job = define_asset_job(
    name="process_billing_data", selection=AssetSelection.all()
)

# Create a schedule to run every hour
hourly_schedule = ScheduleDefinition(
    job=all_assets_job,
    cron_schedule="0 * * * *",  # Run at the top of every hour
)


# Define a sensor to detect new billing files
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
    schedules=[hourly_schedule],
    sensors=[new_billing_file_sensor],
    jobs=[all_assets_job],
    resources={"duckdb": DuckDBResource(database="data/billing.duckdb")},
)
