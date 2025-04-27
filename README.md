# billing-data-pipeline
A demonstration project for a hypothetical billing data pipeline.

## Backfill data

```
dagster job execute -m pipeline.pipeline -j process_billing_data --config configs/backfill.yaml
```


## Running the Dagster UI

```
dagster dev -m pipeline
```