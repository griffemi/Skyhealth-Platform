# Runbooks

## Dagster Pipeline Health
- SSH via IAP to the Dagster VM and ensure `dagster-webserver` and `dagster-daemon` are `active (running)` (`systemctl status dagster-webserver dagster-daemon`).
- Tail logs from journald when a run fails: `journalctl -u dagster-daemon -f`.
- The Ops Agent ships journald to Cloud Logging; filter with `resource.type="gce_instance" AND textPayload:"dagster"` for longer history.

## Spark + Delta Checks
- Delta tables live in GCS. Inspect the most recent partition with `gsutil ls gs://$PROJECT-$ENV-gold/climate_daily_summary/`.
- Dataproc Serverless batches appear via `gcloud dataproc batches list --region=$REGION`; describe a batch for logs if rerunning remotely.

## Gold → BigQuery
- Confirm today’s partition exists:

```sql
SELECT MAX(observation_date)
FROM `${PROJECT}.${DATASET}.climate_daily_summary`;
```

- Re-publish a partition after fixing data: `make publish-bq PARTITION=YYYY-MM-DD`.

## Streamlit
- Current revision URL: `gcloud run services describe $SERVICE --region=$REGION --format='value(status.url)'`.
- Logs stream to Cloud Logging via `resource.type="cloud_run_revision" AND resource.labels.service_name="$SERVICE"`.
