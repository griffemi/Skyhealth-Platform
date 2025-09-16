output "dagster_vm_name" {
  value = google_compute_instance.dagster.name
}

output "dagster_service_account" {
  value = google_service_account.dagster_runner.email
}

output "dagster_sql_instance" {
  value = google_sql_database_instance.dagster.connection_name
}

output "dagster_sql_secret" {
  value = google_secret_manager_secret.dagster_sql_password.secret_id
}

output "lake_buckets" {
  value = {
    bronze      = google_storage_bucket.lake["bronze"].name
    silver      = google_storage_bucket.lake["silver"].name
    gold        = google_storage_bucket.lake["gold"].name
    checkpoints = google_storage_bucket.lake["checkpoints"].name
  }
}

output "dataproc_staging_bucket" {
  value = locals.dataproc_bucket_effective
}

output "bigquery_dataset" {
  value = google_bigquery_dataset.gold.dataset_id
}

output "streamlit_url" {
  value = google_cloud_run_service.streamlit.status[0].url
}
