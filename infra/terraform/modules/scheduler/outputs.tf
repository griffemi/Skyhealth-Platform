output "bronze_job" { value = google_cloud_scheduler_job.bronze_incremental.name }
output "silver_job" { value = google_cloud_scheduler_job.spark_silver.name }
output "dbt_job" { value = google_cloud_scheduler_job.dbt_gold.name }
