output "bucket_name" {
  value = module.gcs_bucket.bucket_name
}
output "dataset" {
  value = module.bigquery.dataset_id
}

output "spark_master_ip" {
  value = module.compute_spark_cluster.master_ip
}

output "worker_mig" {
  value = module.compute_spark_cluster.worker_mig
}

output "streamlit_url" {
  value = module.cloud_run_services.streamlit_url
}

output "lightdash_url" {
  value = module.cloud_run_services.lightdash_url
}
