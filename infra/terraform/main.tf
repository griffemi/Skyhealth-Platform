module "network" {
  source = "./modules/network"
}

module "gcs_bucket" {
  source      = "./modules/gcs_bucket"
  bucket_name = var.bucket_name
}

module "bigquery" {
  source       = "./modules/bigquery"
  dataset_name = var.dataset_name
}

module "service_accounts" {
  source = "./modules/service_accounts"
}

module "firewall" {
  source  = "./modules/firewall"
  network = module.network.network_name
}

module "compute_spark_cluster" {
  source  = "./modules/compute_spark_cluster"
  network = module.network.network_self_link
}

module "artifact_registry" {
  source = "./modules/artifact_registry"
}

module "cloud_run_jobs" {
  source = "./modules/cloud_run_jobs"
  submitter_image = var.submitter_image
  dbt_image       = var.dbt_image
  submitter_env = {
    WORKER_PROJECT        = var.project_id
    WORKER_ZONE           = var.zone
    WORKER_MIG_NAME       = module.compute_spark_cluster.worker_mig
    SPARK_MASTER_REST_URL = "http://${module.compute_spark_cluster.master_ip}:6066"
    SPARK_MASTER_UI_URL   = "http://${module.compute_spark_cluster.master_ip}:8080"
    MASTER_PROJECT        = var.project_id
    MASTER_ZONE           = var.zone
    MASTER_INSTANCE_NAME  = module.compute_spark_cluster.master_name
  }
}

module "cloud_run_services" {
  source          = "./modules/cloud_run_services"
  region          = var.region
  streamlit_image = var.streamlit_image
  lightdash_image = var.lightdash_image
  parquet_export  = var.parquet_export
}

module "scheduler" {
  source         = "./modules/scheduler"
  project_id     = var.project_id
  region         = var.region
  submitter_job  = module.cloud_run_jobs.spark_submitter_name
  dbt_job        = module.cloud_run_jobs.dbt_builder_name
}
