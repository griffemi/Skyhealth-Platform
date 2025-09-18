locals {
  env                     = lower(var.env)
  name_prefix             = "skyhealth-${local.env}"
  project_prefix          = "${var.project_id}-${local.env}"
  bronze_bucket_name      = "${local.project_prefix}-bronze"
  silver_bucket_name      = "${local.project_prefix}-silver"
  gold_bucket_name        = "${local.project_prefix}-gold"
  checkpoints_bucket_name = "${local.project_prefix}-checkpoints"
  warehouse_bucket_name   = "${local.project_prefix}-warehouse"
  dataset_id              = "skyhealth_${local.env}_gold"
  dagster_vm_name         = "${local.name_prefix}-dagster"
  dagster_secret_id       = "${local.name_prefix}-dagster-sql-password"
  dagster_sa_id           = "dagster-runner-${local.env}"
  spark_sa_id             = "spark-runner-${local.env}"
  streamlit_sa_id         = "streamlit-svc-${local.env}"
  dataproc_bucket_name    = var.dataproc_staging_bucket != "" ? var.dataproc_staging_bucket : "${local.project_prefix}-dataproc"
  budget_units            = floor(var.budget_amount)
  budget_nanos            = floor((var.budget_amount - local.budget_units) * 1000000000)
  budget_display_name     = "Skyhealth ${upper(local.env)} Monthly"
  required_services = distinct(concat([
    "compute.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "sqladmin.googleapis.com",
    "secretmanager.googleapis.com",
    "run.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "dataproc.googleapis.com",
    "servicenetworking.googleapis.com",
    "oslogin.googleapis.com",
    "artifactregistry.googleapis.com",
  ], var.project_services))
  lake_buckets = {
    bronze = {
      name      = local.bronze_bucket_name
      retention = var.bronze_retention_days
    }
    silver = {
      name      = local.silver_bucket_name
      retention = var.silver_retention_days
    }
    gold = {
      name      = local.gold_bucket_name
      retention = var.gold_retention_days
    }
    checkpoints = {
      name      = local.checkpoints_bucket_name
      retention = var.bronze_retention_days
    }
    warehouse = {
      name      = local.warehouse_bucket_name
      retention = var.gold_retention_days
    }
  }
  dagster_project_roles = [
    "roles/cloudsql.client",
    "roles/secretmanager.secretAccessor",
    "roles/dataproc.editor",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
  ]
  spark_project_roles = [
    "roles/dataproc.worker",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
  ]
}

data "google_project" "current" {}

resource "google_project_service" "enabled" {
  for_each           = toset(local.required_services)
  service            = each.key
  disable_on_destroy = false
}

module "network" {
  source = "./modules/network"
  region = var.region
  env    = local.env
}

resource "google_artifact_registry_repository" "apps" {
  location      = var.region
  repository_id = "${local.name_prefix}-apps"
  description   = "Skyhealth application images for ${local.env}"
  format        = "DOCKER"
  depends_on    = [google_project_service.enabled["artifactregistry.googleapis.com"]]
}

resource "google_service_account" "dagster_runner" {
  account_id   = local.dagster_sa_id
  display_name = "Dagster runner (${local.env})"
}

resource "google_service_account" "spark_runner" {
  account_id   = local.spark_sa_id
  display_name = "Spark runner (${local.env})"
}

resource "google_service_account" "streamlit" {
  account_id   = local.streamlit_sa_id
  display_name = "Streamlit service (${local.env})"
}

resource "google_project_iam_member" "dagster_roles" {
  for_each = toset(local.dagster_project_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.dagster_runner.email}"
}

resource "google_project_iam_member" "spark_roles" {
  for_each = toset(local.spark_project_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.spark_runner.email}"
}

resource "google_project_iam_member" "streamlit_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.streamlit.email}"
}

resource "random_password" "dagster_sql" {
  length  = 24
  special = true
}

resource "google_secret_manager_secret" "dagster_sql_password" {
  secret_id = local.dagster_secret_id
  replication {
    automatic = true
  }
}

resource "google_secret_manager_secret_version" "dagster_sql_password" {
  secret      = google_secret_manager_secret.dagster_sql_password.id
  secret_data = random_password.dagster_sql.result
}

resource "google_secret_manager_secret_iam_member" "dagster_secret_access" {
  secret_id = google_secret_manager_secret.dagster_sql_password.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.dagster_runner.email}"
}

resource "google_storage_bucket" "lake" {
  for_each                    = local.lake_buckets
  name                        = each.value.name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = local.env != "prod"
  labels = {
    env   = local.env
    layer = each.key
  }
  versioning {
    enabled = true
  }
  lifecycle_rule {
    condition {
      age = each.value.retention
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

resource "google_storage_bucket" "dataproc_staging" {
  count                       = var.dataproc_staging_bucket == "" ? 1 : 0
  name                        = local.dataproc_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = local.env != "prod"
  labels = {
    env       = local.env
    component = "dataproc-staging"
  }
}

locals {
  dataproc_bucket_effective = var.dataproc_staging_bucket != "" ? var.dataproc_staging_bucket : try(google_storage_bucket.dataproc_staging[0].name, local.dataproc_bucket_name)
}

resource "google_storage_bucket_iam_member" "spark_runner_lake" {
  for_each = google_storage_bucket.lake
  bucket   = each.value.name
  role     = "roles/storage.objectAdmin"
  member   = "serviceAccount:${google_service_account.spark_runner.email}"
}

resource "google_storage_bucket_iam_member" "dagster_lake_viewer" {
  for_each = google_storage_bucket.lake
  bucket   = each.value.name
  role     = "roles/storage.objectViewer"
  member   = "serviceAccount:${google_service_account.dagster_runner.email}"
}

resource "google_storage_bucket_iam_member" "spark_runner_dataproc" {
  bucket = locals.dataproc_bucket_effective
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.spark_runner.email}"
}

resource "google_bigquery_dataset" "gold" {
  dataset_id                 = local.dataset_id
  project                    = var.project_id
  location                   = var.region
  delete_contents_on_destroy = local.env != "prod"
  labels = {
    env  = local.env
    tier = "gold"
  }
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
}

resource "google_bigquery_dataset_iam_member" "spark_editor" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.spark_runner.email}"
}

resource "google_bigquery_dataset_iam_member" "streamlit_viewer" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.streamlit.email}"
}

resource "google_compute_global_address" "sql_private_ip" {
  name          = "${local.name_prefix}-sql-private"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  network       = module.network.network_self_link
  prefix_length = 16
}

resource "google_service_networking_connection" "sql" {
  network                 = module.network.network_self_link
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.sql_private_ip.name]
  depends_on              = [google_project_service.enabled["servicenetworking.googleapis.com"]]
}

resource "google_sql_database_instance" "dagster" {
  name             = "${local.name_prefix}-dagster-meta"
  database_version = "POSTGRES_15"
  region           = var.region
  depends_on       = [google_service_networking_connection.sql]
  settings {
    tier              = var.dagster_sql_tier
    disk_size         = var.dagster_sql_disk_size
    disk_autoresize   = true
    availability_type = local.env == "prod" ? "REGIONAL" : "ZONAL"
    ip_configuration {
      ipv4_enabled                                  = false
      private_network                               = module.network.network_self_link
      enable_private_path_for_google_cloud_services = true
      require_ssl                                   = true
    }
    backup_configuration {
      enabled    = true
      start_time = "03:00"
      location   = var.region
      backup_retention_settings {
        retained_backups = var.dagster_sql_backup_retention
        retention_unit   = "COUNT"
      }
    }
    insights_config {
      query_insights_enabled = true
      query_plans_per_minute = 5
      query_string_length    = 1024
    }
    maintenance_window {
      day  = 1
      hour = 5
    }
  }
  deletion_protection = local.env == "prod"
}

resource "google_sql_database" "dagster" {
  name     = "dagster"
  instance = google_sql_database_instance.dagster.name
}

resource "google_sql_user" "dagster" {
  name     = "dagster"
  instance = google_sql_database_instance.dagster.name
  password = random_password.dagster_sql.result
}

resource "google_compute_instance" "dagster" {
  name         = local.dagster_vm_name
  machine_type = var.dagster_machine_type
  zone         = var.zone
  labels = {
    env  = local.env
    role = "dagster"
  }
  boot_disk {
    initialize_params {
      image = var.dagster_boot_image
      type  = "pd-balanced"
      size  = var.dagster_disk_size_gb
    }
  }
  network_interface {
    subnetwork = module.network.subnet_self_link
  }
  metadata = {
    enable-oslogin = "TRUE"
  }
  service_account {
    email  = google_service_account.dagster_runner.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }
  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
  }
  tags       = ["dagster-control-plane"]
  depends_on = [google_project_service.enabled["compute.googleapis.com"]]
}

resource "google_compute_firewall" "dagster_iap" {
  name          = "${local.name_prefix}-dagster-iap"
  network       = module.network.network_name
  target_tags   = ["dagster-control-plane"]
  direction     = "INGRESS"
  priority      = 1000
  source_ranges = ["35.235.240.0/20"]
  allow {
    protocol = "tcp"
    ports    = ["22", "3000", "3001"]
  }
  description = "Allow IAP access for Dagster SSH and web UI"
}

resource "google_service_account_iam_member" "streamlit_runtime" {
  service_account_id = google_service_account.streamlit.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:service-${data.google_project.current.number}@serverless-robot-prod.iam.gserviceaccount.com"
}

resource "google_cloud_run_service" "streamlit" {
  name                       = "${local.name_prefix}-streamlit"
  location                   = var.region
  autogenerate_revision_name = true
  template {
    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale"         = tostring(var.streamlit_max_instances)
        "run.googleapis.com/ingress"               = "all"
        "run.googleapis.com/execution-environment" = "gen2"
      }
      labels = {
        env       = local.env
        component = "streamlit"
      }
    }
    spec {
      service_account_name = google_service_account.streamlit.email
      containers {
        image = var.streamlit_image
        resources {
          limits = {
            cpu    = format("%g", var.streamlit_cpu)
            memory = "${var.streamlit_memory}Mi"
          }
        }
        env {
          name  = "SKYHEALTH_ENV"
          value = local.env
        }
        env {
          name  = "GCP_PROJECT"
          value = var.project_id
        }
        env {
          name  = "BQ_DATASET"
          value = google_bigquery_dataset.gold.dataset_id
        }
        env {
          name  = "BRONZE_BUCKET"
          value = google_storage_bucket.lake["bronze"].name
        }
        env {
          name  = "SILVER_BUCKET"
          value = google_storage_bucket.lake["silver"].name
        }
        env {
          name  = "GOLD_BUCKET"
          value = google_storage_bucket.lake["gold"].name
        }
      }
    }
  }
  traffic {
    percent         = 100
    latest_revision = true
  }
  depends_on = [
    google_project_service.enabled["run.googleapis.com"],
    google_service_account_iam_member.streamlit_runtime,
  ]
}

resource "google_cloud_run_service_iam_member" "streamlit_public" {
  location = google_cloud_run_service.streamlit.location
  project  = var.project_id
  service  = google_cloud_run_service.streamlit.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_monitoring_alert_policy" "dagster_failures" {
  display_name          = "Dagster job failures (${local.env})"
  combiner              = "OR"
  notification_channels = var.notification_channels
  documentation {
    content   = "Alerts when Dagster emits ERROR logs on the control plane VM."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "Dagster ERROR logs"
    condition_monitoring_query_language {
      query    = <<-EOT
        fetch logging
        | filter resource.type = "gce_instance"
        | filter resource.labels.instance_id = "${google_compute_instance.dagster.instance_id}"
        | filter severity >= ERROR
        | align rate(5m)
        | every 5m
        | group_by [], sum
      EOT
      duration = "0s"
      trigger {
        count = 1
      }
    }
  }
}

resource "google_monitoring_alert_policy" "bronze_stale" {
  display_name          = "Bronze ingest stale (${local.env})"
  combiner              = "OR"
  notification_channels = var.notification_channels
  documentation {
    content   = "No new Bronze objects observed in the last 24h."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "Bronze ingest gap"
    condition_absent {
      filter   = "metric.type=\"storage.googleapis.com/storage/object_count\" AND resource.type=\"storage_bucket\" AND resource.label.bucket_name=\"${google_storage_bucket.lake["bronze"].name}\""
      duration = "86400s"
      trigger {
        count = 1
      }
    }
  }
}

resource "google_monitoring_alert_policy" "streamlit_5xx" {
  display_name          = "Streamlit 5xx spike (${local.env})"
  combiner              = "OR"
  notification_channels = var.notification_channels
  documentation {
    content   = "Streamlit Cloud Run returning 5xx errors above threshold."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "Streamlit 5xx count"
    condition_threshold {
      filter = "metric.type=\"run.googleapis.com/request_count\" AND resource.type=\"cloud_run_revision\" AND metric.label.\"response_code_class\"=\"5xx\" AND resource.label.\"service_name\"=\"${google_cloud_run_service.streamlit.name}\""
      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.service_name"]
      }
      comparison      = "COMPARISON_GT"
      threshold_value = 5
      duration        = "0s"
      trigger {
        count = 1
      }
    }
  }
}

resource "google_monitoring_alert_policy" "bq_errors" {
  display_name          = "BigQuery error spikes (${local.env})"
  combiner              = "OR"
  notification_channels = var.notification_channels
  documentation {
    content   = "BigQuery failures detected in project ${var.project_id}."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "BigQuery failed jobs"
    condition_threshold {
      filter = "metric.type=\"bigquery.googleapis.com/query/failed_jobs\" AND resource.type=\"bigquery_project\" AND resource.label.\"project_id\"=\"${var.project_id}\""
      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
      }
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      trigger {
        count = 1
      }
    }
  }
}

resource "google_billing_budget" "project_budget" {
  count           = var.billing_account == "" ? 0 : 1
  billing_account = var.billing_account
  display_name    = local.budget_display_name
  budget_filter {
    projects = ["projects/${var.project_id}"]
  }
  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(local.budget_units)
      nanos         = local.budget_nanos
    }
  }
  threshold_rules {
    threshold_percent = 0.5
  }
  threshold_rules {
    threshold_percent = 0.75
  }
  threshold_rules {
    threshold_percent = 1.0
  }
  all_updates_rule {
    monitoring_notification_channels = var.notification_channels
    disable_default_iam_recipients   = true
  }
}
