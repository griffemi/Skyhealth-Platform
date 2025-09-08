locals {
  base = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs"
}

resource "google_cloud_scheduler_job" "bronze_incremental" {
  name      = "bronze-incremental"
  schedule  = "0 * * * *"
  time_zone = "UTC"
  http_target {
    uri        = "${local.base}/${var.submitter_job}:run"
    http_method = "POST"
    body = base64encode(jsonencode({
      overrides = {
        containerOverrides = [{
          env = [{ name = "MODE", value = "bronze-incremental" }]
        }]
      }
    }))
    headers = { "Content-Type" = "application/json" }
  }
}

resource "google_cloud_scheduler_job" "spark_silver" {
  name      = "spark-silver"
  schedule  = "0 3 * * *"
  time_zone = "UTC"
  http_target {
    uri        = "${local.base}/${var.submitter_job}:run"
    http_method = "POST"
    body = base64encode(jsonencode({
      overrides = {
        containerOverrides = [{ env = [{ name = "MODE", value = "silver" }] }]
      }
    }))
    headers = { "Content-Type" = "application/json" }
  }
}

resource "google_cloud_scheduler_job" "dbt_gold" {
  name      = "dbt-gold"
  schedule  = "30 5 * * *"
  time_zone = "UTC"
  http_target {
    uri        = "${local.base}/${var.dbt_job}:run"
    http_method = "POST"
  }
}
