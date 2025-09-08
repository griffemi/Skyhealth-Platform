resource "google_cloud_run_v2_job" "spark_submitter" {
  name     = "spark-submitter"
  location = var.region
  template {
    template {
      containers {
        image = var.submitter_image
        dynamic "env" {
          for_each = var.submitter_env
          content {
            name  = env.key
            value = env.value
          }
        }
      }
    }
  }
}

resource "google_cloud_run_v2_job" "dbt_builder" {
  name     = "dbt-builder"
  location = var.region
  template {
    template {
      containers {
        image = var.dbt_image
        dynamic "env" {
          for_each = var.dbt_env
          content {
            name  = env.key
            value = env.value
          }
        }
      }
    }
  }
}

