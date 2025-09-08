resource "google_cloud_run_v2_service" "streamlit" {
  name     = "streamlit"
  location = var.region
  template {
    containers {
      image = var.streamlit_image
      env { name = "PARQUET_EXPORT" value = var.parquet_export }
    }
  }
}

resource "google_cloud_run_v2_service" "lightdash" {
  name     = "lightdash"
  location = var.region
  template {
    containers {
      image = var.lightdash_image
    }
  }
}
