resource "google_storage_bucket" "lake" {
  name     = var.bucket_name
  location = var.location
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition { age = var.nearline_days }
    action { type = "SetStorageClass" storage_class = "NEARLINE" }
  }

  lifecycle_rule {
    condition { age = var.coldline_days }
    action { type = "SetStorageClass" storage_class = "COLDLINE" }
  }
}
