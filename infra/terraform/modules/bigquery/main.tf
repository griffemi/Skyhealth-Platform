resource "google_bigquery_dataset" "gold" {
  dataset_id = var.dataset_name
  location   = var.location
}
