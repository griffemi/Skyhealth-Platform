resource "google_service_account" "spark" {
  account_id   = "spark-sa"
  display_name = "Spark Service Account"
}

resource "google_service_account" "jobs" {
  account_id   = "jobs-sa"
  display_name = "Jobs Service Account"
}
