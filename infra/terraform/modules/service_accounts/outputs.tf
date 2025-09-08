output "spark_sa_email" {
  value = google_service_account.spark.email
}
output "jobs_sa_email" {
  value = google_service_account.jobs.email
}
