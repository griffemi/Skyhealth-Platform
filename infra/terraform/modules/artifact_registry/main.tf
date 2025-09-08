resource "google_artifact_registry_repository" "repo" {
  location      = var.location
  repository_id = "skyfeed"
  format        = "DOCKER"
}
