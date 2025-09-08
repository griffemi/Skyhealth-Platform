output "streamlit_url" {
  value = google_cloud_run_v2_service.streamlit.uri
}
output "lightdash_url" {
  value = google_cloud_run_v2_service.lightdash.uri
}
