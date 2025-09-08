resource "google_compute_firewall" "allow" {
  name    = "skyfeed-allow"
  network = var.network
  allow {
    protocol = "tcp"
    ports    = ["22","80","443","7077","6066","8080","18080","8501"]
  }
  source_ranges = ["0.0.0.0/0"]
}
