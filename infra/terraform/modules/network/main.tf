locals {
  name_prefix = "skyhealth-${var.env}"
}

resource "google_compute_network" "network" {
  name                    = "${local.name_prefix}-net"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "${local.name_prefix}-${var.region}-subnet"
  ip_cidr_range = var.subnet_cidr
  region        = var.region
  network       = google_compute_network.network.id
}
