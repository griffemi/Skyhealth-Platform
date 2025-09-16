output "network_name" {
  value = google_compute_network.network.name
}

output "network_self_link" {
  value = google_compute_network.network.self_link
}

output "subnet_name" {
  value = google_compute_subnetwork.subnet.name
}

output "subnet_self_link" {
  value = google_compute_subnetwork.subnet.self_link
}
