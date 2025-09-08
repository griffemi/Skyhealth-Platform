output "master_ip" {
  value = google_compute_instance.master.network_interface[0].access_config[0].nat_ip
}

output "worker_mig" {
  value = google_compute_instance_group_manager.workers.name
}

output "master_name" {
  value = google_compute_instance.master.name
}
