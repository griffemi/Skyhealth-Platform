resource "google_compute_instance" "master" {
  name         = "spark-master"
  machine_type = "e2-standard-2"
  zone         = var.zone
  boot_disk {
    initialize_params { image = "debian-cloud/debian-12" }
  }
  network_interface {
    subnetwork   = var.network
    access_config {}
  }
metadata_startup_script = <<-EOT
#!/bin/bash
apt-get update && apt-get install -y default-jdk python3.13
EOT
}

resource "google_compute_instance_template" "worker" {
  name_prefix  = "spark-worker-"
  machine_type = "e2-standard-2"
  region       = regexreplace(var.zone, "-[a-z]$", "")
  disk {
    source_image = "debian-cloud/debian-12"
    boot         = true
  }
  network_interface {
    subnetwork = var.network
  }
  scheduling {
    preemptible       = true
    automatic_restart = false
  }
metadata_startup_script = <<-EOT
#!/bin/bash
apt-get update && apt-get install -y default-jdk python3.13
/opt/spark/sbin/start-worker.sh spark://${google_compute_instance.master.network_interface[0].network_ip}:7077
EOT
}

resource "google_compute_instance_group_manager" "workers" {
  name               = "spark-workers"
  base_instance_name = "spark-worker"
  zone               = var.zone
  version {
    instance_template = google_compute_instance_template.worker.id
  }
  target_size = 0
}
