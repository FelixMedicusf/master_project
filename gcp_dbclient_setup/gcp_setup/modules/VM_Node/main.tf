resource "google_compute_address" "static_ip" {
  name = "${var.instance-name}-ipv4-address"
  region = trim(var.zone, "-abcd")
}

resource "google_compute_instance" "benchclient" {
  name = var.instance-name
  machine_type = "c3-standard-8"
  zone = var.zone
  tags = ["allow-traffic", "allow-ssh"]

  depends_on=[google_compute_address.static_ip]
  enable_display = true
  boot_disk {
    initialize_params {
      image="ubuntu-os-pro-cloud/ubuntu-pro-2204-lts"
      size  = 10
      type = "pd-ssd"
    }
  }

  network_interface {
    network=var.network
    access_config {
      nat_ip = google_compute_address.static_ip.address
      #ephemeral(flüchtig)
    }
  }

  metadata = {
startup-script = <<SCRIPT
  #!/bin/bash
  set -e

  sudo apt update
  sudo apt install -y openjdk-11-jdk curl

  JAR_URL_MONGODB="https://raw.githubusercontent.com/FelixMedicusf/master_project/main/mongodb/mongodb_bench/mongodbBenchmarkingApplication.jar"
  JAR_URL_MOBILITYDB="https://raw.githubusercontent.com/FelixMedicusf/master_project/main/mobilitydb/mobilitydb_bench/mobilitydbBenchmarkingApplication.jar"


  JAR_DEST_MONGODB="/opt/mongodbBenchmarkingApplication.jar"
  JAR_DEST_MOBILITYDB="/opt/mobilitydbBenchmarkingApplication.jar"

  curl -L -o $JAR_DEST_MONGODB $JAR_URL_MONGODB
  curl -L -o $JAR_DEST_MOBILITYDB $JAR_URL_MOBILITYDB

  chmod 777 -R $JAR_DEST_MONGODB
  chmod 777 -R $JAR_DEST_MOBILITYDB
  chmod 777 /opt/


  # Ensure the JAR file is executable
  chmod +x $JAR_DEST_MONGODB
  chmod +x $JAR_DEST_MOBILITYDB

  SCRIPT
  }
}
