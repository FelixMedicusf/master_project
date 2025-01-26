resource "google_compute_address" "static_ip" {
  name = "${var.instance-name}-ipv4-address"
  region = trim(var.zone, "-abcd")
}

resource "google_compute_instance" "benchclient" {
  name = var.instance-name
  machine_type = "e2-standard-2"
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

  # Update package lists and install Java
  sudo apt update
  sudo apt install -y openjdk-11-jdk curl

  # Define the URL for the JAR file and the destination
  JAR_URL="https://raw.githubusercontent.com/FelixMedicusf/master_project/main/mongodb/mongodb_bench/mongodb_bench.main.jar"
  # JAR_URL="https://raw.githubusercontent.com/FelixMedicusf/master_project/main/mongodb/"

  JAR_DEST="/opt/benchmarkingClient.jar"

  # Download the JAR file
  echo "Downloading JAR file from $JAR_URL..."
  curl -L -o $JAR_DEST $JAR_URL

  # Verify if the JAR file was downloaded successfully
  if [ ! -f "$JAR_DEST" ]; then
      echo "Failed to download the JAR file from $JAR_URL."
      exit 1
  fi

  # Ensure the JAR file is executable
  chmod +x $JAR_DEST

  # Start the JAR file
  echo "Starting JAR file..."
  nohup java -jar $JAR_DEST > /var/log/your-application.log 2>&1 &

  echo "Application started successfully."
  SCRIPT
  }
}
