resource "google_compute_address" "static_ip" {
  name = "${var.instance-name}-ipv4-address"
  region = trim(var.zone, "-abcd")
}

resource "google_compute_instance" "benchClient" {
  name = var.instance-name
  machine_type = "n4-standard-4"
  zone = var.zone
  tags = ["allow-traffic", "allow-ssh"]

  depends_on=[google_compute_address.static_ip]
  boot_disk {
    initialize_params {
      image="ubuntu-os-pro-cloud/ubuntu-pro-2204-lts"
      size  = 5
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

    # Update package lists and install Java (if not already installed)
    sudo apt update
    sudo apt install -y openjdk-11-jdk curl

    # Define the URL for the JAR file and the destination
    JAR_URL=""
    JAR_DEST="/opt/benchmarkingClient.jar"

    # Download the JAR file
    echo "Downloading JAR file from $JAR_URL..."
    curl -L -o $JAR_DEST $JAR_URL

    # Ensure the JAR file is executable
    chmod +x $JAR_DEST

    # Start the JAR file
    echo "Starting JAR file..."
    nohup java -jar $JAR_DEST > /var/log/your-application.log 2>&1 &

    echo "Application started successfully."
  SCRIPT
  }
}
