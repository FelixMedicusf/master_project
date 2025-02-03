resource "google_compute_address" "static_ip" {
  name = "${var.instance-name}-ipv4-address"
  region = trim(var.zone, "-abcd")
}

resource "google_compute_instance" "mobilitydb" {
  name = var.instance-name
  machine_type = "c3-standard-8"
  zone = var.zone
  tags = ["allow-traffic", "allow-ssh"]

  depends_on=[google_compute_address.static_ip]
  boot_disk {
    initialize_params {
      image="ubuntu-os-pro-cloud/ubuntu-pro-2204-lts"
      size  = 250
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
    startup-script=<<SCRIPT
    sudo apt update
    SCRIPT
  }
}
