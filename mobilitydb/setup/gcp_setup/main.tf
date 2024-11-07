provider "google" {
  project = var.project-id
  region = var.region
  zone = "${var.region}-b"
}

module "node" {
  source = "./modules/VM_Node"
  for_each = var.names_and_zones
  network = google_compute_network.mobilitydb_network.self_link
  instance-name = each.key
  zone = each.value
  depends_on = [google_compute_network.mobilitydb_network]

}

resource "google_compute_network" "mobilitydb_network" {
  name = var.network-name
  auto_create_subnetworks = "true"
}

resource "google_compute_firewall" "mobilitydb_firewall_ingress" {
  name="allow-ingress-mobilitydb"
  network=google_compute_network.mobilitydb_network.self_link
  source_ranges = ["0.0.0.0/0"]

  depends_on = [google_compute_network.mobilitydb_network]

  allow {
    protocol = "tcp"
    ports = [
      
    ]
  }
}

resource "google_compute_firewall" "mobilitydb_firewall_egresss" {
  name="allow-egress-mobilitydb"
  network=google_compute_network.mobilitydb_network.self_link
  destination_ranges = ["0.0.0.0/0"]
  direction = "EGRESS"

  depends_on = [google_compute_network.mobilitydb_network]

  allow {
    protocol = "tcp"
    ports = [
      "1433", 
      "5432"
    ]
  }
    allow {
    protocol = "udp"
    ports = [
      "1434"
    ]
  }
}

resource "google_compute_firewall" "allow_ssh" {
  name          = "allow-ssh-mobilitydb"
  network       = google_compute_network.mobilitydb_network.self_link
  source_ranges = ["0.0.0.0/0"] // 0.0.0.0 refers to all IPv4 addresses 

  depends_on = [google_compute_network.mobilitydb_network]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}