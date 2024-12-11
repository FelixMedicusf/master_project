provider "google" {
  project = var.project-id
  region = var.region
  zone = "${var.region}-b"
}

module "node" {
  source = "./modules/VM_Node"
  for_each = var.names_and_zones
  network = google_compute_network.mongodb_network.self_link
  instance-name = each.key
  zone = each.value
  depends_on = [google_compute_network.mongodb_network]

}

resource "google_compute_network" "mongodb_network" {
  name = var.network-name
  auto_create_subnetworks = "true"
}

resource "google_compute_firewall" "mongodb_firewall_ingress" {
  name="allow-ingress-mongodb"
  network=google_compute_network.mongodb_network.self_link
  source_ranges = ["0.0.0.0/0"]

  depends_on = [google_compute_network.mongodb_network]

  allow {
    protocol = "tcp"
    ports = [
      
    ]
  }
}

resource "google_compute_firewall" "mongodb_firewall_egresss" {
  name="allow-egress-mongodb"
  network=google_compute_network.mongodb_network.self_link
  destination_ranges = ["0.0.0.0/0"]
  direction = "EGRESS"

  depends_on = [google_compute_network.mongodb_network]

  allow {
    protocol = "tcp"
    ports = [
      "27019", 
      "27018", 
      "27017", 
      "28017"
    ]
  }
    allow {
    protocol = "udp"
    ports = [
      "27019", 
      "27018", 
      "27017", 
      "28017"
    ]
  }
}

resource "google_compute_firewall" "allow_ssh" {
  name          = "allow-ssh-mongodb"
  network       = google_compute_network.mongodb_network.self_link
  source_ranges = ["0.0.0.0/0"] // 0.0.0.0 refers to all IPv4 addresses 

  depends_on = [google_compute_network.mongodb_network]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}