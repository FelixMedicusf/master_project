provider "google" {
  project = var.project-id
  region = var.region
  zone = "${var.region}-b"
}

module "node" {
  source = "./modules/VM_Node"
  for_each = var.names_and_zones
  network = google_compute_network.geomesa_network.self_link
  instance-name = each.key
  zone = each.value
  depends_on = [google_compute_network.geomesa_network]

}

resource "google_compute_network" "geomesa_network" {
  name = var.network-name
  auto_create_subnetworks = "true"
}

resource "google_compute_firewall" "geomesa_firewall_ingress" {
  name="allow-ingress-geomesa"
  network=google_compute_network.geomesa_network.self_link
  source_ranges = ["0.0.0.0/0"]

  depends_on = [google_compute_network.geomesa_network]

  allow {
    protocol = "tcp"
    ports = [
      
    ]
  }
}

resource "google_compute_firewall" "geomesa_firewall_egresss" {
  name="allow-egress-geomesa"
  network=google_compute_network.geomesa_network.self_link
  destination_ranges = ["0.0.0.0/0"]
  direction = "EGRESS"

  depends_on = [google_compute_network.geomesa_network]

  allow {
    protocol = "tcp"
    ports = [
      "2181", # zookeeper client port
      "2888", # communication between zookeeper nodes
      "3888", # zookeeper leader election port
      "4445", 
      "8020",
      "50010", 
      "50020",
      "50070", 
      "50090",
      "9132",
      "9133", 
      "9995", 
      "9996", 
      "9997", 
      "9998", 
      "9870", 
      "8088",
      "19888",
      "9999", 
      "12234", 
      "42424", 
      "10001", 
      "10002"
    ]
  }
    allow {
    protocol = "udp"
    ports = [
      ""
    ]
  }
}

resource "google_compute_firewall" "allow_ssh" {
  name          = "allow-ssh-geomesa"
  network       = google_compute_network.geomesa_network.self_link
  source_ranges = ["0.0.0.0/0"] // 0.0.0.0 refers to all IPv4 addresses 

  depends_on = [google_compute_network.geomesa_network]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}