output "instance_name" {
    value = google_compute_instance.mobilitydb.name
}

output "public_ip" {
  value = google_compute_address.static_ip.address
}

output "zone" {
  value = google_compute_instance.mobilitydb.zone
}
