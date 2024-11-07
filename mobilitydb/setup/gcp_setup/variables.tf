variable "project-id" {
  type = string 
  default = "csbws2223"
}

variable "region" {
  type = string 
  default = "europe-west1"
}

variable "instance-name" {
  type = string
  default = "mobilitydb-node"
}

variable "network-name" {
  type = string 
  default = "mobilitydb-network"
}

variable "names_and_zones"{
  type = map(string)

  # europe-west1: Belgien, europe-west2: London, europe-west-3: FFM 
  default = {
    mobilitydb-node-1 = "europe-west1-b"
    mobilitydb-node-2 = "europe-west1-b"
    mobilitydb-node-3 = "europe-west1-b"
  }
}



