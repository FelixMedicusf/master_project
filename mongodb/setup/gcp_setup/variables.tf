variable "project-id" {
  type = string 
  default = "still-emissary-449218-f3"
}

variable "region" {
  type = string 
  default = "europe-west1"
}

variable "instance-name" {
  type = string
  default = "mongodb-node"
}

variable "network-name" {
  type = string 
  default = "mongodb-network"
}

variable "names_and_zones"{
  type = map(string)

  # europe-west1: Belgien
  default = {
    mongodb-node-1 = "europe-west1-b"
    mongodb-node-2 = "europe-west1-b"
    mongodb-node-3 = "europe-west1-b"

  }
}



