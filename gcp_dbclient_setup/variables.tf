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
  default = "benchclient-node"
}

variable "network-name" {
  type = string 
  default = "benchclient-network"
}

variable "names_and_zones"{
  type = map(string)

  # europe-west1: Belgien, europe-west2: London, europe-west-3: FFM 
  default = {
    benchclient-node-1 = "europe-west1-b"
  }
}