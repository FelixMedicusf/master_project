variable "project-id" {
  type = string 
  default = "master-project-430210"
}

variable "region" {
  type = string 
  default = "europe-west1"
}

variable "instance-name" {
  type = string
  default = "secondo-node"
}

variable "network-name" {
  type = string 
  default = "secondo-network"
}

variable "names_and_zones"{
  type = map(string)

  # europe-west1: Belgien, europe-west2: London, europe-west-3: FFM 
  default = {
    secondo-node-1 = "europe-west1-b"
    secondo-node-2 = "europe-west1-b"
  }
}



