# HFT Arena — cloud cluster provisioning (GKE skeleton).
# Demonstrates the horizontal-scaling deliverable: a managed K8s cluster with an
# autoscaling node pool sized for the distributed bot fleet. Swap the provider +
# cluster resource for EKS/AKS as needed.

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  # backend "gcs" { bucket = "my-tf-state"; prefix = "hft-arena" }   # set for real use
}

variable "project" { type = string }
variable "region"  { type = string  default = "us-central1" }
variable "cluster_name" { type = string  default = "hft-arena" }

# Node pool bounds: the cluster autoscaler grows nodes as the bot-fleet HPA adds
# pods, so the load generator can scale horizontally to bombard a submission.
variable "min_nodes" { type = number  default = 1 }
variable "max_nodes" { type = number  default = 6 }
variable "machine_type" { type = string  default = "e2-standard-8" }

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_container_cluster" "arena" {
  name                     = var.cluster_name
  location                 = var.region
  remove_default_node_pool = true
  initial_node_count       = 1
  networking_mode          = "VPC_NATIVE"
  ip_allocation_policy {}
}

resource "google_container_node_pool" "arena_nodes" {
  name     = "arena-pool"
  cluster  = google_container_cluster.arena.id
  location = var.region

  autoscaling {
    min_node_count = var.min_nodes
    max_node_count = var.max_nodes
  }

  node_config {
    machine_type = var.machine_type
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    # Guaranteed-QoS sandboxes can use the static CPU-manager policy for pinning.
    kubelet_config {
      cpu_manager_policy = "static"
      cpu_cfs_quota      = true
      pod_pids_limit     = 4096
    }
  }
}

output "cluster_endpoint" {
  value     = google_container_cluster.arena.endpoint
  sensitive = true
}
output "kubectl_config_cmd" {
  value = "gcloud container clusters get-credentials ${var.cluster_name} --region ${var.region} --project ${var.project}"
}
