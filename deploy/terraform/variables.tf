# Input variables for ZephyrCoord deployment

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Name of the EKS cluster"
  type        = string
  default     = "zephyr-coord"
}

variable "cluster_size" {
  description = "Number of ZephyrCoord nodes (should be odd: 3, 5, or 7)"
  type        = number
  default     = 3

  validation {
    condition     = contains([3, 5, 7], var.cluster_size)
    error_message = "Cluster size must be 3, 5, or 7 for quorum."
  }
}

variable "instance_type" {
  description = "EC2 instance type for EKS nodes"
  type        = string
  default     = "t3.medium"
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "kubernetes_version" {
  description = "Kubernetes version for EKS"
  type        = string
  default     = "1.29"
}

variable "zephyr_image" {
  description = "Docker image for ZephyrCoord"
  type        = string
  default     = "ghcr.io/ichbingautam/zephyr-coord:latest"
}

variable "storage_size" {
  description = "Size of persistent storage per node (GB)"
  type        = number
  default     = 20
}
