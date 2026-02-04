# Main Terraform configuration

locals {
  name   = "${var.cluster_name}-${var.environment}"
  azs    = slice(data.aws_availability_zones.available.names, 0, 3)

  tags = {
    Cluster     = local.name
    Environment = var.environment
  }
}

data "aws_availability_zones" "available" {
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

data "aws_caller_identity" "current" {}
