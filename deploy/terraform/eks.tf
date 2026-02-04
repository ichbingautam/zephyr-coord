# EKS cluster configuration

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 19.0"

  cluster_name    = local.name
  cluster_version = var.kubernetes_version

  cluster_endpoint_public_access = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  # EKS Managed Node Groups
  eks_managed_node_groups = {
    zephyr = {
      name = "zephyr-nodes"

      instance_types = [var.instance_type]
      capacity_type  = "ON_DEMAND"

      min_size     = var.cluster_size
      max_size     = var.cluster_size + 2
      desired_size = var.cluster_size

      labels = {
        role = "zephyr-coord"
      }

      tags = local.tags
    }
  }

  # Cluster access
  manage_aws_auth_configmap = true

  aws_auth_roles = []
  aws_auth_users = []

  tags = local.tags
}

# IAM role for ZephyrCoord pods (if needed for AWS integrations)
resource "aws_iam_role" "zephyr_pod_role" {
  name = "${local.name}-pod-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = module.eks.oidc_provider_arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "${module.eks.oidc_provider}:aud" = "sts.amazonaws.com"
            "${module.eks.oidc_provider}:sub" = "system:serviceaccount:zephyr-coord:zephyr-coord"
          }
        }
      }
    ]
  })

  tags = local.tags
}
