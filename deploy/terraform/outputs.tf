# Output values

output "cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = module.eks.cluster_endpoint
}

output "cluster_security_group_id" {
  description = "Security group ID attached to the EKS cluster"
  value       = module.eks.cluster_security_group_id
}

output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "private_subnets" {
  description = "Private subnet IDs"
  value       = module.vpc.private_subnets
}

output "zephyr_client_endpoint" {
  description = "ZephyrCoord client endpoint (LoadBalancer)"
  value       = "Run: kubectl get svc -n zephyr-coord zephyr-coord-client"
}

output "zephyr_headless_dns" {
  description = "Internal DNS for ZephyrCoord nodes"
  value       = "zephyr-coord-{0,1,2}.zephyr-coord-headless.zephyr-coord.svc.cluster.local"
}

output "kubeconfig_command" {
  description = "Command to update kubeconfig"
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}
