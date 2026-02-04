# ZephyrCoord Deployment

This directory contains Terraform and Kubernetes configurations for deploying ZephyrCoord to AWS EKS.

## Prerequisites

- AWS CLI configured with appropriate credentials
- Terraform >= 1.5.0
- kubectl

## Quick Start

```bash
cd deploy/terraform

# Copy and customize variables
cp terraform.tfvars.example terraform.tfvars

# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Deploy
terraform apply

# Configure kubectl
aws eks update-kubeconfig --region us-east-1 --name zephyr-coord-dev

# Verify deployment
kubectl get pods -n zephyr-coord
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         AWS VPC                              │
│  ┌─────────────────────────────────────────────────────────┐│
│  │                    Private Subnets                       ││
│  │  ┌─────────────────────────────────────────────────────┐││
│  │  │                    EKS Cluster                       │││
│  │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐             │││
│  │  │  │zephyr-0 │  │zephyr-1 │  │zephyr-2 │             │││
│  │  │  │ (Leader)│  │(Follower│  │(Follower│             │││
│  │  │  └────┬────┘  └────┬────┘  └────┬────┘             │││
│  │  │       │            │            │                   │││
│  │  │       └────────────┼────────────┘                   │││
│  │  │              Headless Service                       │││
│  │  │              (Peer Discovery)                       │││
│  │  └─────────────────────────────────────────────────────┘││
│  └─────────────────────────────────────────────────────────┘│
│                            │                                 │
│  ┌─────────────────────────┼───────────────────────────────┐│
│  │                  Public Subnets                          ││
│  │                         │                                ││
│  │              Network Load Balancer                       ││
│  │                   (Port 2181)                            ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
                             │
                        Clients
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `environment` | Environment (dev/staging/prod) | `dev` |
| `region` | AWS region | `us-east-1` |
| `cluster_size` | Number of nodes (3/5/7) | `3` |
| `instance_type` | EC2 instance type | `t3.medium` |
| `storage_size` | Storage per node (GB) | `20` |

## Production Considerations

1. **Enable remote state** - Uncomment S3 backend in `versions.tf`
2. **Multi-AZ NAT** - Set `single_nat_gateway = false` for HA
3. **Increase resources** - Use larger instance types and storage
4. **Enable encryption** - All storage is encrypted by default
5. **Configure backup** - Set up EBS snapshots

## Connecting to ZephyrCoord

```bash
# Get LoadBalancer endpoint
kubectl get svc -n zephyr-coord zephyr-coord-client

# Connect using zkCli or compatible client
zkCli.sh -server <EXTERNAL-IP>:2181
```

## Cleanup

```bash
terraform destroy
```
