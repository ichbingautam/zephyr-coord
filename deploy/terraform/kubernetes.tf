# Kubernetes resources for ZephyrCoord

resource "kubernetes_namespace" "zephyr" {
  metadata {
    name = "zephyr-coord"
    labels = {
      name = "zephyr-coord"
    }
  }

  depends_on = [module.eks]
}

# ServiceAccount for ZephyrCoord pods
resource "kubernetes_service_account" "zephyr" {
  metadata {
    name      = "zephyr-coord"
    namespace = kubernetes_namespace.zephyr.metadata[0].name
    labels = {
      app = "zephyr-coord"
    }
  }
}

# Role with minimal permissions for peer discovery
resource "kubernetes_role" "zephyr" {
  metadata {
    name      = "zephyr-coord"
    namespace = kubernetes_namespace.zephyr.metadata[0].name
    labels = {
      app = "zephyr-coord"
    }
  }

  rule {
    api_groups = [""]
    resources  = ["pods"]
    verbs      = ["get", "list", "watch"]
  }

  rule {
    api_groups = [""]
    resources  = ["endpoints"]
    verbs      = ["get", "list", "watch"]
  }

  rule {
    api_groups = [""]
    resources  = ["configmaps"]
    verbs      = ["get", "list", "watch"]
  }
}

# RoleBinding to associate ServiceAccount with Role
resource "kubernetes_role_binding" "zephyr" {
  metadata {
    name      = "zephyr-coord"
    namespace = kubernetes_namespace.zephyr.metadata[0].name
    labels = {
      app = "zephyr-coord"
    }
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.zephyr.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.zephyr.metadata[0].name
    namespace = kubernetes_namespace.zephyr.metadata[0].name
  }
}

# NetworkPolicy for traffic restriction
resource "kubernetes_network_policy" "zephyr" {
  metadata {
    name      = "zephyr-coord"
    namespace = kubernetes_namespace.zephyr.metadata[0].name
    labels = {
      app = "zephyr-coord"
    }
  }

  spec {
    pod_selector {
      match_labels = {
        app = "zephyr-coord"
      }
    }

    policy_types = ["Ingress", "Egress"]

    # Allow client connections from any source
    ingress {
      ports {
        protocol = "TCP"
        port     = "2181"
      }
    }

    # Allow follower/election traffic only from same app
    ingress {
      from {
        pod_selector {
          match_labels = {
            app = "zephyr-coord"
          }
        }
      }
      ports {
        protocol = "TCP"
        port     = "2888"
      }
      ports {
        protocol = "TCP"
        port     = "3888"
      }
    }

    # Allow admin API from same namespace
    ingress {
      from {
        namespace_selector {}
      }
      ports {
        protocol = "TCP"
        port     = "8080"
      }
    }

    # Allow DNS resolution
    egress {
      to {
        namespace_selector {}
      }
      ports {
        protocol = "UDP"
        port     = "53"
      }
      ports {
        protocol = "TCP"
        port     = "53"
      }
    }

    # Allow cluster-internal communication
    egress {
      to {
        pod_selector {
          match_labels = {
            app = "zephyr-coord"
          }
        }
      }
      ports {
        protocol = "TCP"
        port     = "2181"
      }
      ports {
        protocol = "TCP"
        port     = "2888"
      }
      ports {
        protocol = "TCP"
        port     = "3888"
      }
    }
  }
}

resource "kubernetes_config_map" "zephyr_config" {
  metadata {
    name      = "zephyr-coord-config"
    namespace = kubernetes_namespace.zephyr.metadata[0].name
  }

  data = {
    "config.yaml" = <<-EOT
      # ZephyrCoord Configuration
      tickTime: 2000
      initLimit: 10
      syncLimit: 5
      dataDir: /data
      clientPort: 2181
      maxClientCnxns: 60
      snapCount: 100000
    EOT
  }
}

resource "kubernetes_storage_class" "zephyr_storage" {
  metadata {
    name = "zephyr-storage"
  }
  storage_provisioner = "ebs.csi.aws.com"
  reclaim_policy      = "Retain"
  volume_binding_mode = "WaitForFirstConsumer"

  parameters = {
    type      = "gp3"
    encrypted = "true"
  }
}

resource "kubernetes_stateful_set" "zephyr" {
  metadata {
    name      = "zephyr-coord"
    namespace = kubernetes_namespace.zephyr.metadata[0].name
  }

  spec {
    service_name = "zephyr-coord-headless"
    replicas     = var.cluster_size

    selector {
      match_labels = {
        app = "zephyr-coord"
      }
    }

    template {
      metadata {
        labels = {
          app = "zephyr-coord"
        }
      }

      spec {
        service_account_name             = kubernetes_service_account.zephyr.metadata[0].name
        termination_grace_period_seconds = 30

        affinity {
          pod_anti_affinity {
            required_during_scheduling_ignored_during_execution {
              label_selector {
                match_labels = {
                  app = "zephyr-coord"
                }
              }
              topology_key = "kubernetes.io/hostname"
            }
          }
        }

        container {
          name  = "zephyr-coord"
          image = var.zephyr_image

          port {
            container_port = 2181
            name           = "client"
          }
          port {
            container_port = 2888
            name           = "follower"
          }
          port {
            container_port = 3888
            name           = "election"
          }
          port {
            container_port = 8080
            name           = "admin"
          }

          env {
            name = "POD_NAME"
            value_from {
              field_ref {
                field_path = "metadata.name"
              }
            }
          }

          env {
            name  = "CLUSTER_SIZE"
            value = tostring(var.cluster_size)
          }

          resources {
            requests = {
              cpu    = "500m"
              memory = "512Mi"
            }
            limits = {
              cpu    = "2"
              memory = "2Gi"
            }
          }

          volume_mount {
            name       = "data"
            mount_path = "/data"
          }

          volume_mount {
            name       = "config"
            mount_path = "/etc/zephyr-coord"
          }

          liveness_probe {
            exec {
              command = ["sh", "-c", "echo ruok | nc localhost 2181 | grep imok"]
            }
            initial_delay_seconds = 30
            period_seconds        = 10
            timeout_seconds       = 5
          }

          readiness_probe {
            exec {
              command = ["sh", "-c", "echo ruok | nc localhost 2181 | grep imok"]
            }
            initial_delay_seconds = 10
            period_seconds        = 5
            timeout_seconds       = 3
          }
        }

        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.zephyr_config.metadata[0].name
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "data"
      }
      spec {
        access_modes       = ["ReadWriteOnce"]
        storage_class_name = kubernetes_storage_class.zephyr_storage.metadata[0].name
        resources {
          requests = {
            storage = "${var.storage_size}Gi"
          }
        }
      }
    }
  }

  depends_on = [module.eks]
}

# Headless service for peer discovery
resource "kubernetes_service" "zephyr_headless" {
  metadata {
    name      = "zephyr-coord-headless"
    namespace = kubernetes_namespace.zephyr.metadata[0].name
  }

  spec {
    cluster_ip = "None"

    selector = {
      app = "zephyr-coord"
    }

    port {
      name        = "client"
      port        = 2181
      target_port = 2181
    }
    port {
      name        = "follower"
      port        = 2888
      target_port = 2888
    }
    port {
      name        = "election"
      port        = 3888
      target_port = 3888
    }
  }
}

# LoadBalancer service for client access
resource "kubernetes_service" "zephyr_client" {
  metadata {
    name      = "zephyr-coord-client"
    namespace = kubernetes_namespace.zephyr.metadata[0].name
    annotations = {
      "service.beta.kubernetes.io/aws-load-balancer-type"            = "nlb"
      "service.beta.kubernetes.io/aws-load-balancer-nlb-target-type" = "ip"
    }
  }

  spec {
    type = "LoadBalancer"

    selector = {
      app = "zephyr-coord"
    }

    port {
      name        = "client"
      port        = 2181
      target_port = 2181
    }
  }
}

# Pod Disruption Budget
resource "kubernetes_pod_disruption_budget" "zephyr" {
  metadata {
    name      = "zephyr-coord-pdb"
    namespace = kubernetes_namespace.zephyr.metadata[0].name
  }

  spec {
    min_available = floor(var.cluster_size / 2) + 1

    selector {
      match_labels = {
        app = "zephyr-coord"
      }
    }
  }
}
