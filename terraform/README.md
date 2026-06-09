# HFT Arena — Cloud Provisioning (Terraform skeleton)

Provisions a managed Kubernetes cluster to host the platform, then the
`../k8s/` manifests deploy the services onto it.

```bash
cd terraform
terraform init
terraform apply -var='project=my-gcp-project' -var='region=us-central1'
# wire kubectl to the new cluster, then:
kubectl apply -k ../k8s/
```

This is a **skeleton** demonstrating the horizontal-scaling story end-to-end
(cloud cluster → autoscaled node pool → HPA-driven bot fleet). It targets GKE by
default; the same shape applies to EKS/AKS by swapping the provider + cluster
resource. Fill in real credentials/backends before use.
