# remote-cluster-provisioner
## Prerequisites
### Management cluster
### GPU Node/Cluster
- use ubuntu 22 jammy
- make passwordless user
- Install GPU Drivers


## Troubleshooting
You initialized Kubernetes but:

CNI plugins were not installed
or

/opt/cni/bin is empty
or

The CNI tarball was never extracted
```bash
sudo mkdir -p /opt/cni/bin

CNI_VERSION="v1.5.1"

wget https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-amd64-${CNI_VERSION}.tgz

sudo tar -C /opt/cni/bin -xzf cni-plugins-linux-amd64-${CNI_VERSION}.tgz

```

```bash 
kubectl delete packagevariants enterprise-gateway-variant gpu-operator-variant harbor-variant k8s-dra-driver-gpu-variant kai-scheduler-variant keycloak-variant kubeflow-variant kueue-variant kyverno-variant minio-variant nfs-provisioner-variant prometheus-stack-variant ml-platform-admin platform-overlays-variant post-install-config-variant


# add finalizer delete for

kubectl delete repository.infra.nephio.org


kubectl patch svc dex -n auth --type=json -p='[{"op":"replace","path":"/spec/selector","value":{"app":"dex"}}]'


kubectl patch kustomization ml-platform-system \
-n ml-platform-system \
--type merge \
-p '{"spec":{"suspend":true}}'
```