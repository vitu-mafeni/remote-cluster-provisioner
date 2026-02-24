# remote-cluster-provisioner
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