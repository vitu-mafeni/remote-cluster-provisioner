#!/bin/bash
# Installs pinned versions of kind, kubebuilder and kubectl and verifies the
# SHA-256 of every download against the checksums published by each project for
# that exact version (kind: <url>.sha256sum, kubectl: <url>.sha256, kubebuilder:
# checksums.txt on the GitHub release). To upgrade, bump the version AND the
# checksum together (the checksum URLs are in the comments below).
set -euxo pipefail

KIND_VERSION=v0.30.0
# https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64.sha256sum
KIND_SHA256=517ab7fc89ddeed5fa65abf71530d90648d9638ef0c4cde22c2c11f8097b8889

# Matches cliVersion in PROJECT.
KUBEBUILDER_VERSION=v4.8.0
# https://github.com/kubernetes-sigs/kubebuilder/releases/download/${KUBEBUILDER_VERSION}/checksums.txt
KUBEBUILDER_SHA256=4f784e52845db48320cd37b579ae331149a8a61a2e13ed04230034c97a017897

KUBECTL_VERSION=v1.34.2
# https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl.sha256
KUBECTL_SHA256=9591f3d75e1581f3f7392e6ad119aab2f28ae7d6c6e083dc5d22469667f27253

# download <url> <sha256> <dest>
download() {
  local url="$1" sha="$2" dest="$3" tmp
  tmp="$(mktemp)"
  curl -fsSL -o "$tmp" "$url"
  echo "${sha}  ${tmp}" | sha256sum -c -
  chmod +x "$tmp"
  mv "$tmp" "$dest"
}

download "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64" \
  "$KIND_SHA256" /usr/local/bin/kind

download "https://github.com/kubernetes-sigs/kubebuilder/releases/download/${KUBEBUILDER_VERSION}/kubebuilder_linux_amd64" \
  "$KUBEBUILDER_SHA256" /usr/local/bin/kubebuilder

download "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
  "$KUBECTL_SHA256" /usr/local/bin/kubectl

# Ignore "already exists" on container rebuilds.
docker network create -d=bridge --subnet=172.19.0.0/24 kind || true

kind version
kubebuilder version
docker --version
go version
kubectl version --client
