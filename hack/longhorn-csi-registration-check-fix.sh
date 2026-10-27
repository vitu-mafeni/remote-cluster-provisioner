#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  hack/longhorn-csi-registration-check-fix.sh check NODE
  hack/longhorn-csi-registration-check-fix.sh fix NODE

check prints the CSINode, Longhorn CSI pod, registrar logs, and related events.
fix recreates an empty CSINode and restarts only that node's Longhorn CSI pod.
The fix prompts for the exact node name and refuses CSINodes that list any drivers.
EOF
}

action=${1:-}
node=${2:-}
namespace=${LONGHORN_NAMESPACE:-longhorn-system}

if [[ "$action" != check && "$action" != fix ]] || [[ -z "$node" ]] || [[ $# -ne 2 ]]; then
  usage >&2
  exit 2
fi

command -v kubectl >/dev/null || { echo "kubectl is required" >&2; exit 1; }
kubectl get node "$node" >/dev/null

get_drivers() {
  kubectl get csinode "$node" \
    -o jsonpath='{range .spec.drivers[*]}{.name}{"\n"}{end}' 2>/dev/null
}

get_plugin_pod() {
  kubectl -n "$namespace" get pods \
    -l app=longhorn-csi-plugin \
    --field-selector "spec.nodeName=$node" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

show_status() {
  echo "== CSINode: $node =="
  if kubectl get csinode "$node" -o yaml; then
    :
  else
    echo "CSINode not found"
  fi

  echo "== Longhorn CSI pod on $node =="
  pod=$(get_plugin_pod || true)
  if [[ -z "$pod" ]]; then
    echo "No Longhorn CSI plugin pod found on $node"
    return
  fi
  kubectl -n "$namespace" get pod "$pod" -o wide

  echo "== node-driver-registrar logs: $pod =="
  kubectl -n "$namespace" logs "$pod" -c node-driver-registrar --timestamps --tail=100 || true

  echo "== Longhorn CSI pod events: $pod =="
  kubectl -n "$namespace" describe pod "$pod" | sed -n '/Events:/,$p'

  echo "== Host socket check =="
  echo "On $node, check with: sudo ls -la /var/lib/kubelet/plugins_registry /var/lib/kubelet/plugins/driver.longhorn.io"
  echo "For kubelet registration errors, check: sudo journalctl -u kubelet --since '30 minutes ago' --no-pager | grep -Ei 'driver.longhorn.io|registration|csi'"
}

if [[ "$action" == check ]]; then
  show_status
  exit 0
fi

if ! kubectl get csinode "$node" >/dev/null 2>&1; then
  echo "CSINode $node does not exist; refusing the empty-CSINode recovery." >&2
  exit 1
fi

drivers=$(get_drivers)
if [[ -n "$drivers" ]]; then
  if grep -Fxq 'driver.longhorn.io' <<<"$drivers"; then
    echo "Longhorn CSI is already registered on $node; no repair needed."
  else
    echo "CSINode $node has other registered drivers; refusing to delete it:" >&2
    printf '%s\n' "$drivers" >&2
    exit 1
  fi
  exit 0
fi

pod=$(get_plugin_pod || true)
if [[ -z "$pod" ]]; then
  echo "No Longhorn CSI plugin pod found on $node; refusing repair." >&2
  exit 1
fi

echo "This will delete the empty CSINode $node and restart pod $namespace/$pod."
echo "CSI operations on this node may be briefly interrupted."
read -r -p "Type '$node' to continue: " confirmation
if [[ "$confirmation" != "$node" ]]; then
  echo "Cancelled."
  exit 1
fi

kubectl delete csinode "$node" --wait=true
kubectl -n "$namespace" delete pod "$pod" --wait=true

echo "Waiting for driver.longhorn.io to register on $node..."
for _ in $(seq 1 60); do
  if drivers=$(get_drivers) && grep -Fxq 'driver.longhorn.io' <<<"$drivers"; then
    echo "Longhorn CSI registered on $node."
    exit 0
  fi
  sleep 2
done

echo "Timed out waiting for CSI registration. Current diagnostics:" >&2
show_status
exit 1