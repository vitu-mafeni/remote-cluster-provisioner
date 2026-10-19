package kubeadm

import "strings"

// LonghornPackages are the apt packages Longhorn needs on every node (from the
// Longhorn install docs): open-iscsi (iscsid) for volume attach, nfs-common as
// the NFSv4 client for RWX volumes, cryptsetup/dmsetup for encrypted volumes.
var LonghornPackages = []string{"open-iscsi", "nfs-common", "cryptsetup", "dmsetup"}

// LonghornPackagesArg is LonghornPackages as a space-separated apt argument list.
func LonghornPackagesArg() string { return strings.Join(LonghornPackages, " ") }
