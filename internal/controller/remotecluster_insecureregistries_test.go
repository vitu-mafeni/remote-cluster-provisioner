package controller

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func cpWithInsecure(regs ...string) *infrav1.RemoteCluster {
	cp := node("cp", "c1", "control-plane")
	cp.Spec.VPNConfig = infrav1.VPNConfig{IP: "10.8.0.2", VPNServerPublicIP: "203.0.113.9"}
	cp.Spec.NodeInfo.SoftwareConfig.InsecureRegistries = regs
	return cp
}

func TestDesiredNetConfigFields_CarriesInsecureRegistries(t *testing.T) {
	spec := desiredNodeProvisionNetConfigFields(cpWithInsecure("harbor.example.com:30002", "reg.local"))
	want := []string{"harbor.example.com:30002", "reg.local"}
	if !reflect.DeepEqual(spec.SoftwareConfig.InsecureRegistries, want) {
		t.Errorf("insecureRegistries = %v, want %v", spec.SoftwareConfig.InsecureRegistries, want)
	}
	if got := desiredNodeProvisionNetConfigFields(cpWithInsecure()).SoftwareConfig.InsecureRegistries; len(got) != 0 {
		t.Errorf("no registries -> none synced, got %v", got)
	}
}

func TestNetConfigSyncHash_ChangesWhenInsecureRegistriesChange(t *testing.T) {
	h := func(regs ...string) string {
		s, err := netConfigSyncHash(cpWithInsecure(regs...))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	base := h()
	added := h("harbor.example.com:30002")
	if base == added {
		t.Error("adding an insecure registry must change the sync hash")
	}
	if added == h("harbor.example.com:30002", "other.local") {
		t.Error("extending the list must change the sync hash")
	}
	if added == h("harbor.example.com:30003") {
		t.Error("editing an entry must change the sync hash")
	}
	if added != h("harbor.example.com:30002") {
		t.Error("the hash must be deterministic")
	}
	if h("harbor.example.com:30002") == base {
		t.Error("removal must be detected: hash after removal must differ from the hash with the entry")
	}
}

func TestPushNetConfigViaSSH_InsecureRegistriesSetAndCleared(t *testing.T) {
	set := cpWithInsecure("harbor.example.com:30002")
	sw, _ := netConfigPatch(t, set)["softwareConfig"].(map[string]any)
	got, _ := sw["insecureRegistries"].([]any)
	if len(got) != 1 || got[0] != "harbor.example.com:30002" {
		t.Errorf("patch must carry the list, got %v", sw["insecureRegistries"])
	}

	// Removing the list must send an explicit null (merge-patch deletion):
	// omitempty alone would leave the old list on the remote object.
	cleared := cpWithInsecure()
	sw, _ = netConfigPatch(t, cleared)["softwareConfig"].(map[string]any)
	v, present := sw["insecureRegistries"]
	if !present || v != nil {
		t.Errorf("patch must contain insecureRegistries: null, got present=%v value=%v (%v)", present, v, sw)
	}
}

func TestEnsureLocalNodeProvisionNetConfig_SyncsAndClearsInsecureRegistries(t *testing.T) {
	cp := cpWithInsecure("harbor.example.com:30002")
	r := newTestReconciler(t, cp)
	ctx := context.Background()
	get := func() []string {
		nc := &mlv1alpha1.NodeProvisionNetConfig{}
		if err := r.Get(ctx, client.ObjectKey{Name: "c1-netconfig", Namespace: "default"}, nc); err != nil {
			t.Fatal(err)
		}
		return nc.Spec.SoftwareConfig.InsecureRegistries
	}
	if err := r.ensureLocalNodeProvisionNetConfig(ctx, cp, cp); err != nil {
		t.Fatal(err)
	}
	if got := get(); !reflect.DeepEqual(got, []string{"harbor.example.com:30002"}) {
		t.Errorf("created NetConfig insecureRegistries = %v", got)
	}
	cp.Spec.NodeInfo.SoftwareConfig.InsecureRegistries = nil
	if err := r.ensureLocalNodeProvisionNetConfig(ctx, cp, cp); err != nil {
		t.Fatal(err)
	}
	if got := get(); len(got) != 0 {
		t.Errorf("removing the list must clear it on the local NetConfig, got %v", got)
	}
}

func TestReconcile_InvalidInsecureRegistryFailsWithClearMessage(t *testing.T) {
	cp := cpWithInsecure("http://harbor.example.com:30002")
	cp.Finalizers = []string{remoteClusterFinalizer}
	r := newTestReconciler(t, cp)

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Error("a spec error must be re-checked later, not dropped")
	}
	got := &infrav1.RemoteCluster{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(cp), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != phaseFailed || !strings.Contains(got.Status.Message, "insecureRegistries[0]") {
		t.Errorf("want Failed with the offending entry named, got phase=%q message=%q", got.Status.Phase, got.Status.Message)
	}
}

// A merge patch only clears what it names: removing imagePrepulls or
// imagePullSecretRef from the control-plane RemoteCluster must send explicit
// nulls, otherwise the remote NodeProvisionNetConfig keeps pre-pulling the
// removed images / using the stale pull secret.
func TestNetConfigPatchBody_ClearsRemovedOptionalSoftwareFields(t *testing.T) {
	sw := func(body []byte) map[string]any {
		var got struct {
			Spec struct {
				SoftwareConfig map[string]any `json:"softwareConfig"`
			} `json:"spec"`
		}
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		return got.Spec.SoftwareConfig
	}

	cp := cpWithInsecure()
	body, err := netConfigPatchBody(cp)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"imagePrepulls", "imagePullSecretRef"} {
		if v, present := sw(body)[k]; !present || v != nil {
			t.Errorf("%s unset on the cluster: patch must carry an explicit null, got present=%v value=%v", k, present, v)
		}
	}

	cp.Spec.NodeInfo.SoftwareConfig.ImagePrepulls = []infrav1.ImagePrepull{{Image: "reg.example/a:1", NodeTarget: "all"}}
	cp.Spec.NodeInfo.SoftwareConfig.ImagePullSecretRef = &infrav1.SecretKeyReference{Name: "pull"}
	body, err = netConfigPatchBody(cp)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"imagePrepulls", "imagePullSecretRef"} {
		if v := sw(body)[k]; v == nil {
			t.Errorf("%s set on the cluster: patch must carry its value, got null/absent", k)
		}
	}
}
