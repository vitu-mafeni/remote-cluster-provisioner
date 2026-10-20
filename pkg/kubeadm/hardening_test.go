package kubeadm

import (
	"os/exec"
	"strings"
	"testing"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
)

func bashSyntax(t *testing.T, script string) {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	cmd := exec.Command("bash", "-n")
	cmd.Stdin = strings.NewReader(script)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bash -n failed: %v\n%s\n--- script ---\n%s", err, out, script)
	}
}

func TestParseKubernetesVersion(t *testing.T) {
	clean, repo, err := ParseKubernetesVersion("v1.35.2")
	if err != nil || clean != "1.35.2" || repo != "1.35" {
		t.Fatalf("got %q %q %v", clean, repo, err)
	}
	for _, bad := range []string{"", "1", "1.35.0; id", "v1.x", "1.35.0 && reboot"} {
		if _, _, err := ParseKubernetesVersion(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestCgroupV1CompatibilitySettings(t *testing.T) {
	tests := []struct {
		version, wantConfigStep, wantPreflightArg string
	}{
		{"1.34.9", "", ""},
		{"1.35.0", "if [ ! -e /sys/fs/cgroup/cgroup.controllers ]; then", " $(if [ ! -e /sys/fs/cgroup/cgroup.controllers ]; then printf '%s' '--ignore-preflight-errors=SystemVerification'; fi)"},
		{"1.36.1", "if [ ! -e /sys/fs/cgroup/cgroup.controllers ]; then", " $(if [ ! -e /sys/fs/cgroup/cgroup.controllers ]; then printf '%s' '--ignore-preflight-errors=SystemVerification'; fi)"},
		{"2.0.0", "if [ ! -e /sys/fs/cgroup/cgroup.controllers ]; then", " $(if [ ! -e /sys/fs/cgroup/cgroup.controllers ]; then printf '%s' '--ignore-preflight-errors=SystemVerification'; fi)"},
	}
	for _, tt := range tests {
		gotConfigStep, gotPreflightArg := cgroupV1CompatibilitySettings(tt.version)
		if !strings.HasPrefix(gotConfigStep, tt.wantConfigStep) || gotPreflightArg != tt.wantPreflightArg {
			t.Errorf("cgroupV1CompatibilitySettings(%q) = %q, %q; want config prefix %q and preflight arg %q", tt.version, gotConfigStep, gotPreflightArg, tt.wantConfigStep, tt.wantPreflightArg)
		}
	}
}

func TestNormalizeKubeadmConfigReplacesTabs(t *testing.T) {
	got := normalizeKubeadmConfig("featureGates:\n\tDRAConsumableCapacity: true\n")
	want := "featureGates:\n  DRAConsumableCapacity: true\n"
	if got != want {
		t.Fatalf("normalizeKubeadmConfig() = %q, want %q", got, want)
	}
}

func TestValidateJoinCommand(t *testing.T) {
	good := "kubeadm join 10.8.0.1:6443 --token abcdef.0123456789abcdef --discovery-token-ca-cert-hash sha256:0123abcd"
	if err := ValidateJoinCommand(good); err != nil {
		t.Fatalf("rejected valid join command: %v", err)
	}
	for _, bad := range []string{
		"", "rm -rf /", good + "; reboot", good + " && id", good + " | tee x", "kubeadm join $(id):6443", good + "\nid", "kubeadm join a:1 --token 'x'",
	} {
		if err := ValidateJoinCommand(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestJoinWorkerStepsResetBetweenAttempts(t *testing.T) {
	steps := joinWorkerSteps("kubeadm join 10.8.0.1:6443 --token a.b --discovery-token-ca-cert-hash sha256:00")
	all := strings.Join(steps, "\n")
	if !strings.Contains(all, "kubeadm reset --force") {
		t.Errorf("join retry loop must run kubeadm reset between attempts:\n%s", all)
	}
	for _, s := range steps {
		bashSyntax(t, s)
	}
	// A suspicious endpoint must not be embedded in the reachability wait.
	evil := joinWorkerSteps(`kubeadm join $(id):6443 --token a.b`)
	if len(evil) != 1 {
		t.Fatalf("an invalid endpoint must skip the reachability wait step, got %d steps", len(evil))
	}
	if strings.Contains(evil[0], "https://$(id)") || strings.Contains(evil[0], "node_already_joined $(id)") {
		t.Errorf("unvalidated endpoint embedded in a script:\n%s", evil[0])
	}
}

func TestInsecureRegistriesConfStep(t *testing.T) {
	step, err := insecureRegistriesConfStep(infrav1.SoftwareConfig{ImagePrepulls: []infrav1.ImagePrepull{
		{Image: "harbor.example.com:30002/team/img:1"},
		{Image: "evil';touch /tmp/x;'.com/img:1"},
		{Image: "library/nginx"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	bashSyntax(t, step)
	if !strings.Contains(step, "harbor.example.com:30002") {
		t.Errorf("valid registry missing:\n%s", step)
	}
	if strings.Contains(step, "touch") {
		t.Errorf("invalid registry host was embedded:\n%s", step)
	}
}

func TestInsecureRegistriesConfStepExplicitAndDerived(t *testing.T) {
	step, err := insecureRegistriesConfStep(infrav1.SoftwareConfig{
		InsecureRegistries: []string{"zeta.local:5000", "harbor.example.com:30002"},
		ImagePrepulls:      []infrav1.ImagePrepull{{Image: "harbor.example.com:30002/team/img:1"}, {Image: "10.0.0.5:8080/x:1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	bashSyntax(t, step)
	for _, h := range []string{"zeta.local:5000", "harbor.example.com:30002", "10.0.0.5:8080"} {
		if strings.Count(step, `location = "`+h+`"`) != 1 {
			t.Errorf("%s must appear exactly once:\n%s", h, step)
		}
	}
	if i, j, k := strings.Index(step, "10-0-0-5-8080"), strings.Index(step, "harbor-example-com-30002"), strings.Index(step, "zeta-local-5000"); !(i < j && j < k) {
		t.Errorf("hosts must be sorted:\n%s", step)
	}

	if _, err := insecureRegistriesConfStep(infrav1.SoftwareConfig{InsecureRegistries: []string{"http://harbor:80"}}); err == nil {
		t.Error("an explicit entry with a scheme must be rejected")
	}
	if s, err := insecureRegistriesConfStep(infrav1.SoftwareConfig{}); err != nil || s != "" {
		t.Errorf("no registries -> no step, got %q, %v", s, err)
	}
}

func TestYAMLStringQuotesClusterName(t *testing.T) {
	got := yamlString("prod\nkind: Evil")
	if strings.Contains(got, "\n") || !strings.HasPrefix(got, `"`) {
		t.Fatalf("clusterName not safely quoted: %q", got)
	}
}

func TestFlannelStepSyntaxAndNoPipeIntoGrepQ(t *testing.T) {
	for _, disableVPN := range []bool{false, true} {
		step := flannelIfaceStep(disableVPN)
		bashSyntax(t, "CHANGED=0\n"+step)
		if strings.Contains(step, "| grep -q") {
			t.Errorf("disableVPN=%v: steps run under pipefail; `| grep -q` can SIGPIPE the producer:\n%s", disableVPN, step)
		}
	}
}
