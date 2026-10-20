package runtime

import (
	"reflect"
	"strings"
	"testing"
)

func TestValidateInsecureRegistry(t *testing.T) {
	good := []string{"harbor", "harbor.example.com", "harbor.example.com:30002", "10.0.0.5:8080", "a-b.c-d:1", "localhost:65535", "A1"}
	for _, h := range good {
		if err := ValidateInsecureRegistry(h); err != nil {
			t.Errorf("%q must be valid: %v", h, err)
		}
	}
	bad := []string{
		"", "http://harbor.example.com", "https://x:5000", "harbor.example.com/path", "harbor.example.com:30002/team",
		"has space.com", " harbor", "harbor ", "ha'rbor", `ha"rbor`, "a;b", "a$(id)", "a`id`", "a&b", "a|b", "a\nb", "a\\b",
		"-harbor", "harbor-", ".harbor", "harbor.", "harbor:", "harbor:abc", "harbor:0", "harbor:70000", "harbor:123456", "[::1]:5000", "a,b",
		"héllo.com", strings.Repeat("a", 300),
	}
	for _, h := range bad {
		if err := ValidateInsecureRegistry(h); err == nil {
			t.Errorf("%q must be rejected", h)
		}
	}
}

func TestValidateInsecureRegistries_NamesTheEntryAndBoundsTheList(t *testing.T) {
	err := ValidateInsecureRegistries([]string{"ok.example.com", "http://bad"})
	if err == nil || !strings.Contains(err.Error(), "insecureRegistries[1]") || !strings.Contains(err.Error(), "http://bad") {
		t.Errorf("error must name the entry, got %v", err)
	}
	many := make([]string, MaxInsecureRegistries+1)
	for i := range many {
		many[i] = "r.example.com"
	}
	if err := ValidateInsecureRegistries(many); err == nil {
		t.Error("too many entries must be rejected")
	}
	if err := ValidateInsecureRegistries(nil); err != nil {
		t.Errorf("nil list is valid: %v", err)
	}
}

func TestInsecureRegistryHosts_MergeDedupeSort(t *testing.T) {
	got, err := InsecureRegistryHosts(
		[]string{"zeta.local:5000", "harbor.example.com:30002", "zeta.local:5000"},
		[]string{
			"harbor.example.com:30002/team/img:1", // duplicate of an explicit entry
			"10.0.0.5:8080/x:1",
			"library/nginx",            // Docker Hub org, not a registry
			"nginx",                    // no host
			"evil';touch /x;'.com/i:1", // invalid host: skipped, not an error
			"docker.io/vitu1/img:2",
			"10.0.0.5:8080/y:2", // duplicate derived host
		})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"10.0.0.5:8080", "docker.io", "harbor.example.com:30002", "zeta.local:5000"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("hosts = %v, want %v", got, want)
	}

	// Only derived (no explicit list): the backward-compatible behaviour.
	got, err = InsecureRegistryHosts(nil, []string{"harbor.example.com:30002/a:1", "library/x"})
	if err != nil || !reflect.DeepEqual(got, []string{"harbor.example.com:30002"}) {
		t.Errorf("derived-only = %v, %v", got, err)
	}
	// Only explicit, host without dot/colon is accepted when explicit.
	got, err = InsecureRegistryHosts([]string{"registry"}, nil)
	if err != nil || !reflect.DeepEqual(got, []string{"registry"}) {
		t.Errorf("explicit single-label host = %v, %v", got, err)
	}
	if got, err = InsecureRegistryHosts(nil, nil); err != nil || len(got) != 0 {
		t.Errorf("empty -> empty, got %v, %v", got, err)
	}
	if _, err = InsecureRegistryHosts([]string{"http://x"}, nil); err == nil {
		t.Error("an invalid explicit entry must be an error")
	}
}

func TestInsecureRegistriesScript(t *testing.T) {
	if InsecureRegistriesScript(nil, true) != "" || InsecureRegistriesStep(nil) != "" {
		t.Error("no hosts -> empty script")
	}
	hosts := []string{"harbor.example.com:30002", "reg.local"}

	withSudo := InsecureRegistriesScript(hosts, true)
	bashSyntax(t, withSudo)
	for _, w := range []string{
		"sudo mkdir -p '/etc/containers/registries.conf.d'",
		"sudo tee '/etc/containers/registries.conf.d/50-insecure-harbor-example-com-30002.conf'",
		"sudo tee '/etc/containers/registries.conf.d/50-insecure-reg-local.conf'",
		"[[registry]]\nlocation = \"harbor.example.com:30002\"\ninsecure = true\nCNLAB_REG_EOF",
		"[[registry]]\nlocation = \"reg.local\"\ninsecure = true\nCNLAB_REG_EOF",
	} {
		if !strings.Contains(withSudo, w) {
			t.Errorf("missing %q in:\n%s", w, withSudo)
		}
	}
	if withSudo != InsecureRegistriesStep(hosts) {
		t.Error("InsecureRegistriesStep must be the sudo form")
	}

	root := InsecureRegistriesScript(hosts, false)
	bashSyntax(t, root)
	if strings.Contains(root, "sudo") {
		t.Errorf("the root (cloud-init) form must not use sudo:\n%s", root)
	}

	// Defence in depth: a host that bypassed validation is never embedded.
	evil := InsecureRegistriesScript([]string{"ok.example.com", "x';touch /tmp/pwn;'"}, true)
	if strings.Contains(evil, "pwn") || !strings.Contains(evil, "ok.example.com") {
		t.Errorf("invalid host embedded or valid host lost:\n%s", evil)
	}
	if InsecureRegistriesScript([]string{"bad host"}, true) != "" {
		t.Error("only invalid hosts -> empty script")
	}
}

// "a.b" and "a-b" map to the same file name; both must survive.
func TestInsecureRegistriesScript_FileNameCollision(t *testing.T) {
	s := InsecureRegistriesScript([]string{"a-b", "a.b"}, false)
	bashSyntax(t, s)
	if n := strings.Count(s, "| tee "); n != 2 {
		t.Fatalf("want 2 tee commands:\n%s", s)
	}
	if !strings.Contains(s, "50-insecure-a-b.conf") || !strings.Contains(s, "50-insecure-a-b-2.conf") {
		t.Errorf("colliding file names must be disambiguated:\n%s", s)
	}
}

func TestInsecureRegistryDropIn(t *testing.T) {
	path, content := InsecureRegistryDropIn("harbor.example.com:30002")
	if path != "/etc/containers/registries.conf.d/50-insecure-harbor-example-com-30002.conf" {
		t.Errorf("path = %q", path)
	}
	if content != "[[registry]]\nlocation = \"harbor.example.com:30002\"\ninsecure = true\n" {
		t.Errorf("content = %q", content)
	}
}
