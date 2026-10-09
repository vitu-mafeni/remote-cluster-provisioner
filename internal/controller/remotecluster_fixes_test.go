package controller

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
)

const testSSHSecret = "ssh-pw"

func sshSecretObj() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testSSHSecret, Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("pw")},
	}
}

// okHandler answers every script with success and no output.
func okHandler(string) (string, string, int) { return "", "", 0 }

// withSSH points a node at an in-process fake SSH server (password auth).
func withSSH(n *infrav1.RemoteCluster, srv *fakeSSH) *infrav1.RemoteCluster {
	n.Spec.Host = "127.0.0.1"
	n.Spec.Port = srv.port()
	n.Spec.User = "u"
	n.Spec.Auth = infrav1.RemoteClusterAuth{PasswordSecretRef: &infrav1.SecretKeyReference{Name: testSSHSecret, Key: "password"}}
	return n
}

// withDeadSSH points a node at a local port nothing listens on.
func withDeadSSH(t *testing.T, n *infrav1.RemoteCluster) *infrav1.RemoteCluster {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	n.Spec.Host = "127.0.0.1"
	n.Spec.Port = strconv.Itoa(port)
	n.Spec.User = "u"
	n.Spec.Auth = infrav1.RemoteClusterAuth{PasswordSecretRef: &infrav1.SecretKeyReference{Name: testSSHSecret, Key: "password"}}
	return n
}

// novpnNode is a node in a VPN-less cluster; host is its SSH endpoint, while
// the node IP is discovered from the host's primary local route.
func novpnNode(name, nodeType string) *infrav1.RemoteCluster {
	n := node(name, "c1", nodeType)
	n.Spec.DisableVPN = true
	n.Annotations = map[string]string{annotationProvisionedVPNMode: vpnModeNoVPN}
	return n
}

// vpnMode turns a node into one provisioned with the VPN (no VPN IP is set, so
// the local fake server is still reachable through spec.host).
func vpnMode(ns ...*infrav1.RemoteCluster) {
	for _, n := range ns {
		n.Spec.DisableVPN = false
		n.Annotations[annotationProvisionedVPNMode] = vpnModeVPN
	}
}

func readyCP(srv *fakeSSH) *infrav1.RemoteCluster {
	cp := withSSH(novpnNode("cp", "control-plane"), srv)
	cp.Status.Phase = phaseReady
	cp.Status.JoinCommand = "kubeadm join 127.0.0.1:6443 --token abcdef.0123456789abcdef --discovery-token-ca-cert-hash sha256:" + strings.Repeat("a", 64)
	return cp
}

func reqFor(o client.Object) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKeyFromObject(o)}
}

func cpHandler(failUsedIP *atomic.Int32) func(string) (string, string, int) {
	return func(script string) (string, string, int) {
		switch {
		case strings.Contains(script, "usedIPAddresses/-") && failUsedIP != nil && failUsedIP.Add(-1) >= 0:
			return "", "boom", 1
		case strings.Contains(script, "kubectl get nodes"):
			return "cp Ready control-plane 5d v1 10.8.0.2 <none> Ubuntu 5.15 cri-o\nw-host Ready <none> 1d v1 127.0.0.1 <none> Ubuntu 5.15 cri-o\n",
				"sudo: unable to resolve host cp\n", 0
		}
		return "", "", 0
	}
}

// --- finding 2 / 10: labelling is part of the retried finalization ----------

func TestFinalizeWorkerJoin_FailsOnceThenRetryLabelsNode(t *testing.T) {
	fail := &atomic.Int32{}
	fail.Store(1) // the used-IP update fails on the first attempt only
	cpSrv := startFakeSSH(t, cpHandler(fail))
	wSrv := startFakeSSH(t, func(s string) (string, string, int) {
		if strings.Contains(s, "hostname") {
			return "W-Host\n", "sudo: unable to resolve host x\n", 0
		}
		return "", "", 0
	})
	cp := readyCP(cpSrv)
	w := withSSH(node("w", "c1", "worker"), wSrv)
	w.Spec.NodeInfo.HardwareType = "GPU"
	w.Status.Phase = phaseProvisioning
	w.Status.Conditions = []metav1.Condition{{Type: "WorkerJoinFailed", Status: metav1.ConditionFalse, Reason: "WorkerJoinFailed", LastTransitionTime: metav1.Now()}}
	w.Annotations = map[string]string{
		annotationWorkerJoined: "true", annotationWorkerFinalizePending: "10.8.0.5", annotationProvisionedVPNMode: vpnModeVPN,
	}
	cp.Annotations[annotationProvisionedVPNMode] = vpnModeVPN
	cp.Spec.DisableVPN = false
	r := newTestReconciler(t, sshSecretObj(), cp, w)
	ctx := context.Background()

	cpClient, err := r.getSSHClient(ctx, cp)
	if err != nil {
		t.Fatal(err)
	}
	defer cpClient.Conn.Close()

	// Attempt 1: the worker becomes Ready, then the used-IP update fails.
	cur := getRC(t, r, "w")
	if err := r.finalizeWorkerJoin(ctx, cur, cp, cpClient); err == nil {
		t.Fatal("expected the first finalization to fail on the used-IP update")
	}
	got := getRC(t, r, "w")
	if got.Status.Phase != phaseReady || got.Annotations[annotationWorkerFinalizePending] != "10.8.0.5" {
		t.Fatalf("after the failure: phase=%q marker=%q (must stay Ready with the marker kept)", got.Status.Phase, got.Annotations[annotationWorkerFinalizePending])
	}
	if cpSrv.ran("kubectl label") != 0 {
		t.Fatal("labelling must not run before the IP was recorded")
	}

	// Attempt 2 (what the Ready-branch retry does): labels the node and clears the marker.
	if err := r.finalizeWorkerJoin(ctx, got, cp, cpClient); err != nil {
		t.Fatal(err)
	}
	label := cpSrv.find("kubectl label")
	for _, want := range []string{"'w-host'", "infra.dcn.ssu.ac.kr/worker=true", "hardware-type='gpu'", "--overwrite"} {
		if !strings.Contains(label, want) {
			t.Errorf("label command lacks %q: %q", want, label)
		}
	}
	got = getRC(t, r, "w")
	if got.Annotations[annotationWorkerFinalizePending] != "" {
		t.Error("marker must be cleared once labelling succeeded")
	}
	if got.Annotations[annotationWorkerJoined] != "true" {
		t.Error("joined annotation must be kept")
	}
	if apimeta.FindStatusCondition(got.Status.Conditions, "WorkerJoinFailed") != nil {
		t.Error("stale WorkerJoinFailed condition must be gone once the worker is Ready")
	}
	if n := cpSrv.ran("usedIPAddresses/-"); n != 2 {
		t.Errorf("used-IP update should have been attempted twice, ran %d", n)
	}
	// Idempotent: a third run only repeats the harmless --overwrite label.
	if err := r.finalizeWorkerJoin(ctx, getRC(t, r, "w"), cp, cpClient); err != nil {
		t.Fatal(err)
	}
}

func TestFinalizeWorkerJoin_LabelFailureKeepsMarkerAndResolvesNameViaControlPlane(t *testing.T) {
	failLabel := &atomic.Bool{}
	failLabel.Store(true)
	cpSrv := startFakeSSH(t, func(s string) (string, string, int) {
		if strings.Contains(s, "kubectl label") && failLabel.Load() {
			return "", "no such node", 1
		}
		return cpHandler(nil)(s)
	})
	cp := readyCP(cpSrv)
	// The worker is unreachable: its node name comes from its IP on the control-plane.
	w := withDeadSSH(t, novpnNode("w", "worker"))
	w.Status.Phase = phaseReady
	w.Annotations[annotationWorkerJoined] = "true"
	w.Annotations[annotationWorkerFinalizePending] = "none"
	r := newTestReconciler(t, sshSecretObj(), cp, w)
	ctx := context.Background()
	cpClient, err := r.getSSHClient(ctx, cp)
	if err != nil {
		t.Fatal(err)
	}
	defer cpClient.Conn.Close()

	if err := r.finalizeWorkerJoin(ctx, getRC(t, r, "w"), cp, cpClient); err == nil {
		t.Fatal("a failed label must fail the finalization")
	}
	if getRC(t, r, "w").Annotations[annotationWorkerFinalizePending] != "none" {
		t.Fatal("marker must survive a labelling failure")
	}
	failLabel.Store(false)
	if err := r.finalizeWorkerJoin(ctx, getRC(t, r, "w"), cp, cpClient); err != nil {
		t.Fatal(err)
	}
	if l := cpSrv.find("kubectl label"); !strings.Contains(l, "'w-host'") || !strings.Contains(l, "hardware-type='cpu'") {
		t.Errorf("label command: %q", l)
	}
	if getRC(t, r, "w").Annotations[annotationWorkerFinalizePending] != "" {
		t.Error("marker not cleared")
	}
}

func TestFinalizeWorkerJoin_UnresolvableNodeNameIsAnError(t *testing.T) {
	cpSrv := startFakeSSH(t, okHandler) // empty node table
	cp := readyCP(cpSrv)
	w := withDeadSSH(t, novpnNode("w", "worker"))
	w.Annotations[annotationWorkerFinalizePending] = "none"
	r := newTestReconciler(t, sshSecretObj(), cp, w)
	cpClient, err := r.getSSHClient(context.Background(), cp)
	if err != nil {
		t.Fatal(err)
	}
	defer cpClient.Conn.Close()
	err = r.finalizeWorkerJoin(context.Background(), getRC(t, r, "w"), cp, cpClient)
	if err == nil || !strings.Contains(err.Error(), "node name") {
		t.Fatalf("want a node-name error, got %v", err)
	}
	if cpSrv.ran("kubectl label") != 0 {
		t.Error("must never label a guessed node")
	}
}

// --- finding 2/4 / 10: Ready-worker retry WITH a control-plane --------------

func TestReconcile_ReadyWorkerWithPendingFinalizeIsFinalizedViaControlPlane(t *testing.T) {
	cpSrv := startFakeSSH(t, cpHandler(nil))
	cp := readyCP(cpSrv)
	w := withDeadSSH(t, novpnNode("w", "worker"))
	w.Status.Phase = phaseReady
	w.Annotations[annotationWorkerJoined] = "true"
	w.Annotations[annotationWorkerFinalizePending] = "10.8.0.5"
	vpnMode(cp, w)
	r := newTestReconciler(t, sshSecretObj(), cp, w)

	res, err := r.Reconcile(context.Background(), reqFor(w))
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter != 0 || res.Requeue {
		t.Errorf("finalized worker needs no further wake-up, got %+v", res)
	}
	if cpSrv.ran("usedIPAddresses/-") != 1 || cpSrv.ran("kubectl label") != 1 {
		t.Errorf("expected one used-IP update and one label, got %d / %d", cpSrv.ran("usedIPAddresses/-"), cpSrv.ran("kubectl label"))
	}
	if got := getRC(t, r, "w"); got.Annotations[annotationWorkerFinalizePending] != "" {
		t.Error("marker not cleared")
	}
}

// --- finding 4: joined worker never needs its own SSH connection ------------

func TestReconcile_JoinedWorkerIsFinalizedWithoutWorkerSSHAndReturnsToReady(t *testing.T) {
	for _, phase := range []string{phaseFailed, phaseProvisioning, ""} {
		cpSrv := startFakeSSH(t, cpHandler(nil))
		cp := readyCP(cpSrv)
		w := withDeadSSH(t, novpnNode("w", "worker")) // the worker host is unreachable
		w.Status.Phase = phase
		w.Status.ProvisionRetryCount = maxProvisionRetries // even a terminal Failed recovers
		w.Status.Conditions = []metav1.Condition{
			{Type: "SSHConnectionFailed", Status: metav1.ConditionFalse, Reason: "SSHConnectionFailed", LastTransitionTime: metav1.Now()},
		}
		w.Annotations[annotationWorkerJoined] = "true"
		w.Annotations[annotationWorkerFinalizePending] = "10.8.0.5"
		r := newTestReconciler(t, sshSecretObj(), cp, w)

		res, err := r.Reconcile(context.Background(), reqFor(w))
		if err != nil {
			t.Fatalf("phase %q: %v", phase, err)
		}
		got := getRC(t, r, "w")
		if got.Status.Phase != phaseReady {
			t.Errorf("phase %q: joined worker ended in %q, want Ready (message %q)", phase, got.Status.Phase, got.Status.Message)
		}
		if got.Status.ProvisionRetryCount != 0 {
			t.Errorf("phase %q: retry budget not reset: %d", phase, got.Status.ProvisionRetryCount)
		}
		if apimeta.FindStatusCondition(got.Status.Conditions, "SSHConnectionFailed") != nil {
			t.Errorf("phase %q: stale SSHConnectionFailed condition left on a Ready worker", phase)
		}
		if got.Annotations[annotationWorkerFinalizePending] != "" {
			t.Errorf("phase %q: finalize marker not cleared (res %+v)", phase, res)
		}
	}
}

func TestReconcile_JoinedWorkerWaitsForReadyControlPlane(t *testing.T) {
	cp := node("cp", "c1", "control-plane") // not Ready yet
	w := novpnNode("w", "worker")
	w.Status.Phase = phaseFailed
	w.Annotations[annotationWorkerJoined] = "true"
	w.Annotations[annotationWorkerFinalizePending] = "none"
	r := newTestReconciler(t, cp, w)
	res, err := r.Reconcile(context.Background(), reqFor(w))
	if err != nil || res.RequeueAfter <= 0 {
		t.Fatalf("want a delayed requeue, got %+v err=%v", res, err)
	}
	got := getRC(t, r, "w")
	if got.Status.Phase != phaseFailed || got.Status.ProvisionRetryCount != 0 {
		t.Errorf("waiting for the control-plane must not change the worker: %q / %d", got.Status.Phase, got.Status.ProvisionRetryCount)
	}
}

// --- finding 1 / 10: NodeProvisionNetConfig follows the RECORDED VPN mode ---

func TestHandleCreateUpdateNodeProvisionConfig_UsesRecordedModeNotSpec(t *testing.T) {
	cpSrv := startFakeSSH(t, func(s string) (string, string, int) {
		if strings.Contains(s, "ip -4 addr show wg0") {
			return "10.8.0.2\n", "sudo: unable to resolve host cp\n", 0
		}
		return "", "", 0
	})
	// Provisioned WITH the VPN, then spec.disableVPN was flipped.
	cp := withSSH(node("cp", "c1", "control-plane"), cpSrv)
	cp.Spec.DisableVPN = true
	cp.Spec.VPNConfig = infrav1.VPNConfig{IP: "10.8.0.2", VPNServerPublicIP: "203.0.113.9"}
	cp.Status.JoinCommand = "kubeadm join x"
	cp.Annotations = map[string]string{annotationProvisionedVPNMode: vpnModeVPN}
	r := newTestReconciler(t, sshSecretObj(), cp)
	ctx := context.Background()
	// The node is addressed through its VPN IP normally; the test host is local.
	conn := *cp
	conn.Spec.VPNConfig.IP = ""
	cpClient, err := r.getSSHClient(ctx, &conn)
	if err != nil {
		t.Fatal(err)
	}
	defer cpClient.Conn.Close()

	if _, err := r.handleCreateUpdateNodeProvisionConfig(ctx, cp, cp, cpClient, cp.Spec.VPNConfig.IP, "create"); err != nil {
		t.Fatal(err)
	}
	if cpSrv.ran("ip -4 addr show wg0") != 1 {
		t.Error("the wg0 address must be used for a node provisioned with the VPN")
	}
	nc := cpSrv.find("kind: NodeProvisionNetConfig\nmetadata")
	if !strings.Contains(nc, "disableVPN: false") || !strings.Contains(nc, "vpnRange:") {
		t.Errorf("remote NetConfig must describe the provisioned (VPN) mode:\n%s", nc)
	}
	if st := cpSrv.find("--subresource=status -p"); !strings.Contains(st, "usedIPAddresses") || !strings.Contains(st, "10.8.0.2") {
		t.Errorf("the VPN address must be recorded in usedIPAddresses: %q", st)
	}
	if !cp.Spec.DisableVPN {
		t.Error("the caller's object must not be modified")
	}
}

func TestHandleCreateUpdateNodeProvisionConfig_UpdateRecordsVPNWorkerIPDespiteFlippedSpec(t *testing.T) {
	cpSrv := startFakeSSH(t, okHandler)
	cp := withSSH(node("cp", "c1", "control-plane"), cpSrv)
	cp.Annotations = map[string]string{annotationProvisionedVPNMode: vpnModeVPN}
	w := node("w", "c1", "worker")
	w.Spec.DisableVPN = true // flipped after the worker was provisioned with the VPN
	w.Annotations = map[string]string{annotationProvisionedVPNMode: vpnModeVPN}
	r := newTestReconciler(t, sshSecretObj(), cp, w)
	cpClient, err := r.getSSHClient(context.Background(), cp)
	if err != nil {
		t.Fatal(err)
	}
	defer cpClient.Conn.Close()

	if _, err := r.handleCreateUpdateNodeProvisionConfig(context.Background(), w, cp, cpClient, "10.8.0.7", "update"); err != nil {
		t.Fatal(err)
	}
	if s := cpSrv.find("usedIPAddresses/-"); !strings.Contains(s, "10.8.0.7") {
		t.Errorf("the VPN worker's IP must be recorded, ran: %q", s)
	}

	// The opposite: recorded no-VPN worker never records an IP even if the spec says VPN.
	w2 := node("w2", "c1", "worker")
	w2.Annotations = map[string]string{annotationProvisionedVPNMode: vpnModeNoVPN}
	before := cpSrv.ran("usedIPAddresses/-")
	if _, err := r.handleCreateUpdateNodeProvisionConfig(context.Background(), w2, cp, cpClient, "192.0.2.9", "update"); err != nil {
		t.Fatal(err)
	}
	if cpSrv.ran("usedIPAddresses/-") != before {
		t.Error("a node provisioned without the VPN must not be recorded in usedIPAddresses")
	}
}

// --- finding 3 / 10: the token refresh is always rescheduled ----------------

func TestReconcilePackageVariants_CompletionSchedulesTokenRefresh(t *testing.T) {
	cp := node("cp", "c1", "control-plane")
	cp.Status.Phase = phaseReady
	cp.Status.JoinCommand = "kubeadm join x"
	cp.Annotations = map[string]string{
		annotationCoreVariantsCreated:  "true",
		annotationJoinTokenRefreshedAt: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
	}
	r := newTestReconciler(t, cp)

	res, err := r.reconcilePackageVariants(context.Background(), getRC(t, r, "cp"))
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > tokenRefreshInterval {
		t.Fatalf("RequeueAfter = %v, want in (0, %v]", res.RequeueAfter, tokenRefreshInterval)
	}
	if want := tokenRefreshInterval - time.Hour; res.RequeueAfter > want || res.RequeueAfter < want-time.Minute {
		t.Errorf("RequeueAfter = %v, want the remaining token lifetime (~%v)", res.RequeueAfter, want)
	}
	if getRC(t, r, "cp").Annotations[annotationPkgVariantsCreated] != "true" {
		t.Error("package-variants-created not stamped")
	}
}

func TestTokenRequeueAfter_IsAlwaysPositiveAndBounded(t *testing.T) {
	at := func(d time.Duration) *infrav1.RemoteCluster {
		c := node("cp", "c1", "control-plane")
		c.Annotations = map[string]string{annotationJoinTokenRefreshedAt: time.Now().Add(d).UTC().Format(time.RFC3339)}
		return c
	}
	for name, c := range map[string]*infrav1.RemoteCluster{
		"absent":        node("cp", "c1", "control-plane"),
		"garbage":       {ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{annotationJoinTokenRefreshedAt: "x"}}},
		"fresh":         at(0),
		"half":          at(-tokenRefreshInterval / 2),
		"overdue":       at(-2 * tokenRefreshInterval),
		"just overdue":  at(-tokenRefreshInterval - time.Second),
		"clock skew":    at(48 * time.Hour),
		"nearly due":    at(-tokenRefreshInterval + time.Second),
		"exactly later": at(-tokenRefreshInterval + 30*time.Second),
	} {
		if d := tokenRequeueAfter(c); d <= 0 || d > tokenRefreshInterval {
			t.Errorf("%s: %v not in (0, %v]", name, d, tokenRefreshInterval)
		}
	}
	if d := tokenRequeueAfter(at(-tokenRefreshInterval - time.Hour)); d != softFailInterval {
		t.Errorf("an overdue token (refresh failed) must be retried after %v, got %v", softFailInterval, d)
	}
}

func TestReconcile_ReadyControlPlaneRefreshesOverdueTokenFromStdoutOnly(t *testing.T) {
	const joinCmd = "kubeadm join 127.0.0.1:6443 --token zzzzzz.0123456789abcdef --discovery-token-ca-cert-hash sha256:"
	srv := startFakeSSH(t, func(s string) (string, string, int) {
		if strings.Contains(s, "kubeadm token create") {
			return joinCmd + strings.Repeat("b", 64) + "\n", "sudo: unable to resolve host cp\n", 0
		}
		return "", "", 0
	})
	cp := readyCP(srv)
	cp.Annotations[annotationNodeProvisionCreated] = "true"
	cp.Annotations[annotationPkgVariantsCreated] = "true"
	cp.Annotations[annotationJoinTokenRefreshedAt] = time.Now().Add(-30 * time.Hour).UTC().Format(time.RFC3339)
	r := newTestReconciler(t, sshSecretObj(), cp)

	res, err := r.Reconcile(context.Background(), reqFor(cp))
	if err != nil {
		t.Fatal(err)
	}
	got := getRC(t, r, "cp")
	if !strings.HasPrefix(got.Status.JoinCommand, joinCmd) || strings.Contains(got.Status.JoinCommand, "sudo:") || strings.Contains(got.Status.JoinCommand, "\n") {
		t.Errorf("join command must be the stdout only: %q", got.Status.JoinCommand)
	}
	if res.RequeueAfter < tokenRefreshInterval-time.Minute || res.RequeueAfter > tokenRefreshInterval {
		t.Errorf("after a refresh the next wake-up is a full interval away, got %v", res.RequeueAfter)
	}
}

// --- finding 6: the VPN mode is recorded late and released on early failure -

func TestReconcileControlPlane_UnreachableHostDoesNotRecordVPNMode(t *testing.T) {
	cp := node("cp", "c1", "control-plane") // no auth: the SSH connection cannot be made
	cp.Spec.DisableVPN = true
	r := newTestReconciler(t, cp)

	if _, err := r.reconcileControlPlane(context.Background(), getRC(t, r, "cp")); err != nil {
		t.Fatal(err)
	}
	got := getRC(t, r, "cp")
	if got.Status.Phase != phaseFailed {
		t.Fatalf("phase = %q", got.Status.Phase)
	}
	if v, ok := got.Annotations[annotationProvisionedVPNMode]; ok {
		t.Fatalf("VPN mode %q recorded although the host was never reached", v)
	}
	// The user corrects the spec: the edit is honoured, not "ignored".
	got.Spec.DisableVPN = false
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if _, done, err := r.reconcileVPNMode(context.Background(), getRC(t, r, "cp")); done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	if c := apimeta.FindStatusCondition(getRC(t, r, "cp").Status.Conditions, conditionVPNModeChangeIgnored); c != nil {
		t.Errorf("corrected spec must not be reported as ignored: %+v", c)
	}
}

func cpJobWithResult(cp *infrav1.RemoteCluster, res controlPlaneJobResult) *controlPlaneJob {
	ch := make(chan controlPlaneJobResult, 1)
	ch <- res
	return &controlPlaneJob{ch: ch, uid: string(cp.UID)}
}

func TestReconcileControlPlane_FailureBeforeAnyPhaseReleasesVPNModeButKeepsItAfterProgress(t *testing.T) {
	for _, tc := range []struct {
		name     string
		progress string // annotationLastCompletedPhaseCP ("" = none)
		wantMode string
	}{
		{"nothing completed", "", ""},
		{"a phase completed", "3", vpnModeNoVPN},
	} {
		cp := node("cp", "c1", "control-plane")
		cp.Spec.DisableVPN = true
		cp.Annotations = map[string]string{annotationProvisionedVPNMode: vpnModeNoVPN}
		if tc.progress != "" {
			cp.Annotations[annotationLastCompletedPhaseCP] = tc.progress
		}
		r := newTestReconciler(t, cp)
		r.controlPlaneJobs.Store("default/cp", cpJobWithResult(cp, controlPlaneJobResult{err: context.DeadlineExceeded}))

		if _, err := r.reconcileControlPlane(context.Background(), getRC(t, r, "cp")); err != nil {
			t.Fatal(err)
		}
		got := getRC(t, r, "cp")
		if got.Status.Phase != phaseFailed || got.Status.ProvisionRetryCount != 1 {
			t.Errorf("%s: phase=%q count=%d", tc.name, got.Status.Phase, got.Status.ProvisionRetryCount)
		}
		if mode := got.Annotations[annotationProvisionedVPNMode]; mode != tc.wantMode {
			t.Errorf("%s: recorded mode = %q, want %q", tc.name, mode, tc.wantMode)
		}
		if tc.progress != "" && got.Annotations[annotationLastCompletedPhaseCP] != tc.progress {
			t.Errorf("%s: phase progress annotation must be untouched, got %q", tc.name, got.Annotations[annotationLastCompletedPhaseCP])
		}
	}
}

func TestReconcileWorker_JoinFailureBeforeAnyPhaseReleasesVPNMode(t *testing.T) {
	cpSrv := startFakeSSH(t, okHandler)
	wSrv := startFakeSSH(t, okHandler)
	cp := readyCP(cpSrv)
	cp.Status.JoinCommand = "not a join command" // rejected by JoinWorkerNode before any phase runs
	w := withSSH(node("w", "c1", "worker"), wSrv)
	w.Spec.DisableVPN = true
	r := newTestReconciler(t, sshSecretObj(), cp, w)
	ctx := context.Background()
	workerClient, err := r.getSSHClient(ctx, w)
	if err != nil {
		t.Fatal(err)
	}
	defer workerClient.Conn.Close()

	if _, err := r.reconcileWorker(ctx, getRC(t, r, "w"), workerClient); err != nil {
		t.Fatal(err)
	}
	got := getRC(t, r, "w")
	if got.Status.Phase != phaseFailed || got.Status.ProvisionRetryCount != 1 {
		t.Fatalf("phase=%q count=%d (%s)", got.Status.Phase, got.Status.ProvisionRetryCount, got.Status.Message)
	}
	if v, ok := got.Annotations[annotationProvisionedVPNMode]; ok {
		t.Errorf("a worker that never completed a phase must not keep VPN mode %q", v)
	}
}

func TestReconcileVPNMode_StaleRecordOfUntouchedWorkerDoesNotWedgeIt(t *testing.T) {
	cp := node("cp", "c1", "control-plane")
	cp.Spec.DisableVPN = true
	cp.Annotations = map[string]string{annotationProvisionedVPNMode: vpnModeNoVPN}
	// Older versions recorded the mode before connecting; the worker never got further.
	w := node("w", "c1", "worker")
	w.Status.Phase = phaseFailed
	w.Annotations = map[string]string{annotationProvisionedVPNMode: vpnModeVPN}
	r := newTestReconciler(t, cp, w)

	if _, done, err := r.reconcileVPNMode(context.Background(), getRC(t, r, "w")); err != nil || !done {
		// unprovisioned worker asking for VPN on a VPN-less cluster inherits: update + requeue
		t.Fatalf("done=%v err=%v", done, err)
	}
	got := getRC(t, r, "w")
	if _, ok := got.Annotations[annotationProvisionedVPNMode]; ok {
		t.Error("the stale record must be released")
	}
	if !got.Spec.DisableVPN {
		t.Error("the never-provisioned worker must inherit the control-plane's mode instead of being stuck in a mismatch")
	}
	if c := apimeta.FindStatusCondition(got.Status.Conditions, conditionVPNModeMismatch); c != nil {
		t.Errorf("no permanent mismatch expected: %+v", c)
	}

	// A worker that completed a phase keeps its record (and the mismatch is reported).
	w2 := node("w2", "c1", "worker")
	w2.Annotations = map[string]string{annotationProvisionedVPNMode: vpnModeVPN, annotationLastCompletedPhaseWorker: "2"}
	r2 := newTestReconciler(t, cp, w2)
	res, done, err := r2.reconcileVPNMode(context.Background(), getRC(t, r2, "w2"))
	if err != nil || !done || res.RequeueAfter != time.Minute {
		t.Fatalf("touched worker: res=%+v done=%v err=%v", res, done, err)
	}
	if getRC(t, r2, "w2").Annotations[annotationProvisionedVPNMode] != vpnModeVPN {
		t.Error("a touched worker must keep its record")
	}
}

// --- finding 7: cnlab credential-sync recovery with a stale object ----------

func TestSyncCnlabCredentials_RecoveryIsPersistedEvenWithStaleObject(t *testing.T) {
	srv := startFakeSSH(t, okHandler)
	creds := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "reg", Namespace: "default"},
		Data:       map[string][]byte{"username": []byte("u"), "token": []byte("t")},
	}
	cp := readyCP(srv)
	cp.Spec.NodeInfo.SoftwareConfig.CnlabRuntime = &infrav1.CnlabRuntimeConfig{CredentialsRef: infrav1.VPNSSHCredentialsRef{Name: "reg"}}
	cp.Status.CnlabSyncRetryCount = 3
	r := newTestReconciler(t, sshSecretObj(), creds, cp)
	ctx := context.Background()

	stale := getRC(t, r, "cp")
	// Someone else writes the object after we read it (the SSH work takes long).
	other := getRC(t, r, "cp")
	other.Status.Message = "concurrent write"
	if err := r.Status().Update(ctx, other); err != nil {
		t.Fatal(err)
	}

	if err := r.syncCnlabCredentialsToRemote(ctx, stale); err != nil {
		t.Fatal(err)
	}
	got := getRC(t, r, "cp")
	if got.Status.CnlabSyncRetryCount != 0 {
		t.Errorf("retry counter not reset on a stale object: %d", got.Status.CnlabSyncRetryCount)
	}
	if c := apimeta.FindStatusCondition(got.Status.Conditions, cnlabSyncConditionType); c == nil || c.Status != metav1.ConditionTrue || c.Reason != "SyncSucceeded" {
		t.Errorf("success condition missing: %+v", c)
	}
	if got.Annotations[annotationCnlabCredentialsHash] == "" {
		t.Error("credentials hash not stamped")
	}
	if got.Status.Message != "concurrent write" {
		t.Errorf("the concurrent write was clobbered: %q", got.Status.Message)
	}
}

// --- finding 8: failure conditions do not survive a transition to Ready -----

func TestRecoverControlPlane_ClearsStaleFailureConditions(t *testing.T) {
	cp := node("cp", "c1", "control-plane")
	cp.Status.Phase = phaseFailed
	cp.Status.JoinCommand = "kubeadm join x"
	mk := func(typ string, st metav1.ConditionStatus) metav1.Condition {
		return metav1.Condition{Type: typ, Status: st, Reason: typ, LastTransitionTime: metav1.Now()}
	}
	cp.Status.Conditions = []metav1.Condition{
		mk("ControlPlaneInitFailed", metav1.ConditionFalse),
		mk("SSHConnectionFailed", metav1.ConditionFalse),
		mk("ClusterRepoFailed", metav1.ConditionFalse),
		mk(cnlabSyncConditionType, metav1.ConditionFalse), // has its own lifecycle
		mk(conditionVPNModeChangeIgnored, metav1.ConditionTrue),
	}
	r := newTestReconciler(t, cp)

	if _, err := r.recoverControlPlane(context.Background(), getRC(t, r, "cp")); err != nil {
		t.Fatal(err)
	}
	got := getRC(t, r, "cp")
	if got.Status.Phase != phaseReady {
		t.Fatalf("phase = %q", got.Status.Phase)
	}
	for _, typ := range []string{"ControlPlaneInitFailed", "SSHConnectionFailed", "ClusterRepoFailed"} {
		if apimeta.FindStatusCondition(got.Status.Conditions, typ) != nil {
			t.Errorf("stale %s=False left on a Ready cluster", typ)
		}
	}
	for _, typ := range []string{cnlabSyncConditionType, conditionVPNModeChangeIgnored, "Recovered"} {
		if apimeta.FindStatusCondition(got.Status.Conditions, typ) == nil {
			t.Errorf("condition %s must be kept/added", typ)
		}
	}
}

func TestSetStatus_ReadyKeepsFailureConditionsOnError(t *testing.T) {
	cp := node("cp", "c1", "control-plane")
	r := newTestReconciler(t, cp)
	c := getRC(t, r, "cp")
	if err := r.setStatus(context.Background(), c, phaseFailed, "SSHConnectionFailed", "x", true); err != nil {
		t.Fatal(err)
	}
	if apimeta.FindStatusCondition(getRC(t, r, "cp").Status.Conditions, "SSHConnectionFailed") == nil {
		t.Fatal("a failure must be recorded")
	}
}

// --- finding 9 ---------------------------------------------------------------

func TestTruncateMessage_IsRuneSafe(t *testing.T) {
	// "€" is 3 bytes: a cut at the byte limit lands inside a rune.
	in := strings.Repeat("a", maxConditionMessage-1) + strings.Repeat("€", 10)
	got := truncateMessage(in)
	if !utf8.ValidString(got) {
		t.Fatalf("truncated message is not valid UTF-8: %q", got[len(got)-8:])
	}
	if !strings.HasSuffix(got, "…") || len(got) > maxConditionMessage+len("…") {
		t.Errorf("unexpected truncation result (len %d)", len(got))
	}
	if short := "héllo"; truncateMessage(short) != short {
		t.Error("short messages must be untouched")
	}
}

func TestHandleDelete_StaleObjectStillRemovesFinalizer(t *testing.T) {
	cp := deleting(node("cp", "c1", "control-plane")) // no auth: node reset fails fast
	r := newTestReconciler(t, cp)
	ctx := context.Background()
	stale := getRC(t, r, "cp")

	// Something else touches the object while the (slow) cleanup runs.
	other := getRC(t, r, "cp")
	other.Annotations = map[string]string{"touched": "yes"}
	if err := r.Update(ctx, other); err != nil {
		t.Fatal(err)
	}

	if _, err := r.handleDelete(ctx, stale); err != nil {
		t.Fatalf("a stale object must not turn into a conflict that re-runs the cleanup: %v", err)
	}
	err := r.Get(ctx, types.NamespacedName{Namespace: "default", Name: "cp"}, &infrav1.RemoteCluster{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("object should be gone once its finalizer is removed, got err=%v", err)
	}
}

func TestHandleDelete_SkipsNodeCleanupWhenAlreadyAttempted(t *testing.T) {
	for _, done := range []bool{false, true} {
		cpSrv := startFakeSSH(t, cpHandler(nil))
		wSrv := startFakeSSH(t, func(s string) (string, string, int) {
			if strings.Contains(s, "hostname") {
				return "w-host\n", "", 0
			}
			return "", "", 0
		})
		cp := readyCP(cpSrv)
		w := deleting(withSSH(novpnNode("w", "worker"), wSrv))
		if done {
			w.Annotations[annotationDeleteNodeCleanupDone] = "true"
		}
		r := newTestReconciler(t, sshSecretObj(), cp, w)

		if _, err := r.handleDelete(context.Background(), getRC(t, r, "w")); err != nil {
			t.Fatal(err)
		}
		resets, drains := wSrv.ran("kubeadm reset"), cpSrv.ran("kubectl drain")
		if done && (resets != 0 || drains != 0) {
			t.Errorf("re-entered delete repeated the node cleanup: %d resets, %d drains", resets, drains)
		}
		if !done && (resets != 1 || drains != 1) {
			t.Errorf("first delete pass must drain and reset once, got %d resets, %d drains", resets, drains)
		}
		if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "w"}, &infrav1.RemoteCluster{}); !apierrors.IsNotFound(err) {
			t.Errorf("done=%v: object should be gone, err=%v", done, err)
		}
	}
}

func TestHandleDelete_ControlPlaneResetKillsRunningKubeadmInit(t *testing.T) {
	srv := startFakeSSH(t, okHandler)
	cp := deleting(withSSH(novpnNode("cp", "control-plane"), srv))
	r := newTestReconciler(t, sshSecretObj(), cp)
	if _, err := r.handleDelete(context.Background(), getRC(t, r, "cp")); err != nil {
		t.Fatal(err)
	}
	script := srv.find("kubeadm reset --force")
	kill := strings.Index(script, "[k]ubeadm init")
	if kill < 0 || kill > strings.Index(script, "kubeadm reset --force") {
		t.Errorf("control-plane reset must first kill kubeadm init:\n%.300s", script)
	}
}

func TestCancelControlPlaneJob_WaitsForTheGoroutineToAcknowledge(t *testing.T) {
	r := &RemoteClusterReconciler{}
	ch := make(chan controlPlaneJobResult, 1)
	var stopped atomic.Bool
	job := &controlPlaneJob{uid: "u", ch: ch, cancel: func() {
		go func() { // the goroutine notices the closed connection a bit later
			time.Sleep(150 * time.Millisecond)
			stopped.Store(true)
			ch <- controlPlaneJobResult{err: context.Canceled}
		}()
	}}
	r.controlPlaneJobs.Store("default/cp", job)

	if !r.cancelControlPlaneJob("default/cp") {
		t.Fatal("expected the cancellation to be acknowledged")
	}
	if !stopped.Load() {
		t.Error("cancelControlPlaneJob returned before the goroutine stopped")
	}
}

func TestCancelControlPlaneJob_WaitIsBounded(t *testing.T) {
	old := controlPlaneCancelAckTimeout
	controlPlaneCancelAckTimeout = 100 * time.Millisecond
	defer func() { controlPlaneCancelAckTimeout = old }()

	r := &RemoteClusterReconciler{}
	r.controlPlaneJobs.Store("default/cp", &controlPlaneJob{uid: "u", ch: make(chan controlPlaneJobResult)}) // never answers
	start := time.Now()
	if r.cancelControlPlaneJob("default/cp") {
		t.Error("an unresponsive goroutine must not be reported as acknowledged")
	}
	if d := time.Since(start); d < 90*time.Millisecond || d > 5*time.Second {
		t.Errorf("wait took %v, want ~100ms", d)
	}
	if _, ok := r.controlPlaneJobs.Load("default/cp"); ok {
		t.Error("job must be forgotten even when it does not acknowledge")
	}
}
