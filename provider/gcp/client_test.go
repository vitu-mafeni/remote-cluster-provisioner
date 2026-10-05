package gcp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// These tests drive the REAL Google client libraries against an httptest server
// that speaks the Compute REST protocol, so what is verified is the wiring of
// this package to the SDK: request paths, the idempotency requestId, operation
// polling, error classification of real SDK errors. They prove nothing about
// what a real project accepts (see the notes in the docs).

type fakeAPI struct {
	mu       sync.Mutex
	requests []string // "METHOD path?query"
	bodies   map[string][]byte
	handler  func(w http.ResponseWriter, r *http.Request)
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(body)) // handlers read it again
	f.mu.Lock()
	key := r.Method + " " + r.URL.Path
	f.requests = append(f.requests, key+"?"+r.URL.RawQuery)
	if f.bodies == nil {
		f.bodies = map[string][]byte{}
	}
	f.bodies[key] = body
	f.mu.Unlock()
	f.handler(w, r)
}

func (f *fakeAPI) reqs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func newSDKAgainst(t *testing.T, h func(w http.ResponseWriter, r *http.Request)) (*sdkCompute, *fakeAPI) {
	t.Helper()
	api := &fakeAPI{handler: h}
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	c, err := newSDKComputeWithOptions(context.Background(), option.WithEndpoint(srv.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, api
}

func opJSON(name, status string) map[string]any {
	return map[string]any{"kind": "compute#operation", "name": name, "status": status, "zone": "zones/us-central1-a"}
}

func TestSDK_InsertInstanceSendsRequestIDAndWaitsForTheOperation(t *testing.T) {
	polls := 0
	c, api := newSDKAgainst(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/projects/p1/zones/us-central1-a/instances"):
			writeJSON(w, 200, opJSON("op-1", "RUNNING"))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/zones/us-central1-a/operations/op-1"):
			polls++
			st := "RUNNING"
			if polls >= 2 {
				st = "DONE"
			}
			writeJSON(w, 200, opJSON("op-1", st))
		default:
			http.NotFound(w, r)
		}
	})
	old := operationTimeout
	operationTimeout = 30 * time.Second
	t.Cleanup(func() { operationTimeout = old })

	inst := &computepb.Instance{Name: proto.String("worker-1"), MachineType: proto.String("zones/us-central1-a/machineTypes/e2-standard-4"),
		Labels: map[string]string{LabelUID: testUID}}
	if err := c.InsertInstance(context.Background(), "p1", "us-central1-a", "11111111-2222-5333-8444-555555555555", inst); err != nil {
		t.Fatal(err)
	}
	if polls < 2 {
		t.Errorf("the insert must wait for the operation to be DONE, polled %d times", polls)
	}
	var insert string
	for _, r := range api.reqs() {
		if strings.HasPrefix(r, "POST ") {
			insert = r
		}
	}
	if !strings.Contains(insert, "requestId=11111111-2222-5333-8444-555555555555") {
		t.Errorf("the insert must carry the idempotency requestId: %s", insert)
	}
	var sent computepb.Instance
	body := api.bodies["POST /compute/v1/projects/p1/zones/us-central1-a/instances"]
	if err := protojson.Unmarshal(body, &sent); err != nil {
		t.Fatalf("body %s: %v", body, err)
	}
	if sent.GetName() != "worker-1" || sent.GetLabels()[LabelUID] != testUID {
		t.Errorf("body: %s", body)
	}
}

func TestSDK_FailedOperationsSurfaceAsClassifiableErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		httpCode int
		code     string
		msg      string
		want     func(error) bool
	}{
		"quota": {403, "QUOTA_EXCEEDED", "Quota 'CPUS' exceeded. Limit: 24.0 in region us-central1.", IsQuotaExceeded},
		"stockout": {503, "ZONE_RESOURCE_POOL_EXHAUSTED",
			"The zone 'projects/p1/zones/us-central1-a' does not have enough resources available to fulfill the request.", IsStockout},
		"permission": {403, "IAM_PERMISSION_DENIED", "Required 'compute.instances.create' permission", IsPermissionDenied},
		"conflict":   {409, "RESOURCE_ALREADY_EXISTS", "The resource already exists", IsAlreadyExists},
		// An operation that finished with an error but no HTTP status (our own
		// conversion in waitOp, not the SDK's).
		"quota without http status":    {0, "QUOTA_EXCEEDED", "Quota 'CPUS' exceeded. Limit: 24.0", IsQuotaExceeded},
		"stockout without http status": {0, "ZONE_RESOURCE_POOL_EXHAUSTED", "does not have enough resources", IsStockout},
	} {
		c, _ := newSDKAgainst(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				writeJSON(w, 200, opJSON("op-x", "RUNNING"))
				return
			}
			op := opJSON("op-x", "DONE")
			if tc.httpCode != 0 {
				op["httpErrorStatusCode"] = tc.httpCode
				op["httpErrorMessage"] = "ERR"
			}
			op["error"] = map[string]any{"errors": []map[string]any{{"code": tc.code, "message": tc.msg}}}
			writeJSON(w, 200, op)
		})
		err := c.InsertInstance(context.Background(), "p1", "us-central1-a", "", &computepb.Instance{Name: proto.String("w")})
		if err == nil {
			t.Errorf("%s: a failed operation must be an error", name)
			continue
		}
		if !tc.want(err) {
			t.Errorf("%s: the real SDK error must classify as %s, got %v (%v)", name, name, Classify(err), err)
		}
	}
}

func TestSDK_HTTPErrorsClassifyThroughTheRealClient(t *testing.T) {
	c, _ := newSDKAgainst(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/instances/missing"):
			writeJSON(w, 404, map[string]any{"error": map[string]any{"code": 404, "message": "The resource was not found",
				"errors": []map[string]any{{"reason": "notFound", "message": "not found"}}}})
		case strings.HasSuffix(r.URL.Path, "/instances/denied"):
			writeJSON(w, 403, map[string]any{"error": map[string]any{"code": 403, "message": "Required 'compute.instances.get' permission",
				"errors": []map[string]any{{"reason": "forbidden", "message": "denied"}}}})
		case strings.HasSuffix(r.URL.Path, "/instances/limited"):
			writeJSON(w, 429, map[string]any{"error": map[string]any{"code": 429, "message": "Too Many Requests"}})
		case strings.HasSuffix(r.URL.Path, "/instances/ok"):
			writeJSON(w, 200, map[string]any{"name": "ok", "status": "RUNNING",
				"networkInterfaces": []map[string]any{{"networkIP": "10.1.2.3", "accessConfigs": []map[string]any{{"natIP": "34.9.9.9"}}}}})
		default:
			http.NotFound(w, r)
		}
	})
	ctx := context.Background()
	if _, err := c.GetInstance(ctx, "p1", "z", "missing"); !IsNotFound(err) {
		t.Errorf("404 -> %v: %v", Classify(err), err)
	}
	if _, err := c.GetInstance(ctx, "p1", "z", "denied"); !IsPermissionDenied(err) {
		t.Errorf("403 -> %v: %v", Classify(err), err)
	}
	if _, err := c.GetInstance(ctx, "p1", "z", "limited"); !IsTransient(err) {
		t.Errorf("429 -> %v: %v", Classify(err), err)
	}
	inst, err := c.GetInstance(ctx, "p1", "z", "ok")
	if err != nil {
		t.Fatal(err)
	}
	if in, ex := instanceIPs(inst); in != "10.1.2.3" || ex != "34.9.9.9" || inst.GetStatus() != "RUNNING" {
		t.Errorf("decoded instance: %q %q %q", in, ex, inst.GetStatus())
	}
}

func TestSDK_ReadOnlyLookupsAndFirewallsUseTheExpectedPaths(t *testing.T) {
	c, api := newSDKAgainst(t, func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case r.Method == http.MethodGet && strings.Contains(p, "/operations/"):
			writeJSON(w, 200, map[string]any{"name": p[strings.LastIndex(p, "/")+1:], "status": "DONE"})
		case strings.HasSuffix(p, "/zones") && r.Method == http.MethodGet:
			writeJSON(w, 200, map[string]any{"items": []map[string]any{
				{"name": "us-central1-c", "status": "UP"}, {"name": "us-central1-a", "status": "UP"}}})
		case strings.Contains(p, "/machineTypes/"):
			writeJSON(w, 200, map[string]any{"name": "e2-standard-4"})
		case strings.Contains(p, "/acceleratorTypes/"):
			writeJSON(w, 200, map[string]any{"name": "nvidia-tesla-t4"})
		case strings.Contains(p, "/global/networks/default"):
			writeJSON(w, 200, map[string]any{"name": "default", "autoCreateSubnetworks": true})
		case strings.Contains(p, "/global/images/family/ubuntu-2204-lts"):
			writeJSON(w, 200, map[string]any{"name": "ubuntu-2204-jammy-v1", "architecture": "X86_64",
				"selfLink": "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v1"})
		case strings.HasSuffix(p, "/global/firewalls") && r.Method == http.MethodPost:
			writeJSON(w, 200, map[string]any{"name": "op-fw", "status": "DONE"})
		case strings.Contains(p, "/global/firewalls/np-w-wg") && r.Method == http.MethodDelete:
			writeJSON(w, 200, map[string]any{"name": "op-fwd", "status": "DONE"})
		case strings.Contains(p, "/global/firewalls/np-w-wg") && r.Method == http.MethodGet:
			writeJSON(w, 200, map[string]any{"name": "np-w-wg", "description": firewallOwnerMarker})
		case strings.Contains(p, "/zones/z/instances/w") && r.Method == http.MethodDelete:
			writeJSON(w, 200, map[string]any{"name": "op-d", "status": "DONE", "zone": "zones/z"})
		default:
			http.NotFound(w, r)
		}
	})
	ctx := context.Background()
	zones, err := c.ListZones(ctx, "p1", "us-central1")
	if err != nil || len(zones) != 2 || zones[0].GetName() != "us-central1-a" {
		t.Fatalf("zones sorted by name: %v %v", zones, err)
	}
	if _, err := c.GetMachineType(ctx, "p1", "us-central1-a", "e2-standard-4"); err != nil {
		t.Error(err)
	}
	if _, err := c.GetAcceleratorType(ctx, "p1", "us-central1-a", "nvidia-tesla-t4"); err != nil {
		t.Error(err)
	}
	if n, err := c.GetNetwork(ctx, "p1", "default"); err != nil || !n.GetAutoCreateSubnetworks() {
		t.Errorf("network: %v %v", n, err)
	}
	img, err := c.GetImageFromFamily(ctx, DefaultImageProject, DefaultImageFamily)
	if err != nil || compactResourcePath(img.GetSelfLink()) != "projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v1" {
		t.Errorf("image: %v %v", img, err)
	}
	fw := &computepb.Firewall{Name: proto.String("np-w-wg"), Network: proto.String("projects/p1/global/networks/default"),
		Allowed: []*computepb.Allowed{{IPProtocol: proto.String("udp"), Ports: []string{"51820"}}}}
	if err := c.InsertFirewall(ctx, "p1", fw); err != nil {
		t.Error(err)
	}
	if got, err := c.GetFirewall(ctx, "p1", "np-w-wg"); err != nil || got.GetDescription() != firewallOwnerMarker {
		t.Errorf("firewall: %v %v", got, err)
	}
	if err := c.DeleteFirewall(ctx, "p1", "np-w-wg"); err != nil {
		t.Error(err)
	}
	if err := c.DeleteInstance(ctx, "p1", "z", "w"); err != nil {
		t.Error(err)
	}

	joined := strings.Join(api.reqs(), "\n")
	for _, want := range []string{
		"/projects/p1/zones?", // zones list (with a region filter)
		"/projects/p1/zones/us-central1-a/machineTypes/e2-standard-4",
		"/projects/p1/zones/us-central1-a/acceleratorTypes/nvidia-tesla-t4",
		"/projects/p1/global/networks/default",
		"/projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts",
		"POST /compute/v1/projects/p1/global/firewalls",
		"DELETE /compute/v1/projects/p1/global/firewalls/np-w-wg",
		"DELETE /compute/v1/projects/p1/zones/z/instances/w",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("expected a request containing %q in:\n%s", want, joined)
		}
	}
	if !strings.Contains(joined, "filter=") || !strings.Contains(joined, "us-central1-") {
		t.Errorf("the zone listing must be filtered by region:\n%s", joined)
	}
}

// The whole provisioning flow, through the real SDK clients (no fake computeAPI).
func TestSDK_EndToEndProvisionInstanceVPNless(t *testing.T) {
	created := map[string]*computepb.Instance{}
	var mu sync.Mutex
	api := &fakeAPI{}
	api.handler = func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodGet && strings.Contains(p, "/operations/"):
			writeJSON(w, 200, map[string]any{"name": p[strings.LastIndex(p, "/")+1:], "status": "DONE"})
		case r.Method == http.MethodGet && strings.Contains(p, "/instances/worker-1"):
			if inst, ok := created["worker-1"]; ok {
				b, _ := protojson.Marshal(inst)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(b)
				return
			}
			writeJSON(w, 404, map[string]any{"error": map[string]any{"code": 404, "message": "not found"}})
		case r.Method == http.MethodGet && strings.Contains(p, "/global/firewalls/"):
			writeJSON(w, 404, map[string]any{"error": map[string]any{"code": 404, "message": "not found"}})
		case r.Method == http.MethodPost && strings.HasSuffix(p, "/global/firewalls"):
			writeJSON(w, 200, map[string]any{"name": "op-fw", "status": "DONE"})
		case r.Method == http.MethodPost && strings.HasSuffix(p, "/instances"):
			body, _ := io.ReadAll(r.Body)
			var inst computepb.Instance
			if err := protojson.Unmarshal(body, &inst); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			created[inst.GetName()] = &inst
			writeJSON(w, 200, map[string]any{"name": "op-i", "status": "DONE", "zone": "zones/us-central1-a"})
		default:
			http.NotFound(w, r)
		}
	}
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	old := newClient
	newClient = func(ctx context.Context, _ Credentials) (computeAPI, error) {
		return newSDKComputeWithOptions(ctx, option.WithEndpoint(srv.URL), option.WithoutAuthentication())
	}
	t.Cleanup(func() { newClient = old })

	np := testNP()
	np.Spec.DisableVPN = true
	_, pub, err := testKey()
	if err != nil {
		t.Fatal(err)
	}
	nc := newProvEnvNetConfig()
	res, err := ProvisionInstance(context.Background(), np, testCreds(), pub, nil, nc, pkgruntimeConfig())
	if err != nil {
		t.Fatal(err)
	}
	if res.InstanceID != "worker-1" || res.Adopted {
		t.Fatalf("res = %+v", res)
	}
	mu.Lock()
	inst := created["worker-1"]
	mu.Unlock()
	if inst == nil {
		t.Fatal("no instance reached the API")
	}
	if v, ok := metaValue(inst, "startup-script"); !ok || !strings.Contains(v, "kubeadm join") {
		t.Error("startup-script did not survive the wire")
	}
	if inst.GetDisks()[0].GetInitializeParams().GetSourceImage() == "" || inst.GetLabels()[LabelUID] != testUID {
		t.Errorf("instance on the wire: %v", inst)
	}
	// A second run adopts it (the flow is idempotent through the real client too).
	res, err = ProvisionInstance(context.Background(), np, testCreds(), pub, nil, nc, pkgruntimeConfig())
	if err != nil || !res.Adopted {
		t.Fatalf("a retry must adopt: %+v %v", res, err)
	}
}
