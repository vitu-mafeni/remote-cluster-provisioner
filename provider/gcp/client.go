package gcp

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
)

// computeAPI is the narrow slice of the Compute Engine API this package needs.
// It is an interface so every workflow can be unit-tested with a fake; the real
// implementation (sdkCompute) wraps the official REST clients and waits for the
// long-running operations behind mutating calls, so a nil error means the
// change has taken effect.
type computeAPI interface {
	GetInstance(ctx context.Context, project, zone, name string) (*computepb.Instance, error)
	// InsertInstance creates the instance and waits for the insert operation to
	// finish (quota/stockout/permission errors surface here). requestID makes a
	// retried call idempotent on the server for ~60 minutes.
	InsertInstance(ctx context.Context, project, zone, requestID string, inst *computepb.Instance) error
	DeleteInstance(ctx context.Context, project, zone, name string) error

	GetFirewall(ctx context.Context, project, name string) (*computepb.Firewall, error)
	InsertFirewall(ctx context.Context, project string, fw *computepb.Firewall) error
	DeleteFirewall(ctx context.Context, project, name string) error

	GetNetwork(ctx context.Context, project, name string) (*computepb.Network, error)
	GetSubnetwork(ctx context.Context, project, region, name string) (*computepb.Subnetwork, error)
	ListZones(ctx context.Context, project, region string) ([]*computepb.Zone, error)
	GetMachineType(ctx context.Context, project, zone, name string) (*computepb.MachineType, error)
	GetAcceleratorType(ctx context.Context, project, zone, name string) (*computepb.AcceleratorType, error)
	GetImageFromFamily(ctx context.Context, project, family string) (*computepb.Image, error)

	Close() error
}

// newClient builds the compute client for creds; tests replace it with a fake.
var newClient = func(ctx context.Context, creds Credentials) (computeAPI, error) {
	return newSDKCompute(ctx, creds)
}

// operationTimeout bounds how long a single mutating call waits for its
// operation. A timeout is an ordinary (retryable) error: the operation keeps
// running server-side and the next reconcile observes the result.
var operationTimeout = 5 * time.Minute

type sdkCompute struct {
	instances *compute.InstancesClient
	firewalls *compute.FirewallsClient
	networks  *compute.NetworksClient
	subnets   *compute.SubnetworksClient
	zones     *compute.ZonesClient
	machines  *compute.MachineTypesClient
	accels    *compute.AcceleratorTypesClient
	images    *compute.ImagesClient
}

func newSDKCompute(ctx context.Context, creds Credentials) (*sdkCompute, error) {
	if len(creds.JSON) == 0 {
		return nil, errors.New("no GCP credentials")
	}
	return newSDKComputeWithOptions(ctx, option.WithCredentialsJSON(creds.JSON)) // validated by ParseServiceAccountKey
}

// newSDKComputeWithOptions builds the REST clients with the given options (tests
// point them at an httptest server with option.WithEndpoint).
func newSDKComputeWithOptions(ctx context.Context, opts ...option.ClientOption) (*sdkCompute, error) {
	c := &sdkCompute{}
	var err error
	if c.instances, err = compute.NewInstancesRESTClient(ctx, opts...); err != nil {
		return nil, fmt.Errorf("creating GCE instances client: %w", err)
	}
	if c.firewalls, err = compute.NewFirewallsRESTClient(ctx, opts...); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("creating GCE firewalls client: %w", err)
	}
	if c.networks, err = compute.NewNetworksRESTClient(ctx, opts...); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("creating GCE networks client: %w", err)
	}
	if c.subnets, err = compute.NewSubnetworksRESTClient(ctx, opts...); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("creating GCE subnetworks client: %w", err)
	}
	if c.zones, err = compute.NewZonesRESTClient(ctx, opts...); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("creating GCE zones client: %w", err)
	}
	if c.machines, err = compute.NewMachineTypesRESTClient(ctx, opts...); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("creating GCE machine types client: %w", err)
	}
	if c.accels, err = compute.NewAcceleratorTypesRESTClient(ctx, opts...); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("creating GCE accelerator types client: %w", err)
	}
	if c.images, err = compute.NewImagesRESTClient(ctx, opts...); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("creating GCE images client: %w", err)
	}
	return c, nil
}

func (c *sdkCompute) Close() error {
	var errs []error
	if c.instances != nil {
		errs = append(errs, c.instances.Close())
	}
	if c.firewalls != nil {
		errs = append(errs, c.firewalls.Close())
	}
	if c.networks != nil {
		errs = append(errs, c.networks.Close())
	}
	if c.subnets != nil {
		errs = append(errs, c.subnets.Close())
	}
	if c.zones != nil {
		errs = append(errs, c.zones.Close())
	}
	if c.machines != nil {
		errs = append(errs, c.machines.Close())
	}
	if c.accels != nil {
		errs = append(errs, c.accels.Close())
	}
	if c.images != nil {
		errs = append(errs, c.images.Close())
	}
	return errors.Join(errs...)
}

// waitOp waits for op and converts an error recorded on the finished operation
// into a *googleapi.Error so Classify can read it.
func waitOp(ctx context.Context, op *compute.Operation) error {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if err := op.Wait(ctx); err != nil {
		return err
	}
	if e := op.Proto().GetError(); e != nil && len(e.GetErrors()) > 0 {
		first := e.GetErrors()[0]
		code := int(op.Proto().GetHttpErrorStatusCode())
		if code == 0 {
			code = 500
		}
		return &googleapi.Error{
			Code:    code,
			Message: fmt.Sprintf("%s: %s", first.GetCode(), first.GetMessage()),
			Errors:  []googleapi.ErrorItem{{Reason: first.GetCode(), Message: first.GetMessage()}},
		}
	}
	return nil
}

func (c *sdkCompute) GetInstance(ctx context.Context, project, zone, name string) (*computepb.Instance, error) {
	return c.instances.Get(ctx, &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: name})
}

func (c *sdkCompute) InsertInstance(ctx context.Context, project, zone, requestID string, inst *computepb.Instance) error {
	req := &computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: inst}
	if requestID != "" {
		req.RequestId = proto.String(requestID)
	}
	op, err := c.instances.Insert(ctx, req)
	if err != nil {
		return err
	}
	return waitOp(ctx, op)
}

func (c *sdkCompute) DeleteInstance(ctx context.Context, project, zone, name string) error {
	op, err := c.instances.Delete(ctx, &computepb.DeleteInstanceRequest{Project: project, Zone: zone, Instance: name})
	if err != nil {
		return err
	}
	return waitOp(ctx, op)
}

func (c *sdkCompute) GetFirewall(ctx context.Context, project, name string) (*computepb.Firewall, error) {
	return c.firewalls.Get(ctx, &computepb.GetFirewallRequest{Project: project, Firewall: name})
}

func (c *sdkCompute) InsertFirewall(ctx context.Context, project string, fw *computepb.Firewall) error {
	op, err := c.firewalls.Insert(ctx, &computepb.InsertFirewallRequest{Project: project, FirewallResource: fw})
	if err != nil {
		return err
	}
	return waitOp(ctx, op)
}

func (c *sdkCompute) DeleteFirewall(ctx context.Context, project, name string) error {
	op, err := c.firewalls.Delete(ctx, &computepb.DeleteFirewallRequest{Project: project, Firewall: name})
	if err != nil {
		return err
	}
	return waitOp(ctx, op)
}

func (c *sdkCompute) GetNetwork(ctx context.Context, project, name string) (*computepb.Network, error) {
	return c.networks.Get(ctx, &computepb.GetNetworkRequest{Project: project, Network: name})
}

func (c *sdkCompute) GetSubnetwork(ctx context.Context, project, region, name string) (*computepb.Subnetwork, error) {
	return c.subnets.Get(ctx, &computepb.GetSubnetworkRequest{Project: project, Region: region, Subnetwork: name})
}

func (c *sdkCompute) ListZones(ctx context.Context, project, region string) ([]*computepb.Zone, error) {
	it := c.zones.List(ctx, &computepb.ListZonesRequest{
		Project: project,
		Filter:  proto.String(fmt.Sprintf(`name eq "%s-.*"`, region)),
	})
	var out []*computepb.Zone
	for {
		z, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, z)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetName() < out[j].GetName() })
	return out, nil
}

func (c *sdkCompute) GetMachineType(ctx context.Context, project, zone, name string) (*computepb.MachineType, error) {
	return c.machines.Get(ctx, &computepb.GetMachineTypeRequest{Project: project, Zone: zone, MachineType: name})
}

func (c *sdkCompute) GetAcceleratorType(ctx context.Context, project, zone, name string) (*computepb.AcceleratorType, error) {
	return c.accels.Get(ctx, &computepb.GetAcceleratorTypeRequest{Project: project, Zone: zone, AcceleratorType: name})
}

func (c *sdkCompute) GetImageFromFamily(ctx context.Context, project, family string) (*computepb.Image, error) {
	return c.images.GetFromFamily(ctx, &computepb.GetFromFamilyImageRequest{Project: project, Family: family})
}
