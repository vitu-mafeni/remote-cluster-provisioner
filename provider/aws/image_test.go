package aws

import (
	"context"
	"errors"
	"strings"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

// fakeImages is an in-memory ec2ImageAPI.
type fakeImages struct {
	images    []types.Image
	snapshots []types.Snapshot
	archs     map[string][]types.ArchitectureType // instance type -> supported
	typesErr  error
	imageErr  error

	lastImageInput *ec2.DescribeImagesInput
}

func (f *fakeImages) DescribeImages(_ context.Context, in *ec2.DescribeImagesInput, _ ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error) {
	f.lastImageInput = in
	if f.imageErr != nil {
		return nil, f.imageErr
	}
	var out []types.Image
	for _, i := range f.images {
		if len(in.ImageIds) > 0 && !containsString(in.ImageIds, awssdk.ToString(i.ImageId)) {
			continue
		}
		if nf := filterValues(in.Filters, "name"); nf != nil && !wildcardMatch(nf[0], awssdk.ToString(i.Name)) {
			continue
		}
		if af := filterValues(in.Filters, "architecture"); af != nil && !containsString(af, string(i.Architecture)) {
			continue
		}
		out = append(out, i)
	}
	if len(in.ImageIds) > 0 && len(out) == 0 {
		return nil, &smithy.GenericAPIError{Code: "InvalidAMIID.NotFound", Message: "The image id '[" + in.ImageIds[0] + "]' does not exist"}
	}
	return &ec2.DescribeImagesOutput{Images: out}, nil
}

func (f *fakeImages) DescribeSnapshots(_ context.Context, in *ec2.DescribeSnapshotsInput, _ ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error) {
	var out []types.Snapshot
	for _, s := range f.snapshots {
		if containsString(in.SnapshotIds, awssdk.ToString(s.SnapshotId)) {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil, &smithy.GenericAPIError{Code: "InvalidSnapshot.NotFound", Message: "not found"}
	}
	return &ec2.DescribeSnapshotsOutput{Snapshots: out}, nil
}

func (f *fakeImages) DescribeInstanceTypes(_ context.Context, in *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	if f.typesErr != nil {
		return nil, f.typesErr
	}
	a, ok := f.archs[string(in.InstanceTypes[0])]
	if !ok {
		return &ec2.DescribeInstanceTypesOutput{}, nil
	}
	return &ec2.DescribeInstanceTypesOutput{InstanceTypes: []types.InstanceTypeInfo{{ProcessorInfo: &types.ProcessorInfo{SupportedArchitectures: a}}}}, nil
}

// wildcardMatch supports the single trailing/leading '*' the tests use.
func wildcardMatch(pat, s string) bool {
	switch {
	case strings.HasSuffix(pat, "*"):
		return strings.HasPrefix(s, strings.TrimSuffix(pat, "*"))
	case strings.HasPrefix(pat, "*"):
		return strings.HasSuffix(s, strings.TrimPrefix(pat, "*"))
	}
	return pat == s
}

func img(id string, mut func(*types.Image)) types.Image {
	i := ubuntuImage(id)
	if mut != nil {
		mut(&i)
	}
	return i
}

func snap(id string, gb int32, state types.SnapshotState) types.Snapshot {
	return types.Snapshot{SnapshotId: awssdk.String(id), VolumeSize: awssdk.Int32(gb), State: state}
}

func imgNP(mut func(*mlv1alpha1.AWSConfig)) *mlv1alpha1.NodeProvision {
	np := &mlv1alpha1.NodeProvision{}
	np.Spec.Region = "us-east-1"
	np.Spec.InstanceType = "t3.xlarge"
	np.Spec.AWSConfig = &mlv1alpha1.AWSConfig{AMI: "ami-aaaa", SubnetID: "subnet-1"}
	if mut != nil {
		mut(np.Spec.AWSConfig)
	}
	return np
}

func prep(t *testing.T, f *fakeImages, np *mlv1alpha1.NodeProvision) (launchImage, error) {
	t.Helper()
	return prepareLaunchImage(context.Background(), f, np)
}

func x86() map[string][]types.ArchitectureType {
	return map[string][]types.ArchitectureType{"t3.xlarge": {types.ArchitectureTypeX8664}, "m7g.large": {types.ArchitectureTypeArm64}}
}

func TestPrepareLaunchImage_UbuntuDefaults(t *testing.T) {
	f := &fakeImages{images: []types.Image{img("ami-aaaa", nil)}, archs: x86()}
	got, err := prep(t, f, imgNP(nil))
	if err != nil {
		t.Fatal(err)
	}
	if got != (launchImage{RootDeviceName: "/dev/sda1", VolumeGB: 50}) {
		t.Fatalf("got %+v", got)
	}
}

func TestPrepareLaunchImage_UsesTheImagesRootDeviceName(t *testing.T) {
	f := &fakeImages{archs: x86(), images: []types.Image{img("ami-aaaa", func(i *types.Image) {
		i.RootDeviceName = awssdk.String("/dev/xvda")
		i.BlockDeviceMappings = []types.BlockDeviceMapping{{DeviceName: awssdk.String("/dev/xvda"), Ebs: &types.EbsBlockDevice{VolumeSize: awssdk.Int32(100)}}}
	})}}
	got, err := prep(t, f, imgNP(nil))
	if err != nil {
		t.Fatal(err)
	}
	// The mapping must replace /dev/xvda (not add /dev/sda1) and the 100 GB image grows the default 50.
	if got.RootDeviceName != "/dev/xvda" || got.VolumeGB != 100 {
		t.Fatalf("got %+v", got)
	}
	in := buildRunInstancesInput(imgNP(nil), "", got)
	m := in.BlockDeviceMappings
	if len(m) != 1 || awssdk.ToString(m[0].DeviceName) != "/dev/xvda" || awssdk.ToInt32(m[0].Ebs.VolumeSize) != 100 {
		t.Fatalf("block device mappings = %+v", m)
	}
}

func TestPrepareLaunchImage_RejectsUnsuitableImages(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*types.Image)
		want string
	}{
		{"pending", func(i *types.Image) { i.State = types.ImageStatePending }, "not available (state"},
		{"windows platform", func(i *types.Image) { i.Platform = types.PlatformValuesWindows }, "Windows"},
		{"windows details", func(i *types.Image) { i.PlatformDetails = awssdk.String("Windows with SQL Server") }, "Windows"},
		{"instance store", func(i *types.Image) { i.RootDeviceType = types.DeviceTypeInstanceStore }, "not EBS-backed"},
		{"paravirtual", func(i *types.Image) { i.VirtualizationType = types.VirtualizationTypeParavirtual }, "HVM"},
		{"arm image on x86 type", func(i *types.Image) { i.Architecture = types.ArchitectureValuesArm64 }, "AMI ami-aaaa is arm64 but instance type t3.xlarge runs x86_64"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeImages{images: []types.Image{img("ami-aaaa", c.mut)}, archs: x86()}
			_, err := prep(t, f, imgNP(nil))
			wantErr(t, err, c.want)
		})
	}
}

func TestPrepareLaunchImage_ArchitectureMatchesInstanceType(t *testing.T) {
	f := &fakeImages{archs: x86(), images: []types.Image{img("ami-aaaa", func(i *types.Image) { i.Architecture = types.ArchitectureValuesArm64 })}}
	np := imgNP(nil)
	np.Spec.InstanceType = "m7g.large"
	if _, err := prep(t, f, np); err != nil {
		t.Fatalf("arm image on a Graviton type must pass: %v", err)
	}
}

func TestPrepareLaunchImage_ArchLookupFailureIsNotFatal(t *testing.T) {
	f := &fakeImages{typesErr: errors.New("UnauthorizedOperation"), images: []types.Image{img("ami-aaaa", func(i *types.Image) { i.Architecture = types.ArchitectureValuesArm64 })}}
	if _, err := prep(t, f, imgNP(nil)); err != nil {
		t.Fatalf("an unreadable instance type must not block: %v", err)
	}
}

func TestPrepareLaunchImage_NotFoundNamesTheRegion(t *testing.T) {
	_, err := prep(t, &fakeImages{}, imgNP(nil))
	wantErr(t, err, "ami-aaaa is not available in region us-east-1")
	wantErr(t, err, "copy or share")
}

func TestPrepareLaunchImage_APIErrorsAreSanitized(t *testing.T) {
	f := &fakeImages{imageErr: &smithy.GenericAPIError{Code: "UnauthorizedOperation", Message: leakyMsg}}
	_, err := prep(t, f, imgNP(nil))
	if err == nil || strings.Contains(err.Error(), "123456789012") || strings.Contains(err.Error(), "arn:aws") {
		t.Fatalf("err = %v", err)
	}
}

// ── root snapshots ──────────────────────────────────────────────────────────

func TestPrepareLaunchImage_RootSnapshot(t *testing.T) {
	f := &fakeImages{archs: x86(), images: []types.Image{img("ami-aaaa", nil)},
		snapshots: []types.Snapshot{snap("snap-ok", 80, types.SnapshotStateCompleted)}}
	np := imgNP(func(c *mlv1alpha1.AWSConfig) { c.RootSnapshotID = "snap-ok" })
	got, err := prep(t, f, np)
	if err != nil {
		t.Fatal(err)
	}
	// Unset size grows from the 50 GB default to the snapshot's 80 GB.
	if got != (launchImage{RootDeviceName: "/dev/sda1", VolumeGB: 80, SnapshotID: "snap-ok"}) {
		t.Fatalf("got %+v", got)
	}
	m := buildRunInstancesInput(np, "", got).BlockDeviceMappings[0]
	if awssdk.ToString(m.Ebs.SnapshotId) != "snap-ok" || awssdk.ToString(m.DeviceName) != "/dev/sda1" || awssdk.ToInt32(m.Ebs.VolumeSize) != 80 {
		t.Fatalf("mapping = %+v / %+v", m, *m.Ebs)
	}
}

func TestPrepareLaunchImage_RootSnapshotErrors(t *testing.T) {
	f := &fakeImages{archs: x86(), images: []types.Image{img("ami-aaaa", nil)}, snapshots: []types.Snapshot{
		snap("snap-pending", 20, types.SnapshotStatePending), snap("snap-big", 200, types.SnapshotStateCompleted)}}
	_, err := prep(t, f, imgNP(func(c *mlv1alpha1.AWSConfig) { c.RootSnapshotID = "snap-nope" }))
	wantErr(t, err, "snap-nope not found in region us-east-1")
	_, err = prep(t, f, imgNP(func(c *mlv1alpha1.AWSConfig) { c.RootSnapshotID = "snap-pending" }))
	wantErr(t, err, "not completed")
	_, err = prep(t, f, imgNP(func(c *mlv1alpha1.AWSConfig) { c.RootSnapshotID = "snap-big"; c.RootVolumeSizeGB = 100 }))
	wantErr(t, err, "rootVolumeSizeGB 100 is smaller than the root volume of snapshot snap-big (200 GB)")
}

func TestPrepareLaunchImage_ExplicitSizeBelowImageRootIsRejected(t *testing.T) {
	f := &fakeImages{archs: x86(), images: []types.Image{img("ami-aaaa", func(i *types.Image) {
		i.BlockDeviceMappings = []types.BlockDeviceMapping{{DeviceName: awssdk.String("/dev/sda1"), Ebs: &types.EbsBlockDevice{VolumeSize: awssdk.Int32(120)}}}
	})}}
	_, err := prep(t, f, imgNP(func(c *mlv1alpha1.AWSConfig) { c.RootVolumeSizeGB = 60 }))
	wantErr(t, err, "smaller than the root volume of AMI ami-aaaa (120 GB)")
	got, err := prep(t, f, imgNP(nil)) // unset: grows instead of failing
	if err != nil || got.VolumeGB != 120 {
		t.Fatalf("got %+v err %v", got, err)
	}
}

// ── amiName ─────────────────────────────────────────────────────────────────

func named(id, name, created string, arch types.ArchitectureValues) types.Image {
	return img(id, func(i *types.Image) {
		i.Name = awssdk.String(name)
		i.CreationDate = awssdk.String(created)
		i.Architecture = arch
	})
}

func TestResolveAMIByName(t *testing.T) {
	ctx := context.Background()
	f := &fakeImages{archs: x86(), images: []types.Image{
		named("ami-old", "k8s-node-1", "2026-01-01T00:00:00.000Z", types.ArchitectureValuesX8664),
		named("ami-new", "k8s-node-2", "2026-06-01T00:00:00.000Z", types.ArchitectureValuesX8664),
		named("ami-arm", "k8s-node-3", "2026-09-01T00:00:00.000Z", types.ArchitectureValuesArm64),
		named("ami-other", "other-1", "2026-09-02T00:00:00.000Z", types.ArchitectureValuesX8664),
		img("ami-win", func(i *types.Image) {
			i.Name = awssdk.String("k8s-node-win")
			i.CreationDate = awssdk.String("2026-10-01T00:00:00.000Z")
			i.Platform = types.PlatformValuesWindows
		}),
	}}
	got, err := resolveAMIByName(ctx, f, "us-east-1", "k8s-node-*", nil, "t3.xlarge")
	if err != nil || got != "ami-new" {
		t.Fatalf("newest matching x86_64, non-Windows image: %q %v", got, err)
	}
	if o := f.lastImageInput.Owners; len(o) != 1 || o[0] != "self" {
		t.Errorf("owners default to self: %v", o)
	}
	if got, err := resolveAMIByName(ctx, f, "us-east-1", "k8s-node-*", []string{"123456789012"}, "m7g.large"); err != nil || got != "ami-arm" {
		t.Fatalf("arm instance type picks the arm image: %q %v", got, err)
	}
	if o := f.lastImageInput.Owners; len(o) != 1 || o[0] != "123456789012" {
		t.Errorf("owners: %v", o)
	}
	_, err = resolveAMIByName(ctx, f, "us-east-1", "nothing-*", nil, "t3.xlarge")
	wantErr(t, err, `no available AMI named "nothing-*"`)
	_, err = resolveAMIByName(ctx, f, "us-east-1", "", nil, "t3.xlarge")
	wantErr(t, err, "amiName is empty")
}

func TestResolveAMIByName_TiesAreStable(t *testing.T) {
	f := &fakeImages{archs: x86(), images: []types.Image{
		named("ami-b", "n-1", "2026-06-01T00:00:00.000Z", types.ArchitectureValuesX8664),
		named("ami-a", "n-2", "2026-06-01T00:00:00.000Z", types.ArchitectureValuesX8664),
	}}
	for i := 0; i < 5; i++ {
		if got, _ := resolveAMIByName(context.Background(), f, "r", "n-*", nil, "t3.xlarge"); got != "ami-a" {
			t.Fatalf("got %q", got)
		}
		f.images[0], f.images[1] = f.images[1], f.images[0]
	}
}

// ── spec validation ─────────────────────────────────────────────────────────

func TestValidateImageSpec(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*mlv1alpha1.AWSConfig)
		want string
	}{
		{"ok", func(c *mlv1alpha1.AWSConfig) {
			c.AMIName, c.AMIOwners, c.RootSnapshotID = "k8s-*", []string{"self", "123456789012", "amazon"}, "snap-0abc"
		}, ""},
		{"bad ami", func(c *mlv1alpha1.AWSConfig) { c.AMI = "ubuntu" }, "not an AMI ID"},
		{"bad snapshot", func(c *mlv1alpha1.AWSConfig) { c.RootSnapshotID = "vol-123" }, "not a snapshot ID"},
		{"bad owner", func(c *mlv1alpha1.AWSConfig) { c.AMIOwners = []string{"everyone"} }, "amiOwners entry"},
		{"short account", func(c *mlv1alpha1.AWSConfig) { c.AMIOwners = []string{"12345"} }, "amiOwners entry"},
		{"padded name", func(c *mlv1alpha1.AWSConfig) { c.AMIName = " k8s" }, "whitespace"},
		{"negative size", func(c *mlv1alpha1.AWSConfig) { c.RootVolumeSizeGB = -1 }, "negative"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			np := imgNP(c.mut)
			np.Spec.Region = "us-east-1"
			np.Spec.InstanceType = "t3.xlarge"
			err := ValidateAWSConfig(np.Spec)
			if c.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			wantErr(t, err, c.want)
		})
	}
}
