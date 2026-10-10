package aws

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

// ec2ImageAPI is the slice of the EC2 client image/snapshot handling needs (an
// interface so it can be unit-tested with a fake).
type ec2ImageAPI interface {
	DescribeImages(ctx context.Context, params *ec2.DescribeImagesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error)
	DescribeSnapshots(ctx context.Context, params *ec2.DescribeSnapshotsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error)
	DescribeInstanceTypes(ctx context.Context, params *ec2.DescribeInstanceTypesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error)
}

var (
	amiIDRE      = regexp.MustCompile(`^ami-[0-9a-f]+$`)
	snapshotIDRE = regexp.MustCompile(`^snap-[0-9a-f]+$`)
	amiOwnerRE   = regexp.MustCompile(`^(self|amazon|aws-marketplace|[0-9]{12})$`)
)

// defaultRootDeviceName is the root device of the Ubuntu images this
// controller bootstraps; used when no image description is available.
const defaultRootDeviceName = "/dev/sda1"

// validateImageSpec checks the image-related awsConfig fields without any API call.
func validateImageSpec(c *mlv1alpha1.AWSConfig) error {
	if c.AMI != "" && !amiIDRE.MatchString(c.AMI) {
		return fmt.Errorf("spec.awsConfig.ami %q is not an AMI ID (ami-...)", c.AMI)
	}
	if c.RootSnapshotID != "" && !snapshotIDRE.MatchString(c.RootSnapshotID) {
		return fmt.Errorf("spec.awsConfig.rootSnapshotId %q is not a snapshot ID (snap-...)", c.RootSnapshotID)
	}
	if c.AMIName != "" && strings.TrimSpace(c.AMIName) != c.AMIName {
		return fmt.Errorf("spec.awsConfig.amiName must not start or end with whitespace")
	}
	for _, o := range c.AMIOwners {
		if !amiOwnerRE.MatchString(o) {
			return fmt.Errorf("spec.awsConfig.amiOwners entry %q must be self, amazon, aws-marketplace or a 12-digit account ID", o)
		}
	}
	if c.RootVolumeSizeGB < 0 {
		return fmt.Errorf("spec.awsConfig.rootVolumeSizeGB must not be negative")
	}
	return nil
}

// launchImage is what the launch needs to know about the chosen image.
type launchImage struct {
	// RootDeviceName is the image's root device ("/dev/sda1", "/dev/xvda", ...).
	// The root volume mapping must use it: a different name would attach a
	// second volume instead of resizing the root.
	RootDeviceName string
	// VolumeGB is the final root volume size.
	VolumeGB int32
	// SnapshotID overrides the image's root snapshot (empty: the image's own).
	SnapshotID string
}

// requestedRootGB is spec.awsConfig.rootVolumeSizeGB or the default.
func requestedRootGB(c *mlv1alpha1.AWSConfig) int32 {
	if c != nil && c.RootVolumeSizeGB > 0 {
		return c.RootVolumeSizeGB
	}
	return defaultRootVolumeSizeGB
}

// instanceArchitectures returns the CPU architectures of an instance type
// (nil when unknown: the lookup is best effort and must not block provisioning).
func instanceArchitectures(ctx context.Context, api ec2ImageAPI, instanceType string) []string {
	if instanceType == "" {
		return nil
	}
	out, err := api.DescribeInstanceTypes(ctx, &ec2.DescribeInstanceTypesInput{InstanceTypes: []types.InstanceType{types.InstanceType(instanceType)}})
	if err != nil || len(out.InstanceTypes) == 0 || out.InstanceTypes[0].ProcessorInfo == nil {
		if err != nil {
			log.Printf("[WARN] Image check: cannot read the architecture of %s (continuing without): %s", instanceType, describeAPIError(err))
		}
		return nil
	}
	var archs []string
	for _, a := range out.InstanceTypes[0].ProcessorInfo.SupportedArchitectures {
		archs = append(archs, string(a))
	}
	return archs
}

// prepareLaunchImage validates the image (and root snapshot) of a launch and
// returns how to map the root volume. It has no side effects.
func prepareLaunchImage(ctx context.Context, api ec2ImageAPI, np *mlv1alpha1.NodeProvision) (launchImage, error) {
	cfg := np.Spec.AWSConfig
	region := np.Spec.Region

	out, err := api.DescribeImages(ctx, &ec2.DescribeImagesInput{ImageIds: []string{cfg.AMI}})
	if err != nil {
		if hasAPIErrorCode(err, "InvalidAMIID.NotFound", "InvalidAMIID.Unavailable", "InvalidAMIID.Malformed") {
			return launchImage{}, apiErrorf(err, "spec.awsConfig.ami %s is not available in region %s (AMIs are regional: copy or share it to this region)", cfg.AMI, region)
		}
		return launchImage{}, apiErrorf(err, "describing AMI %s", cfg.AMI)
	}
	if len(out.Images) == 0 {
		return launchImage{}, fmt.Errorf("spec.awsConfig.ami %s not found in region %s", cfg.AMI, region)
	}
	img := out.Images[0]

	if img.State != types.ImageStateAvailable {
		return launchImage{}, fmt.Errorf("AMI %s is not available (state %q)", cfg.AMI, img.State)
	}
	if img.Platform == types.PlatformValuesWindows || strings.HasPrefix(strings.ToLower(awssdk.ToString(img.PlatformDetails)), "windows") {
		return launchImage{}, fmt.Errorf("AMI %s is a Windows image; nodes need an Ubuntu Linux image", cfg.AMI)
	}
	if img.RootDeviceType != types.DeviceTypeEbs {
		return launchImage{}, fmt.Errorf("AMI %s is not EBS-backed (root device type %q); only EBS-backed images are supported", cfg.AMI, img.RootDeviceType)
	}
	if img.VirtualizationType != types.VirtualizationTypeHvm {
		return launchImage{}, fmt.Errorf("AMI %s uses %q virtualization; only HVM images are supported", cfg.AMI, img.VirtualizationType)
	}
	if archs := instanceArchitectures(ctx, api, np.Spec.InstanceType); len(archs) > 0 {
		ok := false
		for _, a := range archs {
			ok = ok || a == string(img.Architecture)
		}
		if !ok {
			return launchImage{}, fmt.Errorf("AMI %s is %s but instance type %s runs %s: pick an AMI with a matching architecture",
				cfg.AMI, img.Architecture, np.Spec.InstanceType, strings.Join(archs, "/"))
		}
	}

	res := launchImage{RootDeviceName: awssdk.ToString(img.RootDeviceName)}
	if res.RootDeviceName == "" {
		res.RootDeviceName = defaultRootDeviceName
	}
	// The image's own root volume size is the floor for the new root volume.
	var floor int32
	for _, m := range img.BlockDeviceMappings {
		if awssdk.ToString(m.DeviceName) == res.RootDeviceName && m.Ebs != nil {
			floor = awssdk.ToInt32(m.Ebs.VolumeSize)
		}
	}

	if cfg.RootSnapshotID != "" {
		so, err := api.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{SnapshotIds: []string{cfg.RootSnapshotID}})
		if err != nil {
			if hasAPIErrorCode(err, "InvalidSnapshot.NotFound", "InvalidSnapshotID.Malformed") {
				return launchImage{}, apiErrorf(err, "spec.awsConfig.rootSnapshotId %s not found in region %s (snapshots are regional: copy or share it to this region)", cfg.RootSnapshotID, region)
			}
			return launchImage{}, apiErrorf(err, "describing snapshot %s", cfg.RootSnapshotID)
		}
		if len(so.Snapshots) == 0 {
			return launchImage{}, fmt.Errorf("spec.awsConfig.rootSnapshotId %s not found in region %s", cfg.RootSnapshotID, region)
		}
		snap := so.Snapshots[0]
		if snap.State != types.SnapshotStateCompleted {
			return launchImage{}, fmt.Errorf("snapshot %s is not completed (state %q)", cfg.RootSnapshotID, snap.State)
		}
		res.SnapshotID = cfg.RootSnapshotID
		floor = awssdk.ToInt32(snap.VolumeSize) // the snapshot replaces the image's volume
	}

	want := requestedRootGB(cfg)
	switch {
	case cfg.RootVolumeSizeGB > 0 && cfg.RootVolumeSizeGB < floor:
		src := "AMI " + cfg.AMI
		if res.SnapshotID != "" {
			src = "snapshot " + res.SnapshotID
		}
		return launchImage{}, fmt.Errorf("spec.awsConfig.rootVolumeSizeGB %d is smaller than the root volume of %s (%d GB)", cfg.RootVolumeSizeGB, src, floor)
	case want < floor:
		want = floor // unset size: grow to fit rather than fail
	}
	res.VolumeGB = want
	return res, nil
}

// ResolveAMIByName returns the newest available EBS-backed HVM image whose name
// matches nameFilter among owners (default "self"), with an architecture the
// instance type runs.
func ResolveAMIByName(ctx context.Context, region string, creds AWSCredentials, nameFilter string, owners []string, instanceType string) (string, error) {
	client, err := newEC2Client(ctx, region, creds)
	if err != nil {
		return "", fmt.Errorf("creating EC2 client: %w", err)
	}
	return resolveAMIByName(ctx, client, region, nameFilter, owners, instanceType)
}

func resolveAMIByName(ctx context.Context, api ec2ImageAPI, region, nameFilter string, owners []string, instanceType string) (string, error) {
	if nameFilter == "" {
		return "", fmt.Errorf("spec.awsConfig.amiName is empty")
	}
	if len(owners) == 0 {
		owners = []string{"self"}
	}
	filters := []types.Filter{
		{Name: awssdk.String("name"), Values: []string{nameFilter}},
		{Name: awssdk.String("state"), Values: []string{"available"}},
		{Name: awssdk.String("virtualization-type"), Values: []string{"hvm"}},
		{Name: awssdk.String("root-device-type"), Values: []string{"ebs"}},
	}
	if archs := instanceArchitectures(ctx, api, instanceType); len(archs) > 0 {
		filters = append(filters, types.Filter{Name: awssdk.String("architecture"), Values: archs})
	}
	out, err := api.DescribeImages(ctx, &ec2.DescribeImagesInput{Owners: owners, Filters: filters})
	if err != nil {
		return "", apiErrorf(err, "searching AMIs named %q in %s", nameFilter, region)
	}
	var imgs []types.Image
	for _, i := range out.Images {
		if i.Platform != types.PlatformValuesWindows {
			imgs = append(imgs, i)
		}
	}
	if len(imgs) == 0 {
		return "", fmt.Errorf("no available AMI named %q (owners %s) with a matching architecture in region %s", nameFilter, strings.Join(owners, ","), region)
	}
	// Newest first; ties break on image ID so the choice is stable.
	sort.Slice(imgs, func(i, j int) bool {
		ci, cj := awssdk.ToString(imgs[i].CreationDate), awssdk.ToString(imgs[j].CreationDate)
		if ci != cj {
			return ci > cj
		}
		return awssdk.ToString(imgs[i].ImageId) < awssdk.ToString(imgs[j].ImageId)
	})
	id := awssdk.ToString(imgs[0].ImageId)
	log.Printf("[INFO] AMI resolution: %q in %s is %s (%s)", nameFilter, region, id, awssdk.ToString(imgs[0].Name))
	return id, nil
}
