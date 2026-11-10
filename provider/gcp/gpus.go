package gcp

import (
	"fmt"
	"sort"
	"strings"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

// GPU catalogue: one table that maps every GPU model to its accelerator type name,
// the way it reaches the VM (attached to N1, or built into a machine series) and
// the machine types that carry it. Which series a machine type belongs to (disk,
// NIC, -vws licence, Arm, ...) stays in seriesTable; this table only adds the
// GPU <-> machine type <-> count relation.
//
// Sources (Google Cloud docs, read 2026-11-11):
//   - https://docs.cloud.google.com/compute/docs/gpus  (GPU platforms: names, shapes, counts)
//   - https://docs.cloud.google.com/compute/docs/accelerator-optimized-machines
//   - https://docs.cloud.google.com/compute/docs/gpus/gpu-regions-zones
//
// Quoted: "For these machine types, the GPU model is automatically attached to the
// instance." (A and G series); "N1 machine types don't come with a set number of
// attached GPUs. Instead, you specify the number of GPUs to attach when creating
// the instance." (N1).
// UNVERIFIED: that a4x (GB200) is an Arm shape comes from the existing seriesTable,
// the pages read do not state the CPU architecture.

// gpuShape is one machine type that carries a GPU model.
type gpuShape struct {
	MachineType string
	// Count is the number of GPUs of the model on the machine. 0 marks a fractional
	// shape (G4 1/8, 1/4, 1/2 GPU) that no integer count can request: it is only
	// reachable through an explicit spec.instanceType.
	Count int32
	// Fraction is the human form of a fractional Count ("1/8").
	Fraction string
}

// gpuModel is one row of the catalogue.
type gpuModel struct {
	Model string // human name
	// Accel is the accelerator type name Google lists for the model.
	Accel string
	// VWS is the "-vws" workstation-licence accelerator type ("" = none listed).
	VWS string
	// Attached is true for N1-attached GPUs (guestAccelerators) and false for GPUs
	// built into the machine type. Attached GPUs have no Shapes.
	Attached bool
	// Shapes are the machine types carrying the GPU, smallest first. For attached
	// GPUs, MaxCount lists the per-N1 counts Google gives.
	Shapes []gpuShape
	// Counts are the valid attach counts of an attached GPU (N1 platforms table).
	Counts []int32
}

var gpuCatalogue = []gpuModel{
	// ── attached to N1 ──
	{Model: "NVIDIA T4", Accel: "nvidia-tesla-t4", VWS: "nvidia-tesla-t4-vws", Attached: true, Counts: []int32{1, 2, 4}},
	{Model: "NVIDIA P4", Accel: "nvidia-tesla-p4", VWS: "nvidia-tesla-p4-vws", Attached: true, Counts: []int32{1, 2, 4}},
	// V100: no -vws name on the gpus page ("No" in its vWS column); the regions page
	// says "T4 or V100 vWS" (UNVERIFIED conflict) so no V100 -vws is accepted.
	{Model: "NVIDIA V100", Accel: "nvidia-tesla-v100", Attached: true, Counts: []int32{1, 2, 4, 8}},

	// ── built in ──
	{Model: "NVIDIA A100 40GB", Accel: "nvidia-tesla-a100", Shapes: []gpuShape{
		{MachineType: "a2-highgpu-1g", Count: 1}, {MachineType: "a2-highgpu-2g", Count: 2},
		{MachineType: "a2-highgpu-4g", Count: 4}, {MachineType: "a2-highgpu-8g", Count: 8},
		{MachineType: "a2-megagpu-16g", Count: 16}}},
	{Model: "NVIDIA A100 80GB", Accel: "nvidia-a100-80gb", Shapes: []gpuShape{
		{MachineType: "a2-ultragpu-1g", Count: 1}, {MachineType: "a2-ultragpu-2g", Count: 2},
		{MachineType: "a2-ultragpu-4g", Count: 4}, {MachineType: "a2-ultragpu-8g", Count: 8}}},
	{Model: "NVIDIA L4", Accel: "nvidia-l4", VWS: "nvidia-l4-vws", Shapes: []gpuShape{
		// Several g2 shapes have 1 GPU: the smallest is listed first and wins.
		{MachineType: "g2-standard-4", Count: 1}, {MachineType: "g2-standard-8", Count: 1},
		{MachineType: "g2-standard-12", Count: 1}, {MachineType: "g2-standard-16", Count: 1},
		{MachineType: "g2-standard-32", Count: 1}, {MachineType: "g2-standard-24", Count: 2},
		{MachineType: "g2-standard-48", Count: 4}, {MachineType: "g2-standard-96", Count: 8}}},
	{Model: "NVIDIA H100 80GB", Accel: "nvidia-h100-80gb", Shapes: []gpuShape{
		{MachineType: "a3-highgpu-1g", Count: 1}, {MachineType: "a3-highgpu-2g", Count: 2},
		{MachineType: "a3-highgpu-4g", Count: 4}, {MachineType: "a3-highgpu-8g", Count: 8},
		{MachineType: "a3-edgegpu-8g", Count: 8}}},
	{Model: "NVIDIA H100 Mega 80GB", Accel: "nvidia-h100-mega-80gb", Shapes: []gpuShape{
		{MachineType: "a3-megagpu-8g", Count: 8}}},
	{Model: "NVIDIA H200 141GB", Accel: "nvidia-h200-141gb", Shapes: []gpuShape{
		{MachineType: "a3-ultragpu-8g", Count: 8}}},
	{Model: "NVIDIA B200", Accel: "nvidia-b200", Shapes: []gpuShape{
		{MachineType: "a4-highgpu-8g", Count: 8}}},
	// GB200: a4x-highgpu-4g is Arm per seriesTable, which this provisioner rejects.
	{Model: "NVIDIA GB200", Accel: "nvidia-gb200", Shapes: []gpuShape{
		{MachineType: "a4x-highgpu-4g", Count: 4}}},
	{Model: "NVIDIA RTX PRO 6000", Accel: "nvidia-rtx-pro-6000", VWS: "nvidia-rtx-pro-6000-vws", Shapes: []gpuShape{
		{MachineType: "g4-standard-6", Fraction: "1/8"}, {MachineType: "g4-standard-12", Fraction: "1/4"},
		{MachineType: "g4-standard-24", Fraction: "1/2"},
		{MachineType: "g4-standard-48", Count: 1}, {MachineType: "g4-standard-96", Count: 2},
		{MachineType: "g4-standard-192", Count: 4}, {MachineType: "g4-standard-384", Count: 8}}},
}

// gpuByAccel finds the catalogue row for an accelerator type name, including the
// "-vws" variant. isVWS reports a match on the licence name.
func gpuByAccel(name string) (m *gpuModel, isVWS bool) {
	name = strings.ToLower(strings.TrimSpace(name))
	for i := range gpuCatalogue {
		g := &gpuCatalogue[i]
		if g.Accel == name {
			return g, false
		}
		if g.VWS != "" && g.VWS == name {
			return g, true
		}
	}
	return nil, false
}

// gpuByMachine finds the built-in-GPU row and shape of a machine type.
func gpuByMachine(machineType string) (*gpuModel, *gpuShape) {
	mt := strings.ToLower(strings.TrimSpace(machineType))
	for i := range gpuCatalogue {
		for j := range gpuCatalogue[i].Shapes {
			if gpuCatalogue[i].Shapes[j].MachineType == mt {
				return &gpuCatalogue[i], &gpuCatalogue[i].Shapes[j]
			}
		}
	}
	return nil, nil
}

// shapesFor lists the shapes carrying exactly count GPUs.
func (g *gpuModel) shapesFor(count int32) []gpuShape {
	var out []gpuShape
	for _, s := range g.Shapes {
		if s.Count == count && count > 0 {
			out = append(out, s)
		}
	}
	return out
}

// validCounts describes every way to get the model, for error messages.
func (g *gpuModel) validCounts() string {
	if g.Attached {
		var cs []string
		for _, c := range g.Counts {
			cs = append(cs, fmt.Sprint(c))
		}
		return fmt.Sprintf("attach %s GPU(s) to an N1 machine type", strings.Join(cs, ", "))
	}
	by := map[int32][]string{}
	var keys []int
	for _, s := range g.Shapes {
		if s.Count == 0 {
			continue
		}
		if _, ok := by[s.Count]; !ok {
			keys = append(keys, int(s.Count))
		}
		by[s.Count] = append(by[s.Count], s.MachineType)
	}
	sort.Ints(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%d GPU(s): %s", k, strings.Join(by[int32(k)], ", ")))
	}
	return strings.Join(parts, "; ")
}

// gpuResolution is the outcome of resolving accelerator + machine type.
type gpuResolution struct {
	InstanceType string
	Accelerator  *mlv1alpha1.GCPAccelerator // nil = none attached
	// Note is non-empty when the spec was rewritten (logged, and recorded in status.message).
	Note string
}

// genericDefaultMachine is the machine type ResolveDefaults gives a "gpu" nodeLabel.
// It is the only shape treated as "not chosen by the user" and so replaceable.
func genericDefaultMachine() string { return DefaultMachineTypeForLabel("gpu") }

// resolveGPU turns (machineType, accelerator) into a valid pair. machineType is ""
// when the spec left it empty. The returned error is a plain one-sentence error
// (callers wrap it in Permanent). A nil accelerator or an unknown accelerator name
// is passed through untouched (the later checks and the Compute API judge it).
func resolveGPU(machineType string, acc *mlv1alpha1.GCPAccelerator) (gpuResolution, error) {
	res := gpuResolution{InstanceType: machineType, Accelerator: acc}
	if acc == nil || acc.Type == "" {
		return res, nil
	}
	model, isVWS := gpuByAccel(acc.Type)
	if model == nil {
		return res, nil
	}
	if acc.Count < 0 || (model.Attached && acc.Count > maxAccelerators) {
		return res, fmt.Errorf("spec.gcpConfig.accelerator.count must be between 1 and %d", maxAccelerators)
	}

	// ── attached GPU (N1) ──
	if model.Attached {
		if machineType == "" || seriesKey(machineType) == "n1" {
			if acc.Count > 0 && !containsInt32(model.Counts, acc.Count) {
				return res, fmt.Errorf("%s (%s) can be attached in counts %s only, not %d: set spec.gcpConfig.accelerator.count to one of them",
					model.Model, acc.Type, joinInt32(model.Counts), acc.Count)
			}
			return res, nil // validated further by validateAccelerator
		}
		return res, fmt.Errorf("accelerator type %q can only be attached to an N1 machine type, not %q: use an N1 machine type (e.g. n1-standard-8)", acc.Type, machineType)
	}

	// ── built-in GPU ──
	count := acc.Count
	if count == 0 {
		count = 1
	}
	if isVWS {
		// A -vws name is a licence on the machine's own GPUs, valid on g2/g4 only.
		if machineType == "" || machineType == genericDefaultMachine() {
			shapes := model.shapesFor(count)
			if len(shapes) == 0 {
				return res, noShapeErr(model, acc.Type, count)
			}
			res.InstanceType = shapes[0].MachineType
			res.Note = fmt.Sprintf("machine type %s selected for %d x %s (%s licence kept)", shapes[0].MachineType, count, model.Model, acc.Type)
			return res, nil
		}
		return res, nil // machine/licence consistency: validateAccelerator
	}

	// A non-vws name selects the machine type; nothing is attached.
	if machineType != "" {
		if m, shape := gpuByMachine(machineType); m == model {
			if acc.Count != 0 && shape.Count != 0 && acc.Count != shape.Count {
				return res, fmt.Errorf("machine type %s carries %d x %s but spec.gcpConfig.accelerator.count is %d: use %s", machineType, shape.Count, model.Model, acc.Count, wantShapes(model, acc.Count))
			}
			res.Accelerator = nil
			res.Note = fmt.Sprintf("%s is built into %s: spec.gcpConfig.accelerator %q cleared (built-in GPUs are not attached)", model.Model, machineType, acc.Type)
			return res, nil
		}
		if machineType != genericDefaultMachine() {
			return res, fmt.Errorf("accelerator type %q is a %s built into %s, it cannot be used with machine type %q: set spec.instanceType to %s (or clear spec.gcpConfig.accelerator)",
				acc.Type, model.Model, builtinSeriesNames(model), machineType, wantShapes(model, acc.Count))
		}
	}
	// instanceType empty, or the generic default (replaceable).
	shapes := model.shapesFor(count)
	if len(shapes) == 0 {
		return res, noShapeErr(model, acc.Type, count)
	}
	// Prefer a shape that can run on-demand, but fall back to a Spot-only one.
	pick := shapes[0]
	for _, s := range shapes {
		if !spotOnlyMachine(s.MachineType) {
			pick = s
			break
		}
	}
	if st, _ := lookupSeries(pick.MachineType); st.Arm {
		return res, fmt.Errorf("%s (%s) is only available on Arm machine type %s, which this provisioner does not support (x86_64 only): choose another GPU",
			model.Model, acc.Type, pick.MachineType)
	}
	res.InstanceType = pick.MachineType
	res.Accelerator = nil
	if machineType == "" {
		res.Note = fmt.Sprintf("accelerator %q is built into %s: machine type %s selected for %d GPU(s), accelerator cleared", acc.Type, builtinSeriesNames(model), pick.MachineType, count)
	} else {
		res.Note = fmt.Sprintf("accelerator %q is built into %s and cannot be attached to %s: machine type replaced by %s for %d GPU(s), accelerator cleared", acc.Type, builtinSeriesNames(model), machineType, pick.MachineType, count)
	}
	return res, nil
}

func noShapeErr(m *gpuModel, accel string, count int32) error {
	return fmt.Errorf("no machine type carries %d x %s (%s); valid options: %s", count, m.Model, accel, m.validCounts())
}

func wantShapes(m *gpuModel, count int32) string {
	if count > 0 {
		if s := m.shapesFor(count); len(s) > 0 {
			names := make([]string, len(s))
			for i := range s {
				names[i] = s[i].MachineType
			}
			return strings.Join(names, " or ")
		}
	}
	return "one of: " + m.validCounts()
}

func builtinSeriesNames(m *gpuModel) string {
	seen := map[string]bool{}
	var out []string
	for _, s := range m.Shapes {
		k := seriesKey(s.MachineType)
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return strings.Join(out, "/")
}

// NeedsGPUResolution reports whether the spec's accelerator/machine type pair would
// be rewritten or rejected by ResolveDefaults, so the controller runs it even when
// every other default is already filled in.
func NeedsGPUResolution(machineType string, acc *mlv1alpha1.GCPAccelerator) bool {
	r, err := resolveGPU(machineType, acc)
	return err != nil || r.InstanceType != machineType || (r.Accelerator == nil) != (acc == nil)
}

func containsInt32(xs []int32, v int32) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func joinInt32(xs []int32) string {
	s := make([]string, len(xs))
	for i, x := range xs {
		s[i] = fmt.Sprint(x)
	}
	return strings.Join(s, ", ")
}
