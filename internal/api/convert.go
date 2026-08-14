package api

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orchv1 "github.com/alexk/orch/gen/orch/v1"
	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/store"
)

// Conversions between the wire types and the domain types.
//
// Keeping these apart means the scheduler never holds a protobuf message and
// the wire format can change without reaching into placement logic.

func fromProtoCapabilities(in *orchv1.CapabilityDoc) domain.CapabilityDoc {
	out := domain.CapabilityDoc{Labels: in.GetLabels()}
	for _, d := range in.GetDevices() {
		out.Devices = append(out.Devices, domain.Device{
			Index:             int(d.GetIndex()),
			Vendor:            d.GetVendor(),
			Model:             d.GetModel(),
			VRAMGB:            int(d.GetVramGb()),
			ComputeCapability: d.GetComputeCapability(),
			MIGCapable:        d.GetMigCapable(),
			MIGProfile:        d.GetMigProfile(),
			EncodeSessions:    int(d.GetEncodeSessions()),
			DecodeSessions:    int(d.GetDecodeSessions()),
			ECCPresent:        d.GetEccPresent(),
		})
	}
	if h := in.GetHost(); h != nil {
		out.Host = domain.Host{
			Cores:     int(h.GetCores()),
			RAMGB:     int(h.GetRamGb()),
			ScratchGB: int(h.GetScratchGb()),
			PCIeGen:   int(h.GetPcieGen()),
		}
	}
	if s := in.GetSoftware(); s != nil {
		out.Software = domain.Software{
			Driver:  s.GetDriver(),
			CUDA:    s.GetCuda(),
			Runtime: s.GetRuntime(),
		}
	}
	return out
}

func toProtoCapabilities(in domain.CapabilityDoc) *orchv1.CapabilityDoc {
	out := &orchv1.CapabilityDoc{
		Labels: in.Labels,
		Host: &orchv1.HostSpec{
			Cores:     uint32(in.Host.Cores),
			RamGb:     uint32(in.Host.RAMGB),
			ScratchGb: uint32(in.Host.ScratchGB),
			PcieGen:   uint32(in.Host.PCIeGen),
		},
		Software: &orchv1.SoftwareSpec{
			Driver:  in.Software.Driver,
			Cuda:    in.Software.CUDA,
			Runtime: in.Software.Runtime,
		},
	}
	for _, d := range in.Devices {
		out.Devices = append(out.Devices, &orchv1.Device{
			Index:             uint32(d.Index),
			Vendor:            d.Vendor,
			Model:             d.Model,
			VramGb:            uint32(d.VRAMGB),
			ComputeCapability: d.ComputeCapability,
			MigCapable:        d.MIGCapable,
			MigProfile:        d.MIGProfile,
			EncodeSessions:    uint32(d.EncodeSessions),
			DecodeSessions:    uint32(d.DecodeSessions),
			EccPresent:        d.ECCPresent,
		})
	}
	return out
}

func fromProtoSamples(in []*orchv1.HealthSample) []domain.HealthSample {
	out := make([]domain.HealthSample, 0, len(in))
	for _, s := range in {
		xids := make([]int, 0, len(s.GetXidErrors()))
		for _, x := range s.GetXidErrors() {
			xids = append(xids, int(x))
		}
		out = append(out, domain.HealthSample{
			DeviceIndex:       int(s.GetDeviceIndex()),
			Utilization:       s.GetUtilization(),
			FreeVRAMGB:        int(s.GetFreeVramGb()),
			TemperatureC:      s.GetTemperatureC(),
			PowerW:            s.GetPowerW(),
			ThrottleReasons:   s.GetThrottleReasons(),
			PCIeReplayCount:   s.GetPcieReplayCount(),
			XIDErrors:         xids,
			ECCSupported:      s.GetEccSupported(),
			ECCVolatileErrors: s.GetEccVolatileErrors(),
			RowRemapPending:   s.GetRowRemapPending(),
			SampledAt:         time.Now(),
		})
	}
	return out
}

func toProtoHealth(s domain.HealthSample) *orchv1.HealthSample {
	xids := make([]uint32, 0, len(s.XIDErrors))
	for _, x := range s.XIDErrors {
		xids = append(xids, uint32(x))
	}
	return &orchv1.HealthSample{
		DeviceIndex:       uint32(s.DeviceIndex),
		Utilization:       s.Utilization,
		FreeVramGb:        uint32(s.FreeVRAMGB),
		TemperatureC:      s.TemperatureC,
		PowerW:            s.PowerW,
		ThrottleReasons:   s.ThrottleReasons,
		PcieReplayCount:   s.PCIeReplayCount,
		XidErrors:         xids,
		EccSupported:      s.ECCSupported,
		EccVolatileErrors: s.ECCVolatileErrors,
		RowRemapPending:   s.RowRemapPending,
	}
}

func toProtoAssets(in []domain.AssetRef) []*orchv1.AssetRef {
	out := make([]*orchv1.AssetRef, 0, len(in))
	for _, a := range in {
		out = append(out, &orchv1.AssetRef{
			Digest:    a.Digest,
			Uri:       a.URI,
			SizeBytes: a.SizeBytes,
			MountPath: a.MountPath,
		})
	}
	return out
}

func fromProtoAssets(in []*orchv1.AssetRef) []domain.AssetRef {
	out := make([]domain.AssetRef, 0, len(in))
	for _, a := range in {
		out = append(out, domain.AssetRef{
			Digest:    a.GetDigest(),
			URI:       a.GetUri(),
			SizeBytes: a.GetSizeBytes(),
			MountPath: a.GetMountPath(),
		})
	}
	return out
}

func fromProtoRequest(in *orchv1.ResourceRequest) domain.ResourceRequest {
	return domain.ResourceRequest{
		Slots:                int(in.GetSlots()),
		MinVRAMGB:            int(in.GetMinVramGb()),
		MinComputeCapability: in.GetMinComputeCapability(),
		MinDriver:            in.GetMinDriver(),
		MinCUDA:              in.GetMinCuda(),
		MinEncodeSessions:    int(in.GetMinEncodeSessions()),
		MinDecodeSessions:    int(in.GetMinDecodeSessions()),
		RequireECC:           in.GetRequireEcc(),
		RequiredLabels:       in.GetRequiredLabels(),
		ExclusiveHost:        in.GetExclusiveHost(),
	}
}

func toProtoRequest(in domain.ResourceRequest) *orchv1.ResourceRequest {
	return &orchv1.ResourceRequest{
		Slots:                uint32(in.Slots),
		MinVramGb:            uint32(in.MinVRAMGB),
		MinComputeCapability: in.MinComputeCapability,
		MinDriver:            in.MinDriver,
		MinCuda:              in.MinCUDA,
		MinEncodeSessions:    uint32(in.MinEncodeSessions),
		MinDecodeSessions:    uint32(in.MinDecodeSessions),
		RequireEcc:           in.RequireECC,
		RequiredLabels:       in.RequiredLabels,
		ExclusiveHost:        in.ExclusiveHost,
	}
}

func fromProtoTaskState(s orchv1.TaskState) domain.TaskState {
	switch s {
	case orchv1.TaskState_TASK_STATE_QUEUED:
		return domain.TaskQueued
	case orchv1.TaskState_TASK_STATE_ASSIGNED:
		return domain.TaskAssigned
	case orchv1.TaskState_TASK_STATE_STAGING:
		return domain.TaskStaging
	case orchv1.TaskState_TASK_STATE_RUNNING:
		return domain.TaskRunning
	case orchv1.TaskState_TASK_STATE_DONE:
		return domain.TaskDone
	case orchv1.TaskState_TASK_STATE_FAILED:
		return domain.TaskFailed
	case orchv1.TaskState_TASK_STATE_PREEMPTED:
		return domain.TaskPreempted
	}
	return domain.TaskQueued
}

func toProtoTaskState(s domain.TaskState) orchv1.TaskState {
	switch s {
	case domain.TaskQueued:
		return orchv1.TaskState_TASK_STATE_QUEUED
	case domain.TaskAssigned:
		return orchv1.TaskState_TASK_STATE_ASSIGNED
	case domain.TaskStaging:
		return orchv1.TaskState_TASK_STATE_STAGING
	case domain.TaskRunning:
		return orchv1.TaskState_TASK_STATE_RUNNING
	case domain.TaskDone:
		return orchv1.TaskState_TASK_STATE_DONE
	case domain.TaskFailed:
		return orchv1.TaskState_TASK_STATE_FAILED
	case domain.TaskPreempted:
		return orchv1.TaskState_TASK_STATE_PREEMPTED
	}
	return orchv1.TaskState_TASK_STATE_UNSPECIFIED
}

func fromProtoLeaseMode(m orchv1.LeaseMode) domain.LeaseMode {
	if m == orchv1.LeaseMode_LEASE_MODE_SERVICE {
		return domain.ModeService
	}
	return domain.ModeBatch
}

func toProtoLeaseMode(m domain.LeaseMode) orchv1.LeaseMode {
	if m == domain.ModeService {
		return orchv1.LeaseMode_LEASE_MODE_SERVICE
	}
	return orchv1.LeaseMode_LEASE_MODE_BATCH
}

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// toProtoNode renders a node for the fleet view, including which of its slots
// are held by a borrower.
func toProtoNode(n *store.NodeView, leases []domain.Lease, tasksByLease map[string]leaseTask) *orchv1.Node {
	byLease := make(map[string]domain.Lease, len(leases))
	for _, l := range leases {
		byLease[l.LeaseID] = l
	}

	out := &orchv1.Node{
		NodeId:         n.NodeID,
		Hostname:       n.Hostname,
		PoolId:         n.PoolID,
		State:          string(n.State),
		Capabilities:   toProtoCapabilities(n.Capabilities),
		Labels:         n.Labels,
		Simulated:      n.Simulated,
		LastHeartbeat:  ts(n.LastHeartbeat),
		HostCores:      uint32(n.HostCores),
		HostRamGb:      uint32(n.HostRAMGB),
		HostScratchGb:  uint32(n.HostScratchGB),
		AllocatedCores: n.AllocatedCores,
		AllocatedRamGb: uint32(n.AllocatedRAMGB),
		Lendable:       n.Lendable,
		LoadAvg_1M:     float64(n.LoadAvg1m),
	}

	for _, s := range n.Slots {
		slot := &orchv1.Slot{
			SlotId:            s.SlotID,
			NodeId:            s.NodeID,
			Kind:              string(s.Kind),
			DeviceIndex:       uint32(s.DeviceIndex),
			VramGb:            uint32(s.VRAMGB),
			ComputeCapability: s.ComputeCapability,
			EncodeSessions:    uint32(s.EncodeSessions),
			DecodeSessions:    uint32(s.DecodeSessions),
			EccPresent:        s.ECCPresent,
			State:             string(s.State),
			LeaseId:           s.LeaseID,
			Health:            toProtoHealth(s.Health),
		}
		if l, ok := byLease[s.LeaseID]; ok {
			slot.PoolId = l.PoolID
			slot.Borrowed = l.Borrowed
			slot.JobId = l.JobID
			slot.TaskId = l.TaskID
			if t, ok := tasksByLease[s.LeaseID]; ok {
				slot.JobId, slot.TaskId = t.JobID, t.TaskID
			}
		}
		out.Slots = append(out.Slots, slot)
	}
	return out
}

type leaseTask struct {
	JobID  string
	TaskID string
}

func toProtoJob(j domain.Job, tasks []domain.Task) *orchv1.Job {
	out := &orchv1.Job{
		JobId:       j.JobID,
		Name:        j.Name,
		Owner:       j.Owner,
		PoolId:      j.PoolID,
		ImageDigest: j.ImageDigest,
		State:       string(j.State),
		Preemptible: j.Preemptible,
		SubmittedAt: ts(j.SubmittedAt),
		TasksTotal:  uint32(len(tasks)),
		Request:     toProtoRequest(j.Request),
	}
	for _, t := range tasks {
		switch t.State {
		case domain.TaskDone:
			out.TasksDone++
		case domain.TaskRunning, domain.TaskStaging, domain.TaskAssigned:
			out.TasksRunning++
		case domain.TaskQueued:
			out.TasksQueued++
		case domain.TaskFailed:
			out.TasksFailed++
		}
	}
	return out
}

func toProtoTask(t domain.Task) *orchv1.Task {
	return &orchv1.Task{
		TaskId:          t.TaskID,
		JobId:           t.JobID,
		Index:           uint32(t.Index),
		State:           toProtoTaskState(t.State),
		Attempt:         uint32(t.Attempt),
		AssignedLeaseId: t.AssignedLeaseID,
		NodeId:          t.NodeID,
		OutputPrefix:    t.OutputPrefix,
		Params:          t.Params,
		StartedAt:       ts(t.StartedAt),
		FinishedAt:      ts(t.FinishedAt),
		Preemptions:     uint32(t.Preemptions),
	}
}

func toProtoLease(l domain.Lease) *orchv1.Lease {
	return &orchv1.Lease{
		LeaseId:        l.LeaseID,
		TaskId:         l.TaskID,
		JobId:          l.JobID,
		PoolId:         l.PoolID,
		SlotIds:        l.SlotIDs,
		NodeId:         l.NodeID,
		Mode:           toProtoLeaseMode(l.Mode),
		Borrowed:       l.Borrowed,
		ExclusiveHost:  l.ExclusiveHost,
		HostShareCores: l.HostShareCores,
		HostShareRamGb: uint32(l.HostShareRAMGB),
		GrantedAt:      ts(l.GrantedAt),
		TtlMs:          uint64(l.TTL.Milliseconds()),
		RenewedAt:      ts(l.RenewedAt),
		State:          string(l.State),
	}
}
