package hardware

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/NVIDIA/go-dcgm/pkg/dcgm"
	"github.com/NVIDIA/go-nvml/pkg/nvml"

	"github.com/alexk/orch/internal/domain"
)

// Clock throttle reason bits, as defined by nvml.h's nvmlClocksThrottleReasons
// and mirrored bit-for-bit by DCGM_FI_DEV_CLOCK_THROTTLE_REASONS. DCGM does
// not re-export these as named constants, so they are reproduced here.
const (
	throttleGPUIdle                  uint64 = 0x0000000000000001
	throttleApplicationsClockSetting uint64 = 0x0000000000000002
	throttleSWPowerCap               uint64 = 0x0000000000000004
	throttleHWSlowdown               uint64 = 0x0000000000000008
	throttleSyncBoost                uint64 = 0x0000000000000010
	throttleSWThermalSlowdown        uint64 = 0x0000000000000020
	throttleHWThermalSlowdown        uint64 = 0x0000000000000040
	throttleHWPowerBrakeSlowdown     uint64 = 0x0000000000000080
	throttleDisplayClockSetting      uint64 = 0x0000000000000100
)

var throttleNames = []struct {
	bit  uint64
	name string
}{
	{throttleSWThermalSlowdown, "sw_thermal_slowdown"},
	{throttleHWThermalSlowdown, "hw_thermal_slowdown"},
	{throttleHWSlowdown, "hw_slowdown"},
	{throttleSWPowerCap, "sw_power_cap"},
	{throttleHWPowerBrakeSlowdown, "hw_power_brake_slowdown"},
	{throttleSyncBoost, "sync_boost"},
	{throttleApplicationsClockSetting, "applications_clocks_setting"},
	{throttleDisplayClockSetting, "display_clock_setting"},
	{throttleGPUIdle, "gpu_idle"},
}

// dcgmFields are every field the health sample needs, watched once as a
// single persistent field group rather than the summary GetDeviceStatus call
// this used to build on. GetDeviceStatus does no blank-sentinel filtering of
// its own, so on GeForce-class cards (no ECC, no PCIe replay counting) it
// surfaced DCGM's "not supported" sentinel
// (9223372036854775792) as if it were a real reading. Reading every field
// through this one path lets decodeFieldValues apply the same "blank means
// unsupported, not zero" rule everywhere.
var dcgmFields = []dcgm.Short{
	dcgm.DCGM_FI_DEV_GPU_UTIL,
	dcgm.DCGM_FI_DEV_FB_FREE,
	dcgm.DCGM_FI_DEV_GPU_TEMP,
	dcgm.DCGM_FI_DEV_POWER_USAGE,
	dcgm.DCGM_FI_DEV_CLOCK_THROTTLE_REASONS,
	dcgm.DCGM_FI_DEV_XID_ERROR,
	dcgm.DCGM_FI_DEV_ROW_REMAP_UNCORRECTABLE_TOTAL,
	dcgm.DCGM_FI_DEV_PCIE_REPLAY_COUNTER,
	dcgm.DCGM_FI_DEV_ECC_DBE_VOL_TOTAL,
}

// Watch parameters for the persistent field group. The 1s update frequency
// matches Stream's own sample cadence; maxKeepAge/maxKeepSamples are small
// because only the latest value is ever read (EntitiesGetLatestValues), not
// a history.
const (
	dcgmUpdateFreqUsec = 1_000_000
	dcgmMaxKeepAgeSec  = 30.0
	dcgmMaxKeepSamples = 5
)

// DCGMSource streams fast-path GPU health telemetry via DCGM.
type DCGMSource struct {
	log *slog.Logger

	// gpuIndex maps a DCGM-assigned GPU ID to the NVML device index used
	// everywhere else in the system (capability doc, DeviceIndices,
	// scheduler). The two numbering schemes usually agree on a single
	// workstation but are not guaranteed to, so they are reconciled by GPU
	// UUID once at startup rather than assumed equal.
	gpuIndex map[uint]int
	// eccPresent mirrors NVMLProvider's per-device ECC capability, keyed by
	// the same NVML index, so a health sample can report ECCSupported
	// without a second NVML round-trip per tick.
	eccPresent map[int]bool

	// fieldGroup/watchGroup/entities are created once here rather than
	// per-sample: DCGM only populates fields that are actively being
	// watched, and creating the watch is not free, so it belongs at startup,
	// not on every tick.
	fieldGroup dcgm.FieldHandle
	watchGroup dcgm.GroupHandle
	entities   []dcgm.GroupEntityPair
}

// newDCGMSource starts DCGM (embedded host engine, one per node -- the
// simplest mode for a per-machine agent), builds the GPU-ID-to-device-index
// mapping, and establishes one persistent watch over dcgmFields for every
// GPU on the node.
func newDCGMSource(log *slog.Logger) (*DCGMSource, error) {
	if _, err := dcgm.Init(dcgm.Embedded); err != nil {
		return nil, fmt.Errorf("start embedded host engine: %w", err)
	}

	gpuIndex, eccPresent, err := reconcileDeviceIndices()
	if err != nil {
		dcgm.Shutdown()
		return nil, err
	}

	fieldGroup, err := dcgm.FieldGroupCreate("orch-health", dcgmFields)
	if err != nil {
		dcgm.Shutdown()
		return nil, fmt.Errorf("create field group: %w", err)
	}

	watchGroup, err := dcgm.CreateGroup("orch-health")
	if err != nil {
		_ = dcgm.FieldGroupDestroy(fieldGroup)
		dcgm.Shutdown()
		return nil, fmt.Errorf("create watch group: %w", err)
	}

	entities := make([]dcgm.GroupEntityPair, 0, len(gpuIndex))
	for gpuID := range gpuIndex {
		if err := dcgm.AddEntityToGroup(watchGroup, dcgm.FE_GPU, gpuID); err != nil {
			_ = dcgm.DestroyGroup(watchGroup)
			_ = dcgm.FieldGroupDestroy(fieldGroup)
			dcgm.Shutdown()
			return nil, fmt.Errorf("add gpu %d to watch group: %w", gpuID, err)
		}
		entities = append(entities, dcgm.GroupEntityPair{EntityGroupId: dcgm.FE_GPU, EntityId: gpuID})
	}

	// This is the fix: the previous version of this code called
	// GetLatestValuesForFields without ever watching the fields it read, so
	// every value came back as DCGM's "blank" sentinel, decoded as zero --
	// Throttled()/Degraded() could never fire on real hardware regardless of
	// the card's actual state.
	if err := dcgm.WatchFieldsWithGroupEx(
		fieldGroup, watchGroup, dcgmUpdateFreqUsec, dcgmMaxKeepAgeSec, dcgmMaxKeepSamples,
	); err != nil {
		_ = dcgm.DestroyGroup(watchGroup)
		_ = dcgm.FieldGroupDestroy(fieldGroup)
		dcgm.Shutdown()
		return nil, fmt.Errorf("watch fields: %w", err)
	}

	return &DCGMSource{
		log:        log,
		gpuIndex:   gpuIndex,
		eccPresent: eccPresent,
		fieldGroup: fieldGroup,
		watchGroup: watchGroup,
		entities:   entities,
	}, nil
}

// Close stops the field watch and shuts down the embedded DCGM host engine.
func (s *DCGMSource) Close() error {
	var errs []error
	if err := dcgm.UnwatchFields(s.fieldGroup, s.watchGroup); err != nil {
		errs = append(errs, fmt.Errorf("unwatch fields: %w", err))
	}
	if err := dcgm.DestroyGroup(s.watchGroup); err != nil {
		errs = append(errs, fmt.Errorf("destroy watch group: %w", err))
	}
	if err := dcgm.FieldGroupDestroy(s.fieldGroup); err != nil {
		errs = append(errs, fmt.Errorf("destroy field group: %w", err))
	}
	dcgm.Shutdown()
	return joinErrs(errs)
}

// reconcileDeviceIndices matches each DCGM-reported GPU to the NVML device
// index of the same physical card, by UUID -- the one identifier both
// libraries report for the same device.
func reconcileDeviceIndices() (map[uint]int, map[int]bool, error) {
	nvmlUUIDToIndex := map[string]int{}
	nvmlEccPresent := map[int]bool{}

	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return nil, nil, fmt.Errorf("nvml: get device count: %v", nvml.ErrorString(ret))
	}
	for i := 0; i < count; i++ {
		dev, ret := nvml.DeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			return nil, nil, fmt.Errorf("nvml: get handle for device %d: %v", i, nvml.ErrorString(ret))
		}
		uuid, ret := dev.GetUUID()
		if ret != nvml.SUCCESS {
			return nil, nil, fmt.Errorf("nvml: get uuid for device %d: %v", i, nvml.ErrorString(ret))
		}
		nvmlUUIDToIndex[uuid] = i
		_, eccRet := dev.GetTotalEccErrors(nvml.MEMORY_ERROR_TYPE_UNCORRECTED, nvml.VOLATILE_ECC)
		nvmlEccPresent[i] = eccRet == nvml.SUCCESS
	}

	gpuIDs, err := dcgm.GetSupportedDevices()
	if err != nil {
		return nil, nil, fmt.Errorf("dcgm: get supported devices: %w", err)
	}

	gpuIndex := make(map[uint]int, len(gpuIDs))
	for _, id := range gpuIDs {
		info, err := dcgm.GetDeviceInfo(id)
		if err != nil {
			return nil, nil, fmt.Errorf("dcgm: get device info for gpu %d: %w", id, err)
		}
		idx, ok := nvmlUUIDToIndex[info.UUID]
		if !ok {
			return nil, nil, fmt.Errorf("dcgm: gpu %d (uuid %s) has no matching NVML device", id, info.UUID)
		}
		gpuIndex[id] = idx
	}
	return gpuIndex, nvmlEccPresent, nil
}

// Stream emits one telemetry batch per second, matching the simulated
// source's cadence so the fast health path behaves identically either way.
func (s *DCGMSource) Stream(ctx context.Context) (<-chan []domain.HealthSample, error) {
	ch := make(chan []domain.HealthSample, 4)

	go func() {
		defer close(ch)
		t := time.NewTicker(time.Second)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				batch, err := s.sample()
				if err != nil {
					s.log.Error("dcgm sample failed", "err", err)
					continue
				}
				select {
				case ch <- batch:
				default:
					// Consumer is behind; the next tick is fresher anyway.
				}
			}
		}
	}()

	return ch, nil
}

// sample reads every watched field for every GPU in one round trip
// (UpdateAllFields + EntitiesGetLatestValues), rather than one DCGM call per
// device -- which is also what removes the hidden per-second field-group
// churn that lived inside the old GetDeviceStatus call.
func (s *DCGMSource) sample() ([]domain.HealthSample, error) {
	if err := dcgm.UpdateAllFields(); err != nil {
		return nil, fmt.Errorf("update fields: %w", err)
	}
	values, err := dcgm.EntitiesGetLatestValues(s.entities, dcgmFields, 0)
	if err != nil {
		return nil, fmt.Errorf("get latest field values: %w", err)
	}

	byGPU := make(map[uint][]dcgm.FieldValue_v2, len(s.gpuIndex))
	for _, v := range values {
		byGPU[v.EntityID] = append(byGPU[v.EntityID], v)
	}

	now := time.Now()
	out := make([]domain.HealthSample, 0, len(s.gpuIndex))
	for gpuID, index := range s.gpuIndex {
		d := decodeFieldValues(byGPU[gpuID])
		sample := domain.HealthSample{
			DeviceIndex:       index,
			Utilization:       d.utilization,
			FreeVRAMGB:        d.freeVRAMGB,
			TemperatureC:      d.temperatureC,
			PowerW:            d.powerW,
			ThrottleReasons:   decodeThrottleReasons(d.throttleBits),
			PCIeReplayCount:   d.pcieReplayCount,
			ECCSupported:      s.eccPresent[index],
			ECCVolatileErrors: d.eccVolatileErrors,
			RowRemapPending:   d.rowRemapPending,
			SampledAt:         now,
		}
		if d.xid > 0 {
			sample.XIDErrors = []int{d.xid}
		}
		out = append(out, sample)
	}
	return out, nil
}

// decodedFields is the pure-Go result of decodeFieldValues, kept separate
// from domain.HealthSample so the decode step is unit-testable without
// constructing a full sample.
type decodedFields struct {
	utilization       float64
	freeVRAMGB        int
	temperatureC      float64
	powerW            float64
	throttleBits      uint64
	xid               int
	rowRemapPending   uint64
	pcieReplayCount   uint64
	eccVolatileErrors uint64
}

// decodeFieldValues turns one GPU's watched fields into the values a health
// sample needs, treating DCGM's blank ("no data" / "not supported")
// sentinels as unsupported -- left at the zero value -- rather than as a
// real reading. Conflating "0 replays" with "this card has no PCIe replay
// counter" is exactly the bug this replaces: on a GeForce-class card the old
// GetDeviceStatus path surfaced the raw sentinel
// (9223372036854775792) as if it were a real count.
func decodeFieldValues(values []dcgm.FieldValue_v2) decodedFields {
	var d decodedFields
	for _, v := range values {
		if v.FieldType == dcgm.DCGM_FT_DOUBLE {
			f := v.Float64()
			if f >= dcgm.DCGM_FT_FP64_BLANK {
				continue
			}
			if v.FieldID == dcgm.DCGM_FI_DEV_POWER_USAGE {
				d.powerW = f
			}
			continue
		}

		i := v.Int64()
		if i >= dcgm.DCGM_FT_INT64_BLANK {
			continue
		}
		switch v.FieldID {
		case dcgm.DCGM_FI_DEV_GPU_UTIL:
			d.utilization = float64(i) / 100
		case dcgm.DCGM_FI_DEV_FB_FREE:
			d.freeVRAMGB = int(i / 1024) // MB -> GB
		case dcgm.DCGM_FI_DEV_GPU_TEMP:
			d.temperatureC = float64(i)
		case dcgm.DCGM_FI_DEV_CLOCK_THROTTLE_REASONS:
			d.throttleBits = uint64(i)
		case dcgm.DCGM_FI_DEV_XID_ERROR:
			d.xid = int(i)
		case dcgm.DCGM_FI_DEV_ROW_REMAP_UNCORRECTABLE_TOTAL:
			d.rowRemapPending = uint64(i)
		case dcgm.DCGM_FI_DEV_PCIE_REPLAY_COUNTER:
			d.pcieReplayCount = uint64(i)
		case dcgm.DCGM_FI_DEV_ECC_DBE_VOL_TOTAL:
			d.eccVolatileErrors = uint64(i)
		}
	}
	return d
}

func decodeThrottleReasons(bits uint64) []string {
	if bits == 0 {
		return []string{"none"}
	}
	var reasons []string
	for _, r := range throttleNames {
		if bits&r.bit != 0 {
			reasons = append(reasons, r.name)
		}
	}
	if len(reasons) == 0 {
		reasons = []string{"none"}
	}
	return reasons
}
