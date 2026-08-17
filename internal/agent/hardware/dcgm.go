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

// dcgmDeviceFields are polled once per sample in addition to the summary
// GetDeviceStatus call, which does not cover throttle reasons, XID errors or
// row-remap counts.
var dcgmDeviceFields = []dcgm.Short{
	dcgm.DCGM_FI_DEV_FB_FREE,
	dcgm.DCGM_FI_DEV_CLOCK_THROTTLE_REASONS,
	dcgm.DCGM_FI_DEV_XID_ERROR,
	dcgm.DCGM_FI_DEV_ROW_REMAP_UNCORRECTABLE_TOTAL,
}

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
}

// newDCGMSource starts DCGM (embedded host engine, one per node -- the
// simplest mode for a per-machine agent) and builds the GPU-ID-to-device-index
// mapping.
func newDCGMSource(log *slog.Logger) (*DCGMSource, error) {
	if _, err := dcgm.Init(dcgm.Embedded); err != nil {
		return nil, fmt.Errorf("start embedded host engine: %w", err)
	}

	gpuIndex, eccPresent, err := reconcileDeviceIndices()
	if err != nil {
		dcgm.Shutdown()
		return nil, err
	}

	return &DCGMSource{log: log, gpuIndex: gpuIndex, eccPresent: eccPresent}, nil
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

func (s *DCGMSource) sample() ([]domain.HealthSample, error) {
	now := time.Now()
	out := make([]domain.HealthSample, 0, len(s.gpuIndex))

	for gpuID, index := range s.gpuIndex {
		status, err := dcgm.GetDeviceStatus(gpuID)
		if err != nil {
			return nil, fmt.Errorf("gpu %d: get device status: %w", gpuID, err)
		}

		values, err := dcgm.GetLatestValuesForFields(gpuID, dcgmDeviceFields)
		if err != nil {
			return nil, fmt.Errorf("gpu %d: get latest field values: %w", gpuID, err)
		}
		freeVRAMGB, throttleBits, xid, rowRemap := decodeFields(values)

		sample := domain.HealthSample{
			DeviceIndex:       index,
			Utilization:       float64(status.Utilization.GPU) / 100,
			FreeVRAMGB:        freeVRAMGB,
			TemperatureC:      float64(status.Temperature),
			PowerW:            status.Power,
			ThrottleReasons:   decodeThrottleReasons(throttleBits),
			PCIeReplayCount:   uint64(status.PCI.Throughput.Replays),
			ECCSupported:      s.eccPresent[index],
			ECCVolatileErrors: uint64(status.Memory.ECCErrors.DoubleBit),
			RowRemapPending:   rowRemap,
			SampledAt:         now,
		}
		if xid > 0 {
			sample.XIDErrors = []int{xid}
		}
		out = append(out, sample)
	}
	return out, nil
}

// decodeFields pulls the values requested via dcgmDeviceFields out in order,
// treating DCGM's "blank" (no data / not supported) sentinels as zero rather
// than as a huge sentinel integer.
func decodeFields(values []dcgm.FieldValue_v1) (freeVRAMGB int, throttleBits uint64, xid int, rowRemap uint64) {
	for _, v := range values {
		val := v.Int64()
		if val >= dcgm.DCGM_FT_INT64_BLANK {
			continue
		}
		switch v.FieldID {
		case dcgm.DCGM_FI_DEV_FB_FREE:
			freeVRAMGB = int(val / 1024) // MB -> GB
		case dcgm.DCGM_FI_DEV_CLOCK_THROTTLE_REASONS:
			throttleBits = uint64(val)
		case dcgm.DCGM_FI_DEV_XID_ERROR:
			xid = int(val)
		case dcgm.DCGM_FI_DEV_ROW_REMAP_UNCORRECTABLE_TOTAL:
			rowRemap = uint64(val)
		}
	}
	return freeVRAMGB, throttleBits, xid, rowRemap
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
