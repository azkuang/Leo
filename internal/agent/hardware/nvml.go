package hardware

import (
	"context"
	"fmt"

	"github.com/NVIDIA/go-nvml/pkg/nvml"

	"github.com/alexk/orch/internal/domain"
)

// NVMLProvider enumerates NVIDIA accelerators via NVML.
//
// nvml.Init/Shutdown are process-global, not per-handle, so this type carries
// no state of its own -- it is a receiver to satisfy agent.DeviceProvider and
// to give dcgm.go and hostprobe.go a place to reach a couple of NVML queries
// DCGM does not cover (max PCIe link generation, driver/CUDA version).
type NVMLProvider struct{}

// Enumerate reports every NVIDIA device NVML can see, in NVML's index order.
// That index is also what DCGM, containerd's NVIDIA_VISIBLE_DEVICES, and the
// scheduler's DeviceIndices all agree on -- it is the one true device index
// for this node.
func (p *NVMLProvider) Enumerate(ctx context.Context) ([]domain.Device, error) {
	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("nvml: get device count: %v", nvml.ErrorString(ret))
	}

	out := make([]domain.Device, 0, count)
	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		dev, ret := nvml.DeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			return nil, fmt.Errorf("nvml: get handle for device %d: %v", i, nvml.ErrorString(ret))
		}

		d, err := deviceFromNVML(i, dev)
		if err != nil {
			return nil, fmt.Errorf("nvml: describe device %d: %w", i, err)
		}
		out = append(out, d)
	}
	return out, nil
}

func deviceFromNVML(index int, dev nvml.Device) (domain.Device, error) {
	name, ret := dev.GetName()
	if ret != nvml.SUCCESS {
		return domain.Device{}, fmt.Errorf("get name: %v", nvml.ErrorString(ret))
	}

	mem, ret := dev.GetMemoryInfo()
	if ret != nvml.SUCCESS {
		return domain.Device{}, fmt.Errorf("get memory info: %v", nvml.ErrorString(ret))
	}

	major, minor, ret := dev.GetCudaComputeCapability()
	if ret != nvml.SUCCESS {
		return domain.Device{}, fmt.Errorf("get compute capability: %v", nvml.ErrorString(ret))
	}

	// A NOT_SUPPORTED return means the device cannot do MIG at all, which is
	// itself the answer to "is it MIG-capable" -- distinct from whether MIG is
	// currently enabled, which this enumeration does not report (MIG
	// partitioning is deferred; see Leo.md §16 on the unresolved
	// MIG-vs-attached-display question).
	migCapable := false
	if _, _, ret := dev.GetMigMode(); ret == nvml.SUCCESS {
		migCapable = true
	}

	encodeCapacity, ret := dev.GetEncoderCapacity(nvml.ENCODER_QUERY_H264)
	if ret != nvml.SUCCESS {
		// Not every SKU supports the query (e.g. no NVENC engine at all).
		// Treated as "zero sessions available", not an enumeration failure.
		encodeCapacity = 0
	}

	// NVML has no equivalent decoder *capacity* query -- only
	// DeviceGetDecoderUtilization, which reports current load, not a limit.
	// Reporting a real number here would mean guessing from a static
	// per-model table, which this codebase deliberately avoids (Model is
	// informational; branching or fabricating from it is not). Left at zero
	// until a real source exists.
	const decodeCapacity = 0

	eccPresent := false
	if _, ret := dev.GetTotalEccErrors(nvml.MEMORY_ERROR_TYPE_UNCORRECTED, nvml.VOLATILE_ECC); ret == nvml.SUCCESS {
		eccPresent = true
	}

	return domain.Device{
		Index:             index,
		Vendor:            "nvidia",
		Model:             name,
		VRAMGB:            int(mem.Total / (1 << 30)),
		ComputeCapability: fmt.Sprintf("%d.%d", major, minor),
		MIGCapable:        migCapable,
		EncodeSessions:    encodeCapacity,
		DecodeSessions:    decodeCapacity,
		ECCPresent:        eccPresent,
	}, nil
}

// driverAndCUDAVersion reports the host's driver and CUDA versions, for
// HostProbe's Software half.
func driverAndCUDAVersion() (driver, cuda string, err error) {
	driver, ret := nvml.SystemGetDriverVersion()
	if ret != nvml.SUCCESS {
		return "", "", fmt.Errorf("get driver version: %v", nvml.ErrorString(ret))
	}
	cudaInt, ret := nvml.SystemGetCudaDriverVersion()
	if ret != nvml.SUCCESS {
		return "", "", fmt.Errorf("get cuda driver version: %v", nvml.ErrorString(ret))
	}
	// NVML packs CUDA version as major*1000 + minor*10.
	cuda = fmt.Sprintf("%d.%d", cudaInt/1000, (cudaInt%1000)/10)
	return driver, cuda, nil
}

// maxPCIeGeneration reports the highest PCIe link generation any enumerated
// device supports, as a representative value for the host's Host.PCIeGen.
// Devices behind different PCIe generations on the same host are rare enough
// on workstation hardware that a single representative value is an
// acceptable simplification here.
func maxPCIeGeneration() int {
	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return 0
	}
	best := 0
	for i := 0; i < count; i++ {
		dev, ret := nvml.DeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			continue
		}
		gen, ret := dev.GetMaxPcieLinkGeneration()
		if ret != nvml.SUCCESS {
			continue
		}
		if gen > best {
			best = gen
		}
	}
	return best
}
