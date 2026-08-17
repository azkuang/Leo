package hardware

import (
	"bufio"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/alexk/orch/internal/domain"
)

// newHostProbe returns a HostProbe that reads the machine's own non-GPU
// resources plus the driver/CUDA versions NVML reports. scratchDir is
// statfs'd for free space; it should be the same directory the agent's
// content-addressed cache uses, since that is the disk a task's staged
// assets actually compete for.
func newHostProbe(scratchDir string, log *slog.Logger) func() (domain.Host, domain.Software) {
	return func() (domain.Host, domain.Software) {
		host := domain.Host{
			Cores:     runtime.NumCPU(),
			RAMGB:     ramGB(log),
			ScratchGB: scratchGB(scratchDir, log),
			PCIeGen:   maxPCIeGeneration(),
		}

		driver, cuda, err := driverAndCUDAVersion()
		if err != nil {
			// The agent already fails registration if NVML enumeration itself
			// fails, so getting this far with a broken driver query is
			// unexpected; report it and keep going with empty version
			// strings rather than crash the whole node over a cosmetic field.
			log.Warn("could not read driver/cuda version", "err", err)
		}

		return host, domain.Software{
			Driver:  driver,
			CUDA:    cuda,
			Runtime: "containerd",
		}
	}
}

// ramGB reads total physical memory from /proc/meminfo. There is no portable
// syscall for this on Linux; parsing /proc is the standard approach (it is
// what free(1) and most Go host-info libraries do).
func ramGB(log *slog.Logger) int {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		log.Warn("could not read /proc/meminfo", "err", err)
		return 0
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kb, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0
			}
			return int(kb / (1 << 20)) // kB -> GB
		}
	}
	return 0
}

// scratchGB statfs's the cache directory's filesystem and reports free space.
// Free, not total, because ScratchGB is read as "room available for staging",
// the same meaning the simulated profile's static value approximates.
func scratchGB(dir string, log *slog.Logger) int {
	if dir == "" {
		dir = os.TempDir()
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		log.Warn("could not statfs scratch directory", "dir", dir, "err", err)
		return 0
	}
	// #nosec G115 -- block counts/sizes on real filesystems fit comfortably.
	return int(st.Bavail * uint64(st.Bsize) / (1 << 30))
}
