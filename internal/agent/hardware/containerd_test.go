package hardware

import (
	"reflect"
	"testing"

	"github.com/alexk/orch/internal/agent"
)

func TestTaskEnvCDIMode(t *testing.T) {
	spec := agent.StartSpec{
		Env:           map[string]string{"B": "2", "A": "1"},
		DeviceIndices: []int{0, 1},
	}
	got := taskEnv(spec, GPUModeCDI)
	want := []string{"A=1", "B=2", "NVIDIA_DRIVER_CAPABILITIES=compute,utility"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("taskEnv(cdi) = %v, want %v", got, want)
	}
}

func TestTaskEnvRuntimeBinaryMode(t *testing.T) {
	spec := agent.StartSpec{DeviceIndices: []int{0, 2}}
	got := taskEnv(spec, GPUModeRuntimeBinary)
	want := []string{"NVIDIA_DRIVER_CAPABILITIES=compute,utility", "NVIDIA_VISIBLE_DEVICES=0,2"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("taskEnv(runtime-binary) = %v, want %v", got, want)
	}
}

func TestTaskEnvNoDevicesOmitsGPUVars(t *testing.T) {
	got := taskEnv(agent.StartSpec{Env: map[string]string{"X": "y"}}, GPUModeCDI)
	want := []string{"X=y"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("taskEnv(no devices) = %v, want %v", got, want)
	}
}

func TestCDINames(t *testing.T) {
	got := cdiNames([]int{0, 1, 3})
	want := []string{"nvidia.com/gpu=0", "nvidia.com/gpu=1", "nvidia.com/gpu=3"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("cdiNames() = %v, want %v", got, want)
	}
}

func TestContainerID(t *testing.T) {
	if got := containerID("lease-abc"); got != "orch-lease-abc" {
		t.Errorf("containerID() = %q, want orch-lease-abc", got)
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}.withDefaults()
	if cfg.GPUMode != GPUModeCDI {
		t.Errorf("default GPUMode = %q, want %q", cfg.GPUMode, GPUModeCDI)
	}
	if cfg.DevShmSizeBytes != defaultDevShmSize {
		t.Errorf("default DevShmSizeBytes = %d, want %d", cfg.DevShmSizeBytes, defaultDevShmSize)
	}
	if cfg.NvidiaContainerRuntimeBinary == "" {
		t.Error("expected a default NvidiaContainerRuntimeBinary")
	}
	if cfg.ContainerdNamespace != "orch" {
		t.Errorf("default namespace = %q, want orch", cfg.ContainerdNamespace)
	}
}
