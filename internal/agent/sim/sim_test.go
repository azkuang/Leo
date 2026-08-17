package sim

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alexk/orch/internal/agent"
)

func TestWriteSimOutputWritesIntoWritableMount(t *testing.T) {
	dir := t.TempDir()
	spec := agent.StartSpec{
		Index:   2,
		Attempt: 1,
		Params:  map[string]string{"frame": "10"},
		Mounts: []agent.Mount{
			{HostPath: "/should/not/be/used", ContainerPath: "/inputs/asset", ReadOnly: true},
			{HostPath: dir, ContainerPath: agent.ContainerOutputDir, ReadOnly: false},
		},
	}

	writeSimOutput(spec, 250*time.Millisecond)

	body, err := os.ReadFile(filepath.Join(dir, "output.txt"))
	if err != nil {
		t.Fatalf("expected output.txt to be written: %v", err)
	}
	s := string(body)
	for _, want := range []string{"task_index=2", "attempt=1", "duration_ms=250", "param.frame=10"} {
		if !strings.Contains(s, want) {
			t.Errorf("output.txt missing %q, got:\n%s", want, s)
		}
	}
}

func TestWriteSimOutputNoWritableMountIsNoop(t *testing.T) {
	// Must not panic or create anything when there is no writable mount --
	// e.g. a task with no output mount configured yet.
	writeSimOutput(agent.StartSpec{}, time.Second)
}

func TestNodeCleanupDropsBookkeeping(t *testing.T) {
	n := NewNode(Profile{Devices: []DeviceProfile{{VRAMGB: 8}}, TaskDurationMin: time.Millisecond, TaskDurationMax: 2 * time.Millisecond})
	h, err := n.Start(context.Background(), agent.StartSpec{LeaseID: "l1", DeviceIndices: []int{0}})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := n.Status(context.Background(), h); err != nil {
		t.Fatalf("expected handle to be known before Cleanup: %v", err)
	}
	if err := n.Cleanup(context.Background(), h); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if _, err := n.Status(context.Background(), h); err == nil {
		t.Error("expected handle to be forgotten after Cleanup")
	}
}
