package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"connectrpc.com/connect"

	orchv1 "github.com/alexk/orch/gen/orch/v1"
	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/events"
	"github.com/alexk/orch/internal/store"
)

// Hub owns the agent connections.
//
// One bidirectional stream per node carries heartbeats up and assignments down.
// The hub is also the scheduler's Dispatcher: the loop decides, the hub
// delivers.
type Hub struct {
	store store.Store
	bus   *events.Bus
	log   *slog.Logger

	heartbeatInterval time.Duration

	mu    sync.RWMutex
	conns map[string]*conn
}

type conn struct {
	nodeID    string
	hostname  string
	simulated bool
	send      chan *orchv1.ControlMessage
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *conn) close() {
	c.closeOnce.Do(func() { close(c.closed) })
}

// NewHub creates an agent hub.
func NewHub(st store.Store, bus *events.Bus, log *slog.Logger, heartbeatInterval time.Duration) *Hub {
	return &Hub{
		store:             st,
		bus:               bus,
		log:               log,
		heartbeatInterval: heartbeatInterval,
		conns:             map[string]*conn{},
	}
}

// Connect implements the agent-facing bidirectional stream.
func (h *Hub) Connect(
	ctx context.Context,
	stream *connect.BidiStream[orchv1.AgentMessage, orchv1.ControlMessage],
) error {
	first, err := stream.Receive()
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	reg := first.GetRegister()
	if reg == nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("first message must be a registration"))
	}

	nodeID, activeLeases, err := h.store.RegisterNode(ctx, store.NodeRegistration{
		NodeID:       reg.GetNodeId(),
		Hostname:     reg.GetHostname(),
		Capabilities: fromProtoCapabilities(reg.GetCapabilities()),
		Simulated:    reg.GetSimulated(),
		JoinToken:    reg.GetJoinToken(),
	})
	if err != nil {
		return connect.NewError(connect.CodePermissionDenied, err)
	}

	c := &conn{
		nodeID:    nodeID,
		hostname:  reg.GetHostname(),
		simulated: reg.GetSimulated(),
		send:      make(chan *orchv1.ControlMessage, 64),
		closed:    make(chan struct{}),
	}

	h.mu.Lock()
	if old := h.conns[nodeID]; old != nil {
		// A reconnect while the old stream is still half-open. The new one
		// wins; the old is closed rather than left to double-deliver.
		old.close()
	}
	h.conns[nodeID] = c
	h.mu.Unlock()

	defer h.disconnect(c)

	err = stream.Send(&orchv1.ControlMessage{Msg: &orchv1.ControlMessage_RegisterAck{
		RegisterAck: &orchv1.RegisterAck{
			NodeId:              nodeID,
			HeartbeatIntervalMs: uint32(h.heartbeatInterval.Milliseconds()),
			ActiveLeaseIds:      activeLeases,
		},
	}})
	if err != nil {
		return err
	}

	h.log.Info("node registered",
		"node_id", nodeID, "hostname", reg.GetHostname(),
		"simulated", reg.GetSimulated(), "devices", len(reg.GetCapabilities().GetDevices()))
	h.bus.Emit(events.KindNodeChanged,
		fmt.Sprintf("%s joined (%d devices%s)", reg.GetHostname(),
			len(reg.GetCapabilities().GetDevices()), simSuffix(reg.GetSimulated())),
		map[string]any{"node_id": nodeID, "state": "ready"})

	// One goroutine owns Send. Everything else queues onto c.send, because a
	// stream's Send is not safe for concurrent use.
	errc := make(chan error, 1)
	go func() { errc <- h.pump(ctx, c, stream) }()

	for {
		msg, err := stream.Receive()
		if err != nil {
			select {
			case perr := <-errc:
				if perr != nil {
					return perr
				}
			default:
			}
			return nil
		}

		switch {
		case msg.GetHeartbeat() != nil:
			h.onHeartbeat(ctx, c, msg.GetHeartbeat())
		case msg.GetTaskStatus() != nil:
			h.onTaskStatus(ctx, c, msg.GetTaskStatus())
		}
	}
}

func (h *Hub) pump(ctx context.Context, c *conn, stream *connect.BidiStream[orchv1.AgentMessage, orchv1.ControlMessage]) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.closed:
			return nil
		case msg := <-c.send:
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

func (h *Hub) onHeartbeat(ctx context.Context, c *conn, hb *orchv1.Heartbeat) {
	at := hb.GetSentAt().AsTime()
	if at.IsZero() {
		at = time.Now()
	}

	err := h.store.Heartbeat(ctx, store.HeartbeatUpdate{
		NodeID:        c.nodeID,
		Samples:       fromProtoSamples(hb.GetSamples()),
		CachedDigests: hb.GetCachedDigests(),
		LoadAvg1m:     hb.GetLoadAvg_1M(),
		FreeScratchGB: int(hb.GetFreeScratchGb()),
		At:            at,
	})
	if err != nil {
		h.log.Warn("heartbeat write failed", "node", c.nodeID, "err", err)
		return
	}

	renewed, rejected, err := h.store.RenewLeases(ctx, c.nodeID, hb.GetRenewLeaseIds(), at)
	if err != nil {
		h.log.Warn("lease renewal failed", "node", c.nodeID, "err", err)
		return
	}
	if len(renewed) > 0 || len(rejected) > 0 {
		h.trySend(c, &orchv1.ControlMessage{Msg: &orchv1.ControlMessage_RenewAck{
			RenewAck: &orchv1.LeaseRenewAck{Renewed: renewed, Rejected: rejected},
		}})
	}
}

func (h *Hub) onTaskStatus(ctx context.Context, c *conn, st *orchv1.TaskStatusUpdate) {
	at := st.GetAt().AsTime()
	if at.IsZero() {
		at = time.Now()
	}

	err := h.store.ApplyTaskStatus(ctx, store.TaskStatus{
		LeaseID:      st.GetLeaseId(),
		TaskID:       st.GetTaskId(),
		State:        fromProtoTaskState(st.GetState()),
		Message:      st.GetMessage(),
		ExitCode:     int(st.GetExitCode()),
		OutputPrefix: st.GetOutputPrefix(),
		At:           at,
	})
	if errors.Is(err, store.ErrStaleLease) {
		// Exactly what the fencing token is for: a worker from a preempted
		// attempt still reporting. Its writes went to its own prefix and are
		// ignored, so there is nothing to clean up.
		h.log.Debug("rejected status from a stale lease",
			"lease", st.GetLeaseId(), "task", st.GetTaskId())
		return
	}
	if err != nil {
		h.log.Warn("task status write failed", "task", st.GetTaskId(), "err", err)
		return
	}

	h.bus.Emit(events.KindTaskChanged,
		fmt.Sprintf("task %s %s on %s", shortID(st.GetTaskId()),
			fromProtoTaskState(st.GetState()), c.hostname),
		map[string]any{
			"task_id": st.GetTaskId(),
			"state":   string(fromProtoTaskState(st.GetState())),
			"node_id": c.nodeID,
			"message": st.GetMessage(),
		})
}

// disconnect releases everything a departed node was holding.
//
// Waiting for the heartbeat timeout would work, but a clean disconnect is
// information: the slots can be freed and the tasks requeued now.
func (h *Hub) disconnect(c *conn) {
	c.close()

	h.mu.Lock()
	if h.conns[c.nodeID] == c {
		delete(h.conns, c.nodeID)
	}
	h.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := h.store.SetNodeState(ctx, c.nodeID, domain.NodeOffline); err != nil {
		h.log.Warn("marking node offline failed", "node", c.nodeID, "err", err)
	}
	released, err := h.store.ReleaseLeasesOnNode(ctx, c.nodeID, "agent disconnected", true)
	if err != nil {
		h.log.Warn("releasing leases failed", "node", c.nodeID, "err", err)
	}

	h.log.Info("node disconnected", "node_id", c.nodeID, "leases_requeued", len(released))
	h.bus.Emit(events.KindNodeChanged,
		fmt.Sprintf("%s disconnected, %d lease(s) requeued", c.hostname, len(released)),
		map[string]any{"node_id": c.nodeID, "state": "offline"})
}

func (h *Hub) trySend(c *conn, msg *orchv1.ControlMessage) bool {
	select {
	case c.send <- msg:
		return true
	case <-c.closed:
		return false
	default:
		// The agent is not draining its queue. Dropping is safe: an undelivered
		// assignment lapses with its lease and the task requeues.
		h.log.Warn("dropping message, agent queue full", "node", c.nodeID)
		return false
	}
}

func (h *Hub) lookup(nodeID string) *conn {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.conns[nodeID]
}

// ---------------------------------------------------------------------------
// scheduler.Dispatcher
// ---------------------------------------------------------------------------

// Assign delivers a granted lease to the node that will run it.
func (h *Hub) Assign(nodeID string, p store.Placement, task domain.Task, job domain.Job) error {
	c := h.lookup(nodeID)
	if c == nil {
		return fmt.Errorf("node %s is not connected", nodeID)
	}

	node, err := h.store.GetNode(context.Background(), nodeID)
	if err != nil {
		return err
	}
	indices := make([]uint32, 0, len(p.SlotIDs))
	for _, id := range p.SlotIDs {
		for _, s := range node.Slots {
			if s.SlotID == id {
				indices = append(indices, uint32(s.DeviceIndex))
			}
		}
	}

	as := &orchv1.Assignment{
		LeaseId:       p.LeaseID,
		DeviceIndices: indices,
		Mode:          toProtoLeaseMode(p.Mode),
		HostShare: &orchv1.HostShare{
			Cores: p.HostShareCores,
			RamGb: uint32(p.HostShareRAMGB),
		},
		TtlMs:         uint64(p.TTL.Milliseconds()),
		Borrowed:      p.Borrowed,
		ExclusiveHost: p.ExclusiveHost,
		Task: &orchv1.TaskSpec{
			TaskId:       task.TaskID,
			JobId:        job.JobID,
			Index:        uint32(task.Index),
			ImageDigest:  job.ImageDigest,
			Command:      job.Command,
			Env:          job.Env,
			Assets:       toProtoAssets(job.Assets),
			OutputPrefix: p.OutputPrefix,
			Attempt:      uint32(p.Attempt),
		},
	}

	if !h.trySend(c, &orchv1.ControlMessage{Msg: &orchv1.ControlMessage_Assign{Assign: as}}) {
		return fmt.Errorf("could not deliver assignment to %s", nodeID)
	}
	return nil
}

// Preempt tells a node to stop running a lease.
func (h *Hub) Preempt(nodeID, leaseID, reason string, grace time.Duration) error {
	c := h.lookup(nodeID)
	if c == nil {
		// The node is gone, which achieves the same thing.
		return nil
	}
	h.trySend(c, &orchv1.ControlMessage{Msg: &orchv1.ControlMessage_Preempt{
		Preempt: &orchv1.Preempt{
			LeaseId: leaseID,
			GraceMs: uint64(grace.Milliseconds()),
			Reason:  reason,
		},
	}})
	return nil
}

// Inject forwards a scripted condition to a node for the trigger console.
func (h *Hub) Inject(nodeID, kind string, deviceIndex, value int) error {
	c := h.lookup(nodeID)
	if c == nil {
		return fmt.Errorf("node %s is not connected", nodeID)
	}
	h.trySend(c, &orchv1.ControlMessage{Msg: &orchv1.ControlMessage_Inject{
		Inject: &orchv1.Inject{
			Kind:        kind,
			DeviceIndex: uint32(deviceIndex),
			Value:       int32(value),
		},
	}})
	return nil
}

// Drain asks a node to stop everything it is running.
func (h *Hub) Drain(nodeID, reason string, grace time.Duration) error {
	c := h.lookup(nodeID)
	if c == nil {
		return fmt.Errorf("node %s is not connected", nodeID)
	}
	h.trySend(c, &orchv1.ControlMessage{Msg: &orchv1.ControlMessage_Drain{
		Drain: &orchv1.Drain{Reason: reason, GraceMs: uint64(grace.Milliseconds())},
	}})
	return nil
}

// ConnectedNodes returns the node IDs with a live stream.
func (h *Hub) ConnectedNodes() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(h.conns))
	for id := range h.conns {
		out = append(out, id)
	}
	return out
}

func simSuffix(simulated bool) string {
	if simulated {
		return ", simulated"
	}
	return ""
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
