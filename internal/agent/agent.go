package agent

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/types/known/timestamppb"

	orchv1 "github.com/alexk/orch/gen/orch/v1"
	"github.com/alexk/orch/gen/orch/v1/orchv1connect"
	"github.com/alexk/orch/internal/domain"
)

// Injector is implemented by simulated providers that accept scripted faults.
// Real backends do not implement it, and the agent simply has nothing to
// forward to.
type Injector interface {
	InjectThrottle(deviceIndex int, on bool)
	InjectXID(deviceIndex, code int)
	ClearConditions()
}

// Config configures a node agent.
type Config struct {
	ServerURL string
	JoinToken string
	Hostname  string
	Simulated bool
	CacheDir  string
	Labels    map[string]string

	HeartbeatInterval time.Duration
	ReconnectBackoff  time.Duration
}

// Agent is the node-side runtime.
//
// It holds one bidirectional stream to the control plane: heartbeat up,
// assignments down. Everything the control plane knows about this machine came
// from here, because every fact a human types is a fact that goes stale.
type Agent struct {
	cfg Config
	log *slog.Logger

	devices DeviceProvider
	health  HealthSource
	exec    Executor
	probe   HostProbe
	cache   *Cache

	// Set only when the providers are simulated.
	injector Injector

	client orchv1connect.AgentServiceClient

	mu      sync.Mutex
	nodeID  string
	running map[string]*runningTask
	samples []domain.HealthSample
}

type runningTask struct {
	leaseID string
	taskID  string
	handle  Handle
	cancel  context.CancelFunc
}

// New creates an agent from a set of seam implementations.
func New(cfg Config, log *slog.Logger, devices DeviceProvider, health HealthSource, exec Executor, probe HostProbe) (*Agent, error) {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 2 * time.Second
	}
	if cfg.ReconnectBackoff <= 0 {
		cfg.ReconnectBackoff = 2 * time.Second
	}
	if cfg.Hostname == "" {
		cfg.Hostname, _ = os.Hostname()
	}
	if cfg.CacheDir == "" {
		cfg.CacheDir = filepathJoin(os.TempDir(), "orch-cache-"+cfg.Hostname)
	}

	cache, err := NewCache(cfg.CacheDir)
	if err != nil {
		return nil, err
	}

	a := &Agent{
		cfg:     cfg,
		log:     log,
		devices: devices,
		health:  health,
		exec:    exec,
		probe:   probe,
		cache:   cache,
		running: map[string]*runningTask{},
		client:  newAgentClient(cfg.ServerURL),
	}
	if inj, ok := devices.(Injector); ok {
		a.injector = inj
	}
	return a, nil
}

// newAgentClient builds a gRPC-over-HTTP/2 client.
//
// Bidirectional streaming needs HTTP/2, and the fleet is a trusted
// single-organization network, so h2c (cleartext HTTP/2) is what this dials.
func newAgentClient(serverURL string) orchv1connect.AgentServiceClient {
	httpClient := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		},
	}
	return orchv1connect.NewAgentServiceClient(httpClient, serverURL, connect.WithGRPC())
}

// Cache exposes the content-addressed cache, for pre-warming.
func (a *Agent) Cache() *Cache { return a.cache }

// Run maintains the connection until the context is cancelled.
//
// A dropped connection is ordinary. Users reboot, sleep and unplug
// workstations, so the agent reconnects rather than treating a disconnect as an
// error worth escalating.
func (a *Agent) Run(ctx context.Context) error {
	for {
		if err := a.session(ctx); err != nil && !errors.Is(err, context.Canceled) {
			a.log.Warn("session ended, reconnecting",
				"err", err, "backoff", a.cfg.ReconnectBackoff)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(a.cfg.ReconnectBackoff):
		}
	}
}

// session runs one connection: register, then heartbeat up and act on what
// comes down.
func (a *Agent) session(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream := a.client.Connect(ctx)

	caps, err := a.capabilityDoc(ctx)
	if err != nil {
		return fmt.Errorf("build capability document: %w", err)
	}

	a.mu.Lock()
	nodeID := a.nodeID
	a.mu.Unlock()

	err = stream.Send(&orchv1.AgentMessage{Msg: &orchv1.AgentMessage_Register{
		Register: &orchv1.Register{
			JoinToken:    a.cfg.JoinToken,
			Hostname:     a.cfg.Hostname,
			Capabilities: caps,
			Simulated:    a.cfg.Simulated,
			NodeId:       nodeID,
		},
	}})
	if err != nil {
		return fmt.Errorf("send register: %w", err)
	}

	msg, err := stream.Receive()
	if err != nil {
		return fmt.Errorf("await register ack: %w", err)
	}
	ack := msg.GetRegisterAck()
	if ack == nil {
		return errors.New("expected a register ack")
	}

	a.mu.Lock()
	a.nodeID = ack.GetNodeId()
	a.mu.Unlock()

	a.log.Info("registered with control plane",
		"node_id", ack.GetNodeId(), "simulated", a.cfg.Simulated,
		"devices", len(caps.GetDevices()))

	// Anything running that the control plane does not list is a survivor of a
	// previous session whose lease is gone. It is stopped rather than adopted:
	// the lease ID is the fencing token and this one is no longer valid.
	a.reconcileRunning(ctx, ack.GetActiveLeaseIds())

	go a.consumeHealth(ctx)

	errc := make(chan error, 2)
	go func() { errc <- a.sendLoop(ctx, stream) }()
	go func() { errc <- a.recvLoop(ctx, stream) }()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *Agent) capabilityDoc(ctx context.Context) (*orchv1.CapabilityDoc, error) {
	devices, err := a.devices.Enumerate(ctx)
	if err != nil {
		return nil, err
	}
	host, software := a.probe()

	doc := &orchv1.CapabilityDoc{
		Host: &orchv1.HostSpec{
			Cores:     uint32(host.Cores),
			RamGb:     uint32(host.RAMGB),
			ScratchGb: uint32(host.ScratchGB),
			PcieGen:   uint32(host.PCIeGen),
		},
		Software: &orchv1.SoftwareSpec{
			Driver:  software.Driver,
			Cuda:    software.CUDA,
			Runtime: software.Runtime,
		},
		Labels: a.cfg.Labels,
	}
	for _, d := range devices {
		doc.Devices = append(doc.Devices, &orchv1.Device{
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
	return doc, nil
}

func (a *Agent) consumeHealth(ctx context.Context) {
	ch, err := a.health.Stream(ctx)
	if err != nil {
		a.log.Error("health stream failed", "err", err)
		return
	}
	for batch := range ch {
		a.mu.Lock()
		a.samples = batch
		a.mu.Unlock()
	}
}

// sendLoop pushes a heartbeat on a fixed interval.
//
// The heartbeat carries the fast-path health struct, the running set, the lease
// renewals, and cache residency -- everything the scheduler reads, in one
// message, sub-second and in memory.
func (a *Agent) sendLoop(ctx context.Context, stream *connect.BidiStreamForClient[orchv1.AgentMessage, orchv1.ControlMessage]) error {
	t := time.NewTicker(a.cfg.HeartbeatInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			a.mu.Lock()
			samples := a.samples
			leaseIDs := make([]string, 0, len(a.running))
			running := make([]*orchv1.RunningTask, 0, len(a.running))
			for id, rt := range a.running {
				leaseIDs = append(leaseIDs, id)
				running = append(running, &orchv1.RunningTask{
					LeaseId: id, TaskId: rt.taskID, State: "running",
				})
			}
			a.mu.Unlock()

			hb := &orchv1.Heartbeat{
				SentAt:        timestamppb.Now(),
				Samples:       toProtoSamples(samples),
				Running:       running,
				RenewLeaseIds: leaseIDs,
				CachedDigests: a.cache.Digests(),
			}
			if err := stream.Send(&orchv1.AgentMessage{
				Msg: &orchv1.AgentMessage_Heartbeat{Heartbeat: hb},
			}); err != nil {
				return fmt.Errorf("heartbeat: %w", err)
			}
		}
	}
}

// recvLoop acts on what the control plane sends down.
func (a *Agent) recvLoop(ctx context.Context, stream *connect.BidiStreamForClient[orchv1.AgentMessage, orchv1.ControlMessage]) error {
	for {
		msg, err := stream.Receive()
		if err != nil {
			return fmt.Errorf("receive: %w", err)
		}

		switch {
		case msg.GetAssign() != nil:
			go a.startTask(ctx, stream, msg.GetAssign())

		case msg.GetPreempt() != nil:
			p := msg.GetPreempt()
			a.log.Info("preempted", "lease", p.GetLeaseId(), "reason", p.GetReason())
			a.stopTask(ctx, p.GetLeaseId(), time.Duration(p.GetGraceMs())*time.Millisecond)

		case msg.GetRenewAck() != nil:
			// A rejected renewal means the control plane no longer recognises
			// this lease. Whatever is running under it is a zombie and is
			// stopped immediately.
			for _, id := range msg.GetRenewAck().GetRejected() {
				a.log.Warn("lease rejected, stopping task", "lease", id)
				a.stopTask(ctx, id, 0)
			}

		case msg.GetDrain() != nil:
			d := msg.GetDrain()
			a.log.Info("drain requested", "reason", d.GetReason())
			a.mu.Lock()
			ids := make([]string, 0, len(a.running))
			for id := range a.running {
				ids = append(ids, id)
			}
			a.mu.Unlock()
			for _, id := range ids {
				a.stopTask(ctx, id, time.Duration(d.GetGraceMs())*time.Millisecond)
			}

		case msg.GetInject() != nil:
			a.applyInjection(msg.GetInject())
		}
	}
}

func (a *Agent) applyInjection(in *orchv1.Inject) {
	if a.injector == nil {
		// A real node cannot be told its temperature. Ignoring this is the
		// correct behaviour, not a gap.
		a.log.Debug("ignoring injection on a non-simulated node", "kind", in.GetKind())
		return
	}
	switch in.GetKind() {
	case "thermal_throttle":
		a.injector.InjectThrottle(int(in.GetDeviceIndex()), in.GetValue() != 0)
	case "xid_error":
		a.injector.InjectXID(int(in.GetDeviceIndex()), int(in.GetValue()))
	case "clear":
		a.injector.ClearConditions()
	}
}

// startTask stages assets and runs the container, reporting each transition.
func (a *Agent) startTask(ctx context.Context, stream *connect.BidiStreamForClient[orchv1.AgentMessage, orchv1.ControlMessage], as *orchv1.Assignment) {
	leaseID := as.GetLeaseId()
	task := as.GetTask()

	report := func(state orchv1.TaskState, msg string, exit int32) {
		err := stream.Send(&orchv1.AgentMessage{Msg: &orchv1.AgentMessage_TaskStatus{
			TaskStatus: &orchv1.TaskStatusUpdate{
				LeaseId:      leaseID,
				TaskId:       task.GetTaskId(),
				State:        state,
				Message:      msg,
				ExitCode:     exit,
				OutputPrefix: task.GetOutputPrefix(),
				At:           timestamppb.Now(),
			},
		}})
		if err != nil {
			a.log.Warn("status report failed", "lease", leaseID, "err", err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	a.mu.Lock()
	a.running[leaseID] = &runningTask{leaseID: leaseID, taskID: task.GetTaskId(), cancel: cancel}
	a.mu.Unlock()

	defer func() {
		cancel()
		a.mu.Lock()
		delete(a.running, leaseID)
		a.mu.Unlock()
	}()

	report(orchv1.TaskState_TASK_STATE_STAGING, "staging assets", 0)

	mounts, err := a.cache.Stage(runCtx, fromProtoAssets(task.GetAssets()))
	if err != nil {
		report(orchv1.TaskState_TASK_STATE_FAILED, "staging failed: "+err.Error(), 1)
		return
	}

	indices := make([]int, 0, len(as.GetDeviceIndices()))
	for _, i := range as.GetDeviceIndices() {
		indices = append(indices, int(i))
	}

	spec := StartSpec{
		LeaseID:        leaseID,
		TaskID:         task.GetTaskId(),
		JobID:          task.GetJobId(),
		Image:          task.GetImageDigest(),
		Command:        task.GetCommand(),
		Env:            task.GetEnv(),
		DeviceIndices:  indices,
		HostShareCores: as.GetHostShare().GetCores(),
		HostShareRAMGB: int(as.GetHostShare().GetRamGb()),
		Mounts:         mounts,
		OutputPrefix:   task.GetOutputPrefix(),
	}

	handle, err := a.exec.Start(runCtx, spec)
	if err != nil {
		report(orchv1.TaskState_TASK_STATE_FAILED, "start failed: "+err.Error(), 1)
		return
	}

	a.mu.Lock()
	if rt := a.running[leaseID]; rt != nil {
		rt.handle = handle
	}
	a.mu.Unlock()

	report(orchv1.TaskState_TASK_STATE_RUNNING, "", 0)

	poll := time.NewTicker(500 * time.Millisecond)
	defer poll.Stop()

	for {
		select {
		case <-runCtx.Done():
			// Preemption already tore this down, and the lease is gone. Saying
			// anything now would be rejected at the control plane as a stale
			// holder, which is exactly the intended behaviour.
			return
		case <-poll.C:
			st, err := a.exec.Status(runCtx, handle)
			if err != nil {
				report(orchv1.TaskState_TASK_STATE_FAILED, "status: "+err.Error(), 1)
				return
			}
			switch st.State {
			case ExecExited:
				report(orchv1.TaskState_TASK_STATE_DONE, st.Message, int32(st.ExitCode))
				return
			case ExecFailed:
				report(orchv1.TaskState_TASK_STATE_FAILED, st.Message, int32(st.ExitCode))
				return
			case ExecKilled:
				return
			}
		}
	}
}

func (a *Agent) stopTask(ctx context.Context, leaseID string, grace time.Duration) {
	a.mu.Lock()
	rt := a.running[leaseID]
	a.mu.Unlock()

	if rt == nil {
		return
	}
	if rt.handle != "" {
		if err := a.exec.Stop(ctx, rt.handle, grace); err != nil {
			a.log.Warn("stop failed", "lease", leaseID, "err", err)
		}
	}
	rt.cancel()
}

// reconcileRunning stops anything running that the control plane did not
// acknowledge, after a reconnect.
func (a *Agent) reconcileRunning(ctx context.Context, active []string) {
	valid := make(map[string]bool, len(active))
	for _, id := range active {
		valid[id] = true
	}

	a.mu.Lock()
	var stale []string
	for id := range a.running {
		if !valid[id] {
			stale = append(stale, id)
		}
	}
	a.mu.Unlock()

	for _, id := range stale {
		a.log.Warn("stopping task with an unrecognised lease", "lease", id)
		a.stopTask(ctx, id, 0)
	}
}

func toProtoSamples(in []domain.HealthSample) []*orchv1.HealthSample {
	out := make([]*orchv1.HealthSample, 0, len(in))
	for _, s := range in {
		xids := make([]uint32, 0, len(s.XIDErrors))
		for _, x := range s.XIDErrors {
			xids = append(xids, uint32(x))
		}
		out = append(out, &orchv1.HealthSample{
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

func filepathJoin(a, b string) string {
	if a == "" {
		return b
	}
	return a + string(os.PathSeparator) + b
}
