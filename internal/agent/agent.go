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
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// Reconciler is implemented by executors that can report which leases they
// believe are still running, independent of the agent process's own
// (restart-volatile) bookkeeping. Only ContainerdExecutor needs this:
// containerd is a separate daemon that outlives a crashed agent process, so
// without it a crashed agent leaks GPU containers -- and the VRAM they hold
// -- until the machine reboots. The simulator has no state that survives a
// process restart in the first place, so it does not implement this.
type Reconciler interface {
	RunningLeases(ctx context.Context) (map[string]Handle, error)
}

// stopWatchdogSlack bounds how long the poll loop waits for a stopped task to
// actually reach a terminal state, on top of whatever grace period Stop was
// given, before forcing Cleanup anyway. Without this bound a task whose
// container never reports back (a stuck runtime, a wedged driver) would pin
// its runningTask entry, and the goroutine polling it, forever.
const stopWatchdogSlack = 30 * time.Second

// watchdogRestBetweenTicks is used to disable the watchdog timer when a task
// has not been asked to stop. A timer with a very long duration rather than a
// nil *time.Timer keeps the select loop uniform.
const watchdogRestBetweenTicks = 365 * 24 * time.Hour

// Config configures a node agent.
type Config struct {
	ServerURL string
	JoinToken string
	Hostname  string
	Simulated bool
	CacheDir  string
	// WorkDir holds per-lease scratch state: the writable output directory a
	// task's container mounts, and its log file. Unlike CacheDir this is
	// wiped per attempt (agent.go's startTask removes it once the attempt's
	// outputs are uploaded), so nothing here is precious across restarts.
	WorkDir string
	Labels  map[string]string

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

	// objectStore is the default (long-lived-credential) upload client, used
	// when a lease's assignment carries no per-lease STS credentials -- e.g.
	// a small deployment that has not wired up STS at all. Nil means no
	// object store is configured, in which case the agent uploads nothing
	// and behaves exactly as it did before this existed.
	objectStore *ObjectStore

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
	// cancel tears down runCtx, the context passed to exec.Start and the
	// cache stage. It is invoked only when startTask itself returns (session
	// teardown), never by stopTask -- stopTask used to call this directly,
	// which cancelled the very context the poll loop was selecting on and so
	// made it exit before it could ever observe a terminal Status, leaking a
	// container (and, on real hardware, a snapshot and the VRAM it held) on
	// every preemption.
	cancel context.CancelFunc

	// stopped is closed by stopTask to signal "a Stop has been sent, start
	// the watchdog". stopOnce guards against a second stopTask call (e.g.
	// preemption followed by a rejected lease renewal) closing an
	// already-closed channel.
	stopOnce sync.Once
	stopped  chan struct{}
	// grace is read by the poll loop after stopped fires, to size the
	// watchdog. Written under Agent.mu, alongside every other runningTask
	// field.
	grace time.Duration

	// timedOut distinguishes "killed because the task exceeded its
	// deadline" (which must be reported as a FAILED task) from ordinary
	// preemption (which must not be reported at all -- the lease has moved
	// on and a report would just be rejected as stale).
	timedOut atomic.Bool
}

// New creates an agent from a set of seam implementations. objectStore may be
// nil, meaning no object store is configured; see Config.WorkDir and the
// ObjectStore doc comment.
func New(cfg Config, log *slog.Logger, devices DeviceProvider, health HealthSource, exec Executor, probe HostProbe, objectStore *ObjectStore) (*Agent, error) {
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
	if cfg.WorkDir == "" {
		cfg.WorkDir = filepathJoin(os.TempDir(), "orch-work-"+cfg.Hostname)
	}

	cache, err := NewCache(cfg.CacheDir)
	if err != nil {
		return nil, err
	}

	a := &Agent{
		cfg:         cfg,
		log:         log,
		devices:     devices,
		health:      health,
		exec:        exec,
		probe:       probe,
		cache:       cache,
		objectStore: objectStore,
		running:     map[string]*runningTask{},
		client:      newAgentClient(cfg.ServerURL),
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
	valid := make(map[string]bool, len(ack.GetActiveLeaseIds()))
	for _, id := range ack.GetActiveLeaseIds() {
		valid[id] = true
	}
	a.reconcileRunning(ctx, valid)
	a.reconcileOrphans(ctx, valid)

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

// startTask stages assets, runs the container, uploads its outputs, and
// reports each transition.
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
	rt := &runningTask{leaseID: leaseID, taskID: task.GetTaskId(), cancel: cancel, stopped: make(chan struct{})}
	a.mu.Lock()
	a.running[leaseID] = rt
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

	// A per-attempt writable directory, mode 0777: images that run as a
	// non-root USER cannot write into a root-owned bind mount, and that
	// failure looks like "the container produced nothing" rather than a
	// permissions error anywhere obvious.
	leaseDir := filepath.Join(a.cfg.WorkDir, leaseID)
	outDir := filepath.Join(leaseDir, "out")
	if err := os.MkdirAll(outDir, 0o777); err != nil {
		report(orchv1.TaskState_TASK_STATE_FAILED, "create output dir: "+err.Error(), 1)
		return
	}
	_ = os.Chmod(outDir, 0o777) // MkdirAll honours umask; force it open regardless.
	mounts = append(mounts, Mount{HostPath: outDir, ContainerPath: ContainerOutputDir, ReadOnly: false})
	defer os.RemoveAll(leaseDir)

	indices := make([]int, 0, len(as.GetDeviceIndices()))
	for _, i := range as.GetDeviceIndices() {
		indices = append(indices, int(i))
	}

	env, collisions := buildEnv(task.GetEnv(), task.GetParams(), map[string]string{
		"ORCH_JOB_ID":        task.GetJobId(),
		"ORCH_TASK_ID":       task.GetTaskId(),
		"ORCH_TASK_INDEX":    strconv.Itoa(int(task.GetIndex())),
		"ORCH_ATTEMPT":       strconv.Itoa(int(task.GetAttempt())),
		"ORCH_LEASE_ID":      leaseID,
		"ORCH_OUTPUT_PREFIX": task.GetOutputPrefix(),
		"ORCH_OUTPUT_DIR":    ContainerOutputDir,
	})
	for _, k := range collisions {
		a.log.Warn("task param collides with job env, param wins", "lease", leaseID, "key", k)
	}

	spec := StartSpec{
		LeaseID:        leaseID,
		TaskID:         task.GetTaskId(),
		JobID:          task.GetJobId(),
		Index:          int(task.GetIndex()),
		Attempt:        int(task.GetAttempt()),
		Image:          task.GetImageDigest(),
		Command:        expandArgv(task.GetCommand(), env),
		Env:            env,
		DeviceIndices:  indices,
		HostShareCores: as.GetHostShare().GetCores(),
		HostShareRAMGB: int(as.GetHostShare().GetRamGb()),
		Mounts:         mounts,
		OutputPrefix:   task.GetOutputPrefix(),
		Params:         task.GetParams(),
		LogPath:        filepath.Join(leaseDir, "task.log"),
		Timeout:        time.Duration(task.GetTimeoutMs()) * time.Millisecond,
	}

	objStore := a.resolveObjectStore(leaseID, as.GetCreds())

	startedAt := time.Now()
	handle, err := a.exec.Start(runCtx, spec)
	if err != nil {
		report(orchv1.TaskState_TASK_STATE_FAILED, "start failed: "+err.Error(), 1)
		return
	}

	a.mu.Lock()
	rt.handle = handle
	a.mu.Unlock()

	report(orchv1.TaskState_TASK_STATE_RUNNING, "", 0)

	if spec.Timeout > 0 {
		timer := time.AfterFunc(spec.Timeout, func() {
			rt.timedOut.Store(true)
			a.log.Warn("task exceeded timeout", "lease", leaseID, "timeout", spec.Timeout)
			a.stopTask(context.Background(), leaseID, 10*time.Second)
		})
		defer timer.Stop()
	}

	poll := time.NewTicker(500 * time.Millisecond)
	defer poll.Stop()
	watchdog := time.NewTimer(watchdogRestBetweenTicks)
	defer watchdog.Stop()

	stoppedCh := rt.stopped
	for {
		select {
		case <-ctx.Done():
			// The session itself is ending (agent shutting down or
			// reconnecting). Whatever this task's real state is, the next
			// session's reconcileOrphans will find it via the executor, not
			// via this goroutine.
			return

		case <-stoppedCh:
			// Fires exactly once: after this, block forever on a nil channel
			// so the case never wins a future select (a closed channel would
			// otherwise be perpetually ready and starve the other cases).
			stoppedCh = nil
			a.mu.Lock()
			grace := rt.grace
			a.mu.Unlock()
			watchdog.Reset(grace + stopWatchdogSlack)

		case <-watchdog.C:
			a.log.Warn("watchdog expired waiting for a stopped task to exit", "lease", leaseID)
			_ = a.exec.Cleanup(context.Background(), handle)
			if rt.timedOut.Load() {
				report(orchv1.TaskState_TASK_STATE_FAILED, "exceeded timeout", 0)
			}
			return

		case <-poll.C:
			st, err := a.exec.Status(ctx, handle)
			if err != nil {
				report(orchv1.TaskState_TASK_STATE_FAILED, "status: "+err.Error(), 1)
				_ = a.exec.Cleanup(context.Background(), handle)
				return
			}
			switch st.State {
			case ExecExited, ExecFailed:
				a.finishTask(context.Background(), report, objStore, spec, outDir, startedAt, st)
				_ = a.exec.Cleanup(context.Background(), handle)
				return
			case ExecKilled:
				_ = a.exec.Cleanup(context.Background(), handle)
				if rt.timedOut.Load() {
					report(orchv1.TaskState_TASK_STATE_FAILED, "exceeded timeout", int32(st.ExitCode))
				}
				// Otherwise: preemption already tore this down, and the lease
				// is gone. Saying anything now would be rejected at the
				// control plane as a stale holder, which is exactly the
				// intended behaviour.
				return
			}
		}
	}
}

// finishTask uploads a finished task's outputs (if an object store is
// configured) and reports the terminal state. "done" means the bytes are
// durable, not that the process exited: an upload failure is always reported
// as FAILED, never DONE, and the exit-status FAILED case still tries to
// upload whatever partial output and logs exist.
func (a *Agent) finishTask(
	ctx context.Context, report func(orchv1.TaskState, string, int32),
	objStore *ObjectStore, spec StartSpec, outDir string, startedAt time.Time, st Status,
) {
	finalState := orchv1.TaskState_TASK_STATE_DONE
	if st.State == ExecFailed {
		finalState = orchv1.TaskState_TASK_STATE_FAILED
	}

	if objStore != nil {
		// Keeps the lease renewing via sendLoop (which renews everything
		// still in a.running -- the defer that removes the entry only fires
		// once startTask returns) while the upload, which can take a while
		// for large outputs, is in flight.
		report(orchv1.TaskState_TASK_STATE_RUNNING, "uploading output", 0)
		if err := a.uploadOutputs(ctx, objStore, spec.OutputPrefix, outDir, spec.LogPath, startedAt, st); err != nil {
			a.log.Warn("output upload failed", "lease", spec.LeaseID, "err", err)
			report(orchv1.TaskState_TASK_STATE_FAILED, "upload failed: "+err.Error(), int32(st.ExitCode))
			return
		}
	}

	report(finalState, st.Message, int32(st.ExitCode))
}

// resolveObjectStore picks the client this task's uploads go through: a
// per-lease client built from the control plane's STS credentials when
// present, or the agent's own default (static-credential) client otherwise.
// Either may be nil, meaning no upload happens.
func (a *Agent) resolveObjectStore(leaseID string, creds *orchv1.ObjectCredentials) *ObjectStore {
	if creds == nil || creds.GetAccessKeyId() == "" {
		return a.objectStore
	}
	s, err := NewObjectStoreFromSTS(STSCredentials{
		Endpoint:        creds.GetEndpoint(),
		Bucket:          creds.GetBucket(),
		AccessKeyID:     creds.GetAccessKeyId(),
		SecretAccessKey: creds.GetSecretAccessKey(),
		SessionToken:    creds.GetSessionToken(),
		UseSSL:          creds.GetUseSsl(),
	})
	if err != nil {
		a.log.Warn("could not build per-lease object store client, falling back to default",
			"lease", leaseID, "err", err)
		return a.objectStore
	}
	return s
}

func (a *Agent) stopTask(ctx context.Context, leaseID string, grace time.Duration) {
	a.mu.Lock()
	rt := a.running[leaseID]
	if rt != nil {
		rt.grace = grace
	}
	a.mu.Unlock()

	if rt == nil {
		return
	}
	if rt.handle != "" {
		if err := a.exec.Stop(ctx, rt.handle, grace); err != nil {
			a.log.Warn("stop failed", "lease", leaseID, "err", err)
		}
	}
	rt.stopOnce.Do(func() { close(rt.stopped) })
}

// reconcileRunning stops anything running that the control plane did not
// acknowledge, after a reconnect.
func (a *Agent) reconcileRunning(ctx context.Context, valid map[string]bool) {
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

// reconcileOrphans asks the executor (if it supports Reconciler) which
// leases it believes are still running -- independent of this agent
// process's own bookkeeping, which is empty just after a restart -- and
// kills anything the control plane does not recognise. Containerd is a
// separate daemon that outlives a crashed agent process; without this, a
// crashed agent leaks GPU containers, and the VRAM they hold, until the
// machine reboots.
func (a *Agent) reconcileOrphans(ctx context.Context, valid map[string]bool) {
	rec, ok := a.exec.(Reconciler)
	if !ok {
		return
	}
	running, err := rec.RunningLeases(ctx)
	if err != nil {
		a.log.Warn("listing executor's running leases failed", "err", err)
		return
	}
	for leaseID, h := range running {
		a.mu.Lock()
		_, tracked := a.running[leaseID]
		a.mu.Unlock()
		if tracked || valid[leaseID] {
			// Either this session's own startTask is already watching it, or
			// the control plane still recognises the lease -- in the latter
			// case it is not orphaned, just running without a local
			// runningTask entry to poll it (a smaller gap than leaking it
			// forever, and resolved the moment the lease ends or is
			// preempted through the normal path).
			continue
		}
		a.log.Warn("killing orphaned container from a previous agent process", "lease", leaseID)
		_ = a.exec.Stop(ctx, h, 0)
		_ = a.exec.Cleanup(ctx, h)
	}
}

// buildEnv merges job-level env, task params projected as
// ORCH_PARAM_<UPPERCASE_KEY>, and identity vars into the environment a task
// container sees. Job env applies first so params can never be silently
// shadowed; on a key collision the param wins, and the key is returned so
// the caller can log it.
func buildEnv(jobEnv, params, identity map[string]string) (env map[string]string, collisions []string) {
	env = make(map[string]string, len(jobEnv)+len(params)+len(identity))
	for k, v := range jobEnv {
		env[k] = v
	}

	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		envKey := "ORCH_PARAM_" + strings.ToUpper(k)
		if _, exists := env[envKey]; exists {
			collisions = append(collisions, envKey)
		}
		env[envKey] = params[k]
	}

	for k, v := range identity {
		env[k] = v
	}
	return env, collisions
}

// expandArgv expands ${ORCH_PARAM_X}-style references in each command
// argument against env, so a stock image (ffmpeg, Blender) can take
// arguments like "--frame ${ORCH_PARAM_FRAME}" on argv without a wrapper
// entrypoint.
//
// This happens here, in agent.go, rather than in the executor: putting it in
// containerd.go would make real and simulated nodes diverge, which this
// project does not allow (see the package doc).
func expandArgv(command []string, env map[string]string) []string {
	out := make([]string, len(command))
	for i, c := range command {
		out[i] = os.Expand(c, func(key string) string { return env[key] })
	}
	return out
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
