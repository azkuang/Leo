package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	orchv1 "github.com/alexk/orch/gen/orch/v1"
	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/events"
	"github.com/alexk/orch/internal/scheduler"
	"github.com/alexk/orch/internal/store"
)

// Reclaimer is the scheduler's machine-reclaim entry point. The API depends on
// the narrow capability rather than the whole scheduler.
type Reclaimer interface {
	RequestMachineReclaim(ctx context.Context, nodeID, reason string) ([]string, error)
}

// Server implements the client-facing API.
type Server struct {
	store     store.Store
	bus       *events.Bus
	hub       *Hub
	reclaimer Reclaimer
	log       *slog.Logger
}

// NewServer creates the API server.
func NewServer(st store.Store, bus *events.Bus, hub *Hub, r Reclaimer, log *slog.Logger) *Server {
	return &Server{store: st, bus: bus, hub: hub, reclaimer: r, log: log}
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

// SubmitJob accepts a job and fans it out into tasks.
//
// The fan-out is data the client supplies. A render job puts frame numbers in
// task params; this code does not know that, and must not learn it -- the
// moment the orchestrator knows what a frame is, it became a render farm.
func (s *Server) SubmitJob(
	ctx context.Context,
	req *connect.Request[orchv1.SubmitJobRequest],
) (*connect.Response[orchv1.SubmitJobResponse], error) {
	spec := req.Msg.GetSpec()
	if spec == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("spec is required"))
	}
	if spec.GetImageDigest() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("image_digest is required"))
	}

	poolID := spec.GetPoolId()
	if poolID == "" {
		poolID = domain.DefaultPoolID
	}

	job := domain.Job{
		JobID:       "job-" + uuid.NewString()[:8],
		Name:        spec.GetName(),
		Owner:       spec.GetOwner(),
		PoolID:      poolID,
		ImageDigest: spec.GetImageDigest(),
		Command:     spec.GetCommand(),
		Env:         spec.GetEnv(),
		Assets:      fromProtoAssets(spec.GetAssets()),
		Request:     fromProtoRequest(spec.GetRequest()),
		Mode:        fromProtoLeaseMode(spec.GetMode()),
		Preemptible: spec.GetPreemptible(),
		State:       domain.JobPending,
		SubmittedAt: time.Now(),
	}
	if job.Request.Slots < 1 {
		job.Request.Slots = 1
	}

	// A job with no explicit fan-out is one task. Requiring callers to state
	// that would make the simple case worse for no gain.
	params := spec.GetTasks()
	if len(params) == 0 {
		params = []*orchv1.TaskParams{{}}
	}

	tasks := make([]domain.Task, 0, len(params))
	ids := make([]string, 0, len(params))
	for i, p := range params {
		t := domain.Task{
			TaskID: "task-" + uuid.NewString()[:8],
			JobID:  job.JobID,
			Index:  i,
			Params: p.GetParams(),
			State:  domain.TaskQueued,
		}
		tasks = append(tasks, t)
		ids = append(ids, t.TaskID)
	}

	if err := s.store.CreateJob(ctx, job, tasks); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	s.bus.Emit(events.KindJobChanged,
		fmt.Sprintf("submitted %s: %d task(s) into pool %s", jobLabel(job), len(tasks), poolID),
		map[string]any{"job_id": job.JobID, "tasks": len(tasks), "pool_id": poolID})

	return connect.NewResponse(&orchv1.SubmitJobResponse{
		JobId: job.JobID, TaskIds: ids,
	}), nil
}

// ListJobs returns recent jobs with their task rollups.
func (s *Server) ListJobs(
	ctx context.Context,
	req *connect.Request[orchv1.ListJobsRequest],
) (*connect.Response[orchv1.ListJobsResponse], error) {
	jobs, err := s.store.ListJobs(ctx, req.Msg.GetOwner())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	out := &orchv1.ListJobsResponse{}
	for _, j := range jobs {
		tasks, err := s.store.ListTasks(ctx, j.JobID)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		out.Jobs = append(out.Jobs, toProtoJob(j, tasks))
	}
	return connect.NewResponse(out), nil
}

// GetJob returns a job and every task under it, which is what the frame grid
// renders.
func (s *Server) GetJob(
	ctx context.Context,
	req *connect.Request[orchv1.GetJobRequest],
) (*connect.Response[orchv1.GetJobResponse], error) {
	job, tasks, err := s.store.GetJob(ctx, req.Msg.GetJobId())
	if errors.Is(err, store.ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	out := &orchv1.GetJobResponse{Job: toProtoJob(job, tasks)}
	for _, t := range tasks {
		out.Tasks = append(out.Tasks, toProtoTask(t))
	}
	return connect.NewResponse(out), nil
}

// CancelJob stops a job and tears down whatever it is running.
func (s *Server) CancelJob(
	ctx context.Context,
	req *connect.Request[orchv1.CancelJobRequest],
) (*connect.Response[orchv1.CancelJobResponse], error) {
	leases, err := s.store.CancelJob(ctx, req.Msg.GetJobId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	active, err := s.store.ListActiveLeases(ctx)
	if err == nil {
		byID := map[string]domain.Lease{}
		for _, l := range active {
			byID[l.LeaseID] = l
		}
		for _, id := range leases {
			if l, ok := byID[id]; ok {
				_ = s.hub.Preempt(l.NodeID, id, "job cancelled", 0)
			}
		}
	}

	s.bus.Emit(events.KindJobChanged, "cancelled job "+req.Msg.GetJobId(),
		map[string]any{"job_id": req.Msg.GetJobId(), "state": "cancelled"})
	return connect.NewResponse(&orchv1.CancelJobResponse{}), nil
}

// ---------------------------------------------------------------------------
// Fleet
// ---------------------------------------------------------------------------

// ListNodes returns the fleet view.
func (s *Server) ListNodes(
	ctx context.Context,
	_ *connect.Request[orchv1.ListNodesRequest],
) (*connect.Response[orchv1.ListNodesResponse], error) {
	nodes, err := s.store.ListNodes(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	leases, err := s.store.ListActiveLeases(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	tasksByLease := map[string]leaseTask{}
	for _, l := range leases {
		tasksByLease[l.LeaseID] = leaseTask{JobID: l.JobID, TaskID: l.TaskID}
	}

	out := &orchv1.ListNodesResponse{}
	for _, n := range nodes {
		out.Nodes = append(out.Nodes, toProtoNode(n, leases, tasksByLease))
	}
	return connect.NewResponse(out), nil
}

// ListPools returns pools with live borrow and lend counters.
func (s *Server) ListPools(
	ctx context.Context,
	_ *connect.Request[orchv1.ListPoolsRequest],
) (*connect.Response[orchv1.ListPoolsResponse], error) {
	pools, err := s.store.ListPools(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	nodes, err := s.store.ListNodes(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	leases, err := s.store.ListActiveLeases(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	poolOfNode := map[string]string{}
	totalSlots := map[string]int{}
	nodeIDs := map[string][]string{}
	for _, n := range nodes {
		poolOfNode[n.NodeID] = n.PoolID
		totalSlots[n.PoolID] += len(n.Slots)
		nodeIDs[n.PoolID] = append(nodeIDs[n.PoolID], n.NodeID)
	}

	used, borrowed, lent := map[string]int{}, map[string]int{}, map[string]int{}
	for _, l := range leases {
		n := len(l.SlotIDs)
		used[l.PoolID] += n
		if l.Borrowed {
			borrowed[l.PoolID] += n
			lent[poolOfNode[l.NodeID]] += n
		}
	}

	out := &orchv1.ListPoolsResponse{}
	for _, p := range pools {
		out.Pools = append(out.Pools, &orchv1.Pool{
			PoolId:          p.PoolID,
			Name:            p.Name,
			NodeIds:         nodeIDs[p.PoolID],
			GuaranteedSlots: uint32(p.GuaranteedSlots),
			Lendable:        p.Lendable,
			BorrowCeiling:   uint32(p.BorrowCeiling),
			TotalSlots:      uint32(totalSlots[p.PoolID]),
			UsedSlots:       uint32(used[p.PoolID]),
			BorrowedSlots:   uint32(borrowed[p.PoolID]),
			LentSlots:       uint32(lent[p.PoolID]),
		})
	}
	return connect.NewResponse(out), nil
}

// UpsertPool creates or updates a pool.
func (s *Server) UpsertPool(
	ctx context.Context,
	req *connect.Request[orchv1.UpsertPoolRequest],
) (*connect.Response[orchv1.UpsertPoolResponse], error) {
	p := req.Msg.GetPool()
	if p.GetPoolId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("pool_id is required"))
	}

	name := p.GetName()
	if name == "" {
		name = p.GetPoolId()
	}
	err := s.store.UpsertPool(ctx, domain.Pool{
		PoolID:          p.GetPoolId(),
		Name:            name,
		GuaranteedSlots: int(p.GetGuaranteedSlots()),
		Lendable:        p.GetLendable(),
		BorrowCeiling:   int(p.GetBorrowCeiling()),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	for _, nodeID := range p.GetNodeIds() {
		if err := s.store.AssignNodePool(ctx, nodeID, p.GetPoolId()); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	s.bus.Emit(events.KindPoolChanged, "pool "+p.GetPoolId()+" updated",
		map[string]any{"pool_id": p.GetPoolId()})
	return connect.NewResponse(&orchv1.UpsertPoolResponse{Pool: p}), nil
}

// ListLeases returns every active lease.
func (s *Server) ListLeases(
	ctx context.Context,
	_ *connect.Request[orchv1.ListLeasesRequest],
) (*connect.Response[orchv1.ListLeasesResponse], error) {
	leases, err := s.store.ListActiveLeases(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	out := &orchv1.ListLeasesResponse{}
	for _, l := range leases {
		out.Leases = append(out.Leases, toProtoLease(l))
	}
	return connect.NewResponse(out), nil
}

// ---------------------------------------------------------------------------
// Policy
// ---------------------------------------------------------------------------

// GetPolicy returns the live scoring configuration along with the registered
// scorer names, so the UI can build controls without a hardcoded list.
func (s *Server) GetPolicy(
	ctx context.Context,
	_ *connect.Request[orchv1.GetPolicyRequest],
) (*connect.Response[orchv1.GetPolicyResponse], error) {
	p, err := s.store.GetPolicy(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&orchv1.GetPolicyResponse{
		Policy: &orchv1.Policy{
			Preset:           p.Preset,
			Weights:          scheduler.ResolveWeights(p.Preset, p.Weights),
			ReloadGeneration: uint64(p.ReloadGeneration),
		},
		AvailableScorers: scheduler.ScorerNames(),
		Presets:          scheduler.PresetNames(),
	}), nil
}

// SetPolicy replaces the scoring weights. Policy is data and hot-reloadable, so
// this takes effect on the next placement pass.
func (s *Server) SetPolicy(
	ctx context.Context,
	req *connect.Request[orchv1.SetPolicyRequest],
) (*connect.Response[orchv1.SetPolicyResponse], error) {
	in := req.Msg.GetPolicy()
	p, err := s.store.SetPolicy(ctx, in.GetPreset(), in.GetWeights())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	s.bus.Emit(events.KindPolicy,
		fmt.Sprintf("policy updated (preset %s, generation %d)", p.Preset, p.ReloadGeneration),
		map[string]any{"preset": p.Preset, "weights": p.Weights})

	return connect.NewResponse(&orchv1.SetPolicyResponse{
		Policy: &orchv1.Policy{
			Preset:           p.Preset,
			Weights:          p.Weights,
			ReloadGeneration: uint64(p.ReloadGeneration),
		},
	}), nil
}

// Explain returns the scoring breakdown behind a task's placement.
func (s *Server) Explain(
	ctx context.Context,
	req *connect.Request[orchv1.ExplainRequest],
) (*connect.Response[orchv1.ExplainResponse], error) {
	raw, err := s.store.GetExplanation(ctx, req.Msg.GetTaskId())
	if errors.Is(err, store.ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&orchv1.ExplainResponse{
		Explanation: decodeExplanation(req.Msg.GetTaskId(), raw),
	}), nil
}

// IssueJoinToken mints a single-use enrolment token.
func (s *Server) IssueJoinToken(
	ctx context.Context,
	req *connect.Request[orchv1.IssueJoinTokenRequest],
) (*connect.Response[orchv1.IssueJoinTokenResponse], error) {
	token, err := s.store.IssueJoinToken(ctx, req.Msg.GetPoolId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&orchv1.IssueJoinTokenResponse{Token: token}), nil
}

func jobLabel(j domain.Job) string {
	if j.Name != "" {
		return j.Name
	}
	return j.JobID
}
