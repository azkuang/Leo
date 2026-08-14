package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"

	orchv1 "github.com/alexk/orch/gen/orch/v1"
	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/events"
)

// Trigger is the demo's scripted-event console (§14).
//
// Live demos fail waiting for organic events. Rather than hoping a node
// overheats on cue, the interesting conditions are injectable: node failure,
// thermal throttle, XID error, and the owner coming back for their machines.
//
// Note what this is not. It does not fabricate scheduler behaviour -- every
// button here changes the *world*, and the scheduler reacts through exactly the
// same code paths it would use if the condition had arisen on its own.
func (s *Server) Trigger(
	ctx context.Context,
	req *connect.Request[orchv1.TriggerRequest],
) (*connect.Response[orchv1.TriggerResponse], error) {
	msg := req.Msg

	switch msg.GetKind() {
	case orchv1.TriggerKind_TRIGGER_KIND_NODE_FAILURE:
		return s.triggerNodeFailure(ctx, msg.GetNodeId())

	case orchv1.TriggerKind_TRIGGER_KIND_NODE_RECOVER:
		return s.triggerNodeRecover(ctx, msg.GetNodeId())

	case orchv1.TriggerKind_TRIGGER_KIND_THERMAL_THROTTLE:
		return s.triggerInject(msg.GetNodeId(), "thermal_throttle", 1,
			"thermal throttling injected on %s")

	case orchv1.TriggerKind_TRIGGER_KIND_CLEAR_THROTTLE:
		return s.triggerInject(msg.GetNodeId(), "clear", 0,
			"injected conditions cleared on %s")

	case orchv1.TriggerKind_TRIGGER_KIND_XID_ERROR:
		return s.triggerInject(msg.GetNodeId(), "xid_error", 79,
			"XID 79 injected on %s")

	case orchv1.TriggerKind_TRIGGER_KIND_RECLAIM:
		return s.triggerReclaim(ctx, msg)
	}

	return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("unknown trigger kind"))
}

// triggerNodeFailure simulates a workstation going away without warning.
func (s *Server) triggerNodeFailure(ctx context.Context, nodeID string) (*connect.Response[orchv1.TriggerResponse], error) {
	if nodeID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("node_id is required"))
	}

	if err := s.store.SetNodeState(ctx, nodeID, domain.NodeUnhealthy); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	released, err := s.store.ReleaseLeasesOnNode(ctx, nodeID, "node failure injected", true)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Tell the agent to stop what it is running. If it has genuinely gone, this
	// is a no-op and the leases have already been released above.
	_ = s.hub.Drain(nodeID, "node failure injected", 0)

	ids := leaseIDs(released)
	detail := fmt.Sprintf("%s marked unhealthy, %d lease(s) requeued", nodeID, len(released))
	s.bus.Emit(events.KindTrigger, detail, map[string]any{"node_id": nodeID, "kind": "node_failure"})

	return connect.NewResponse(&orchv1.TriggerResponse{
		Detail: detail, AffectedLeaseIds: ids,
	}), nil
}

// triggerNodeRecover brings a node back and reopens it to borrowers.
func (s *Server) triggerNodeRecover(ctx context.Context, nodeID string) (*connect.Response[orchv1.TriggerResponse], error) {
	if nodeID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("node_id is required"))
	}
	if err := s.store.SetNodeState(ctx, nodeID, domain.NodeReady); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := s.store.SetNodeLendable(ctx, nodeID, true); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	_ = s.hub.Inject(nodeID, "clear", 0, 0)

	detail := nodeID + " recovered and reopened to borrowers"
	s.bus.Emit(events.KindTrigger, detail, map[string]any{"node_id": nodeID, "kind": "node_recover"})
	return connect.NewResponse(&orchv1.TriggerResponse{Detail: detail}), nil
}

// triggerInject forwards a scripted health condition to a simulated node.
func (s *Server) triggerInject(nodeID, kind string, value int, format string) (*connect.Response[orchv1.TriggerResponse], error) {
	if nodeID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("node_id is required"))
	}
	if err := s.hub.Inject(nodeID, kind, 0, value); err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}

	detail := fmt.Sprintf(format, nodeID)
	s.bus.Emit(events.KindTrigger, detail, map[string]any{"node_id": nodeID, "kind": kind})
	return connect.NewResponse(&orchv1.TriggerResponse{Detail: detail}), nil
}

// triggerReclaim is the "CAD user logs in" moment.
//
// The owning pool wants its machines back. Borrowers are evicted and their
// tasks requeued; the borrowing job keeps going, slower, on whatever capacity
// remains. Completed work is untouched, which is the whole point of preempting
// at task granularity rather than job granularity.
func (s *Server) triggerReclaim(ctx context.Context, msg *orchv1.TriggerRequest) (*connect.Response[orchv1.TriggerResponse], error) {
	targets, err := s.reclaimTargets(ctx, msg)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return connect.NewResponse(&orchv1.TriggerResponse{
			Detail: "nothing to reclaim: no borrowers on those machines",
		}), nil
	}

	var affected []string
	for _, nodeID := range targets {
		reason := "owner reclaimed " + nodeID
		ids, err := s.reclaimer.RequestMachineReclaim(ctx, nodeID, reason)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		affected = append(affected, ids...)
	}

	detail := fmt.Sprintf("reclaimed %d machine(s), evicted %d borrowed lease(s)",
		len(targets), len(affected))
	s.bus.Emit(events.KindTrigger, detail, map[string]any{
		"kind": "reclaim", "nodes": targets, "leases": affected,
	})

	return connect.NewResponse(&orchv1.TriggerResponse{
		Detail: detail, AffectedLeaseIds: affected,
	}), nil
}

// reclaimTargets resolves a reclaim request to the machines to empty.
func (s *Server) reclaimTargets(ctx context.Context, msg *orchv1.TriggerRequest) ([]string, error) {
	if msg.GetNodeId() != "" {
		return []string{msg.GetNodeId()}, nil
	}
	if msg.GetPoolId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("reclaim needs a node_id or a pool_id"))
	}

	nodes, err := s.store.ListNodes(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	leases, err := s.store.ListActiveLeases(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Only machines actually hosting a borrower are worth reclaiming; closing
	// an idle machine to lending would be a policy change, not a reclaim.
	lending := map[string]bool{}
	for _, l := range leases {
		if l.Borrowed {
			lending[l.NodeID] = true
		}
	}

	var targets []string
	for _, n := range nodes {
		if n.PoolID == msg.GetPoolId() && lending[n.NodeID] {
			targets = append(targets, n.NodeID)
		}
	}
	return targets, nil
}

func leaseIDs(leases []domain.Lease) []string {
	out := make([]string, 0, len(leases))
	for _, l := range leases {
		out = append(out, l.LeaseID)
	}
	return out
}

// decodeExplanation converts the stored JSON breakdown back into the wire type.
func decodeExplanation(taskID string, raw []byte) *orchv1.PlacementExplanation {
	var doc struct {
		TaskID        string   `json:"task_id"`
		ChosenNodeID  string   `json:"chosen_node_id"`
		ChosenSlotIDs []string `json:"chosen_slot_ids"`
		Candidates    []struct {
			NodeID      string             `json:"node_id"`
			Total       float64            `json:"total"`
			PerScorer   map[string]float64 `json:"per_scorer"`
			WouldBorrow bool               `json:"would_borrow"`
			Chosen      bool               `json:"chosen"`
		} `json:"candidates"`
		FilteredOut []string  `json:"filtered_out"`
		At          time.Time `json:"at"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return &orchv1.PlacementExplanation{TaskId: taskID}
	}

	out := &orchv1.PlacementExplanation{
		TaskId:        doc.TaskID,
		ChosenNodeId:  doc.ChosenNodeID,
		ChosenSlotIds: doc.ChosenSlotIDs,
		FilteredOut:   doc.FilteredOut,
		At:            ts(doc.At),
	}
	for _, c := range doc.Candidates {
		out.Candidates = append(out.Candidates, &orchv1.CandidateScore{
			NodeId:      c.NodeID,
			Total:       c.Total,
			PerScorer:   c.PerScorer,
			WouldBorrow: c.WouldBorrow,
			Chosen:      c.Chosen,
		})
	}
	return out
}
