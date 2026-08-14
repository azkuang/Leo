package api

import (
	"context"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"gopkg.in/yaml.v3"

	orchv1 "github.com/alexk/orch/gen/orch/v1"
	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/events"
)

// BootstrapDoc is the declarative fleet description from §13.
//
// It is a refinement, not a prerequisite: an empty database is a valid state,
// with one implicit default pool containing every node. This file exists so a
// new fleet is one command rather than a click-through.
type BootstrapDoc struct {
	Pools []struct {
		Name          string   `yaml:"name"`
		Nodes         []string `yaml:"nodes"`
		Lendable      *bool    `yaml:"lendable"`
		BorrowCeiling int      `yaml:"borrow_ceiling"`
		Guaranteed    int      `yaml:"guaranteed_slots"`
	} `yaml:"pools"`

	Policy struct {
		Preset  string             `yaml:"preset"`
		Weights map[string]float64 `yaml:"weights"`
	} `yaml:"policy"`
}

// Bootstrap applies a declarative document idempotently.
//
// Nodes are named by hostname, because that is what a human knows; the node IDs
// they map to are assigned by the control plane and never typed by anyone.
// Applying the same file twice changes nothing the second time.
func (s *Server) Bootstrap(
	ctx context.Context,
	req *connect.Request[orchv1.BootstrapRequest],
) (*connect.Response[orchv1.BootstrapResponse], error) {
	var doc BootstrapDoc
	if err := yaml.Unmarshal([]byte(req.Msg.GetYaml()), &doc); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("parse yaml: %w", err))
	}

	nodes, err := s.store.ListNodes(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	byHostname := make(map[string]string, len(nodes))
	for _, n := range nodes {
		byHostname[n.Hostname] = n.NodeID
		byHostname[n.NodeID] = n.NodeID
	}

	var (
		applied []string
		unknown []string
	)

	for _, p := range doc.Pools {
		if p.Name == "" {
			continue
		}
		lendable := true
		if p.Lendable != nil {
			lendable = *p.Lendable
		}

		err := s.store.UpsertPool(ctx, domain.Pool{
			PoolID:          p.Name,
			Name:            p.Name,
			GuaranteedSlots: p.Guaranteed,
			Lendable:        lendable,
			BorrowCeiling:   p.BorrowCeiling,
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		assigned := 0
		for _, host := range p.Nodes {
			nodeID, ok := byHostname[host]
			if !ok {
				// A pool may name machines that have not joined yet. That is
				// ordinary during a rollout, so it is reported rather than
				// treated as an error.
				unknown = append(unknown, host)
				continue
			}
			if err := s.store.AssignNodePool(ctx, nodeID, p.Name); err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			assigned++
		}
		applied = append(applied, fmt.Sprintf("pool %s (%d node(s), ceiling %d, lendable %t)",
			p.Name, assigned, p.BorrowCeiling, lendable))
	}

	if doc.Policy.Preset != "" || len(doc.Policy.Weights) > 0 {
		p, err := s.store.SetPolicy(ctx, doc.Policy.Preset, doc.Policy.Weights)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		applied = append(applied, fmt.Sprintf("policy preset %s (generation %d)", p.Preset, p.ReloadGeneration))
	}

	detail := strings.Join(applied, "; ")
	if len(unknown) > 0 {
		detail += fmt.Sprintf("; %d node(s) not joined yet: %s",
			len(unknown), strings.Join(unknown, ", "))
	}
	if detail == "" {
		detail = "nothing to apply"
	}

	s.bus.Emit(events.KindPoolChanged, "bootstrap applied: "+detail, nil)
	return connect.NewResponse(&orchv1.BootstrapResponse{Detail: detail}), nil
}
