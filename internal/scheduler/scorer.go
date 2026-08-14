package scheduler

import (
	"sort"
	"time"

	"github.com/alexk/orch/internal/domain"
)

// FleetInfo carries the fleet-wide facts a scorer may need, gathered once per
// pass so scoring stays a pure function over data with no I/O.
type FleetInfo struct {
	// BorrowersOnNode counts borrowed leases already resident on each node.
	BorrowersOnNode map[string]int
	// MaxQueueDepth normalises the queue_depth scorer against the busiest node.
	MaxQueueDepth int
	Now           time.Time
}

// ScoreInput is everything a scoring dimension is allowed to look at.
type ScoreInput struct {
	Job       domain.Job
	Request   domain.ResourceRequest
	Candidate *Candidate
	Fleet     *FleetInfo
}

// Scorer is one soft, weighted scoring dimension -- Phase 2 of placement.
//
// This is one of the four extension seams. Implementations are compiled in and
// registered by name; Go's `plugin` package is deliberately not used, since
// dynamically loaded .so files require identical Go versions and dependency
// trees for a benefit this project does not need.
type Scorer interface {
	Name() string
	// Score returns a value in [0,1]. Higher is better. Returning outside that
	// range breaks weight comparability, so implementations clamp.
	Score(in ScoreInput) float64
}

// registry holds every compiled-in scorer by name.
var registry = map[string]Scorer{}

// Register adds a scorer to the registry. It is called from init functions, so
// adding a dimension means adding a file, not editing a switch.
func Register(s Scorer) { registry[s.Name()] = s }

// ScorerNames returns every registered scorer name, sorted. The UI builds its
// weight controls from this rather than a hardcoded list, so a new scorer shows
// up in the interface for free.
func ScorerNames() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Presets are the shipped weight sets. Flipping between them and watching
// placement change is the cheapest convincing thing the scheduler can
// demonstrate, so they are a first-class feature rather than an example config.
var Presets = map[string]map[string]float64{
	// Keep every GPU busy and waste as little capability as possible.
	"throughput": {
		"fit":            0.4,
		"queue_depth":    0.3,
		"thermal":        0.2,
		"borrow_penalty": 0.1,
	},
	// Start the next task as soon as possible: prefer idle machines and
	// machines that already hold the assets.
	"latency": {
		"queue_depth":     0.5,
		"cache_residency": 0.25,
		"thermal":         0.15,
		"fit":             0.1,
	},
	// Protect owners: lean on native capacity, and when borrowing is
	// unavoidable, concentrate it so reclaim stays cheap.
	"fair-share": {
		"borrow_penalty":       0.35,
		"lender_concentration": 0.25,
		"queue_depth":          0.2,
		"reliability":          0.1,
		"fit":                  0.1,
	},
}

// PresetNames returns the shipped preset names, sorted.
func PresetNames() []string {
	names := make([]string, 0, len(Presets))
	for n := range Presets {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// ResolveWeights turns a stored policy into the weight map to score with.
//
// Explicit weights win over the preset, so the UI's sliders can start from a
// preset and diverge from it without having to name the result.
func ResolveWeights(preset string, weights map[string]float64) map[string]float64 {
	out := map[string]float64{}
	if base, ok := Presets[preset]; ok {
		for k, v := range base {
			out[k] = v
		}
	}
	for k, v := range weights {
		out[k] = v
	}
	if len(out) == 0 {
		out = Presets["throughput"]
	}
	return out
}

// score evaluates every weighted dimension for one candidate and records the
// breakdown, which is what lets the UI explain a placement instead of just
// announcing it.
func score(c *Candidate, in ScoreInput, weights map[string]float64) {
	var sum, total float64
	for name, w := range weights {
		if w == 0 {
			continue
		}
		s, ok := registry[name]
		if !ok {
			// An unknown name in the policy is ignored rather than fatal: the
			// weights are user-editable data, and a typo should not stop the
			// fleet from scheduling.
			continue
		}
		v := clamp01(s.Score(in))
		c.Scores[name] = v
		sum += w * v
		total += w
	}
	if total > 0 {
		c.Total = sum / total
	}
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
