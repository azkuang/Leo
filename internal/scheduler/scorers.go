package scheduler

// The registered scoring dimensions. Each is independently weighted, and each
// answers exactly one question about a candidate placement.

func init() {
	Register(fitScorer{})
	Register(queueDepthScorer{})
	Register(thermalScorer{})
	Register(cacheResidencyScorer{})
	Register(borrowPenaltyScorer{})
	Register(lenderConcentrationScorer{})
	Register(reliabilityScorer{})
}

// ---------------------------------------------------------------------------

// fitScorer prefers the smallest adequate slot -- don't burn 96GB on a 7B
// model. Scored as the fraction of the chosen card actually asked for.
type fitScorer struct{}

func (fitScorer) Name() string { return "fit" }

func (fitScorer) Score(in ScoreInput) float64 {
	need := in.Request.MinVRAMGB
	if need <= 0 {
		// With no stated VRAM floor there is no waste to measure, so this
		// dimension has nothing to say and abstains at neutral.
		return 0.5
	}
	var sum float64
	for _, s := range in.Candidate.Slots {
		if s.VRAMGB <= 0 {
			continue
		}
		sum += float64(need) / float64(s.VRAMGB)
	}
	if len(in.Candidate.Slots) == 0 {
		return 0
	}
	return sum / float64(len(in.Candidate.Slots))
}

// ---------------------------------------------------------------------------

// queueDepthScorer approximates projected wait. A slower idle GPU beats a
// faster busy one, which is the whole reason this dimension outranks raw
// capability in the throughput preset.
//
// It also does the spreading half of §7's fragmentation guidance: native
// allocations naturally fan out across idle machines because the busiest node
// always scores worst here.
type queueDepthScorer struct{}

func (queueDepthScorer) Name() string { return "queue_depth" }

func (queueDepthScorer) Score(in ScoreInput) float64 {
	depth := in.Candidate.node.queueDepth
	maxDepth := in.Fleet.MaxQueueDepth
	if maxDepth <= 0 {
		// Nothing is running anywhere; every node is equally idle.
		return 1
	}
	return 1 - float64(depth)/float64(maxDepth+1)
}

// ---------------------------------------------------------------------------

// thermalScorer reads throttle reasons and temperature headroom.
//
// Throttle reasons matter more on desk-side workstations than in a rack: they
// separate "slow because busy" from "slow because degraded", and a scheduler
// that keeps assigning to a thermally capped card is actively making things
// worse.
type thermalScorer struct{}

func (thermalScorer) Name() string { return "thermal" }

func (thermalScorer) Score(in ScoreInput) float64 {
	slots := in.Candidate.Slots
	if len(slots) == 0 {
		return 0
	}

	var sum float64
	for _, s := range slots {
		v := headroom(s.Health.TemperatureC)
		if s.Health.Throttled() {
			// A card already being held back will not deliver what its
			// capabilities advertise, whatever its current temperature reads.
			v *= 0.25
		}
		sum += v
	}
	return sum / float64(len(slots))
}

// headroom maps temperature onto [0,1]. Below 50C is unconstrained; by 90C
// there is nothing left.
func headroom(tempC float64) float64 {
	const (
		cool = 50.0
		hot  = 90.0
	)
	if tempC <= 0 {
		// Unreported temperature is not evidence of a problem.
		return 1
	}
	return clamp01((hot - tempC) / (hot - cool))
}

// ---------------------------------------------------------------------------

// cacheResidencyScorer prefers nodes that already hold the job's assets (§11).
//
// This is the same warm-affinity principle that applies to resident model
// weights, in a general form: the content address is the cache key, so the
// scheduler gets the benefit without knowing what the payload is.
type cacheResidencyScorer struct{}

func (cacheResidencyScorer) Name() string { return "cache_residency" }

func (cacheResidencyScorer) Score(in ScoreInput) float64 {
	assets := in.Job.Assets
	if len(assets) == 0 {
		return 1
	}

	resident := make(map[string]bool, len(in.Candidate.Node.CachedDigests))
	for _, d := range in.Candidate.Node.CachedDigests {
		resident[d] = true
	}

	// Weighted by size: holding the one large scene file matters more than
	// holding three small textures.
	var have, total uint64
	for _, a := range assets {
		size := a.SizeBytes
		if size == 0 {
			size = 1
		}
		total += size
		if resident[a.Digest] {
			have += size
		}
	}
	if total == 0 {
		return 1
	}
	return float64(have) / float64(total)
}

// ---------------------------------------------------------------------------

// borrowPenaltyScorer is a mild bias toward native capacity over borrowing.
//
// Mild is the point. Borrowing is the feature, not a fallback -- this exists so
// that when native and borrowed capacity are otherwise equal, the fleet leaves
// other people's machines alone.
type borrowPenaltyScorer struct{}

func (borrowPenaltyScorer) Name() string { return "borrow_penalty" }

func (borrowPenaltyScorer) Score(in ScoreInput) float64 {
	if in.Candidate.Borrowed {
		return 0
	}
	return 1
}

// ---------------------------------------------------------------------------

// lenderConcentrationScorer packs borrowers onto the fewest lender machines.
//
// This makes reclaim cheaper: taking a machine back evicts borrowers from one
// host rather than several. Native placements score neutral-best here because
// they fragment no lender at all -- their spreading comes from queue_depth.
type lenderConcentrationScorer struct{}

func (lenderConcentrationScorer) Name() string { return "lender_concentration" }

func (lenderConcentrationScorer) Score(in ScoreInput) float64 {
	if !in.Candidate.Borrowed {
		return 1
	}

	existing := in.Fleet.BorrowersOnNode[in.Candidate.Node.NodeID]
	if existing == 0 {
		// Opening a new lender host is the outcome this dimension exists to
		// discourage.
		return 0
	}
	// Diminishing returns: the first co-tenant is most of the benefit.
	return clamp01(float64(existing) / float64(existing+1))
}

// ---------------------------------------------------------------------------

// reliabilityScorer reads the decay-weighted historical failure rate.
//
// A node with no history scores as trusted. Being new is not evidence of being
// bad, and treating it as such would keep a freshly joined machine permanently
// last in line.
type reliabilityScorer struct{}

func (reliabilityScorer) Name() string { return "reliability" }

func (reliabilityScorer) Score(in ScoreInput) float64 {
	ok := in.Candidate.Node.RecentSuccesses
	bad := in.Candidate.Node.RecentFailures
	if ok+bad == 0 {
		return 1
	}
	return ok / (ok + bad)
}
