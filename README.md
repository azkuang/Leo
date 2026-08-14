# orch — GPU compute-sharing orchestrator

A fleet of GPU workstations owned by different teams sits idle most of the time. Teams want
guaranteed access to their own machines; the organization wants those machines busy.

This reconciles both: teams get **guaranteed capacity** in a named pool, idle capacity is **lent**
to other pools automatically, and lent capacity is **reclaimed** on demand by preempting borrowers.
Borrowed work degrades; owned work never waits.

The system is workload-agnostic. It schedules containers against GPU slots. It knows nothing about
rendering, LLMs, or CAD.

---

## Quick start

Needs Go 1.22+, Node 18+, and a Postgres 16 you can connect to.

```bash
make db                    # createdb orch
make build                 # builds the UI, embeds it, compiles three binaries
./scripts/dev-up.sh        # control plane + three simulated nodes
```

Open <http://localhost:9443>. An empty database is a valid state — one implicit `default` pool
containing every node, no quotas, no borrowing — so you can submit a job before configuring
anything:

```bash
bin/orchctl submit --image sha256:demo --count 12 --sim-duration 6s
bin/orchctl fleet
```

Then run the whole story:

```bash
make demo
```

Stop with `./scripts/dev-down.sh`.

---

## The demo

`scripts/demo.sh` runs the §14 narrative end to end:

1. A laptop submits a render it cannot reasonably do locally.
2. The render pool's own two slots are not enough, so the scheduler **borrows idle CAD GPUs**.
3. Frames start completing. Utilisation climbs.
4. **"The CAD user logs in."** `orchctl trigger reclaim --pool cad`
5. Reclaim fires: CAD machines are closed to borrowers, then emptied. In-flight frames are
   preempted and requeued. Completed frames are untouched.
6. The render **keeps going, slower**, on remaining capacity. The laptop still gets its animation.

Everything the trigger console does changes the *world*, not the scheduler's behaviour. The
scheduler reacts through the same code paths it would use if the condition had arisen on its own.

---

## Architecture

```
                        ┌──────────────────────────────┐
   Client (UI/CLI) ────►│        Control plane         │
                        │  API · Scheduler · Postgres  │
                        └───────────┬──────────────────┘
                                    │ gRPC bidi stream
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
              ┌──────────┐    ┌──────────┐    ┌──────────┐
              │  Agent   │    │  Agent   │    │  Agent   │
              └────┬─────┘    └────┬─────┘    └────┬─────┘
                   └───────────────┼───────────────┘
                                   ▼
                          ┌─────────────────┐
    Client ──────────────►│  Object store   │
                          └─────────────────┘
```

**Control plane / data plane separation is absolute.** Model weights, scene files and results never
traverse the control plane. It moves references only.

| Path | What lives there |
|---|---|
| `proto/orch/v1/` | The wire contract: agent stream and client API |
| `internal/domain/` | Core types. No GPU model names anywhere |
| `internal/store/` | Persistence boundary: `Snapshot` in, `Plan` out |
| `internal/store/pgstore/` | Postgres implementation, embedded migrations |
| `internal/scheduler/` | Filter → score → reserve → commit; pools, borrowing, preemption |
| `internal/agent/` | Node runtime and the three node-side seams |
| `internal/agent/sim/` | Simulated device, health and executor providers |
| `internal/api/` | ConnectRPC handlers, agent hub, SSE |
| `web/` | React + Tailwind UI, embedded into `orchd` at build time |

### The scheduler

Single-threaded by design. Placement decisions are inherently serialized against shared state — two
concurrent scorers select the same idle slot. One leader, one loop, transactional commit. At this
fleet size that is over-provisioned by orders of magnitude.

Each pass reads a whole-fleet snapshot, decides, and commits one transaction that revalidates
everything it touches. A multi-slot request is a **gang**: all slots on one machine, all-or-nothing.
Allocating them one at a time deadlocks when two multi-slot jobs interleave.

**Phase 1 — filter.** Every predicate reads the capability document: VRAM, compute capability,
driver and CUDA floors, ECC, labels, host share, pool eligibility. There is no list of GPU models in
the scheduler or anywhere it can reach, so a fleet with entirely different hardware works with zero
code change. Version comparison is numeric, not lexical — `10.0` is newer than `8.9`.

**Phase 2 — score.** Registered, independently weighted dimensions:

| Scorer | Signal |
|---|---|
| `fit` | Prefer the smallest adequate slot — don't burn 96GB on a 7B model |
| `queue_depth` | Projected wait; a slower idle GPU beats a faster busy one |
| `thermal` | Throttle reasons and temperature headroom |
| `cache_residency` | Node already holds the assets, weighted by size |
| `borrow_penalty` | Mild bias toward native capacity |
| `lender_concentration` | Pack borrowers onto the fewest lender machines |
| `reliability` | Decay-weighted failure history; an unknown node is trusted |

Weights are data, hot-reloadable, editable in the UI and via `orchctl set-policy`. Presets ship as
`throughput`, `latency` and `fair-share`.

`orchctl explain <task-id>` shows the full scoring breakdown and why each rejected node did not fit.

### Borrowing and reclaim

- Each pool has **guaranteed** slots it can always claim. Unset means "the machines I own".
- Idle guaranteed capacity is **lendable**.
- Borrowed leases run **preemptible regardless of the job's own flag**. That is the deal that makes
  lending safe for the owner.
- A **borrow ceiling** caps how much any pool may take.

Borrowed leases are always evicted before native ones, which makes preemption ordering trivially
correct. Eviction picks the cheapest victim by elapsed runtime, with a penalty for tasks that have
already been preempted — otherwise the same unlucky task is evicted forever while its peers finish.

Machine reclaim marks the host **non-lendable in the same transaction that evicts**. Evicting first
and closing the door afterwards livelocks: the loop refills slots behind the eviction as fast as
they are freed.

> After a reclaim the machines stay closed to borrowers — their owner is using them. Reopen with
> `orchctl trigger node-recover --node <id>`, or the **heal** button in the fleet view.

### Leases and fencing

Every allocation is a time-bounded lease. **The lease ID is the fencing token.** A status update
quoting a lease the control plane no longer recognises is rejected at commit. Each attempt writes to
`jobs/{job}/tasks/{task}/attempts/{n}/`, so a zombie worker from a preempted attempt writes
somewhere nobody reads — no storage-layer fencing required.

Short TTLs with renewal mean a workstation that gets unplugged frees its slots in seconds.

### Host contention

Machine-owned / slot-lent means a borrowed workload runs on someone else's machine. The GPU is
partitioned; the host is not. Each slot carries a derived share of cores and RAM, enforced through
cgroups v2 via containerd. `--exclusive-host` reserves a whole machine.

---

## Configuration

Pools are a refinement, never a prerequisite:

```yaml
# deploy/fleet.yaml — applied with: orchctl bootstrap deploy/fleet.yaml
pools:
  - name: cad
    nodes: [cad-01, cad-02]
    lendable: true
    borrow_ceiling: 0        # lends, never borrows
  - name: render
    nodes: [render-01]
    lendable: true
    borrow_ceiling: 4
policy:
  preset: throughput
```

Idempotent. Nodes are named by hostname because that is what a human knows; node IDs are assigned by
the control plane and never typed by anyone.

### Node onboarding

```bash
orchctl join-token --pool cad          # single-use
sudo ORCH_SERVER=orch.local:9443 ORCH_JOIN_TOKEN=<token> deploy/install-agent.sh
```

The agent enumerates its own devices, probes host resources, checks driver and CUDA versions, and
registers. Nothing about the machine is typed by a human, because every fact a human types is a fact
that goes stale.

### Manufactured heterogeneity

If every machine is identical, every placement is arbitrary and the scheduler has nothing to show.
`deploy/profiles/*.yaml` describe simulated nodes with different VRAM, compute capability and
thermal behaviour — the desk-side render box idles at 50°C and throttles under load; the racked CAD
workstations do not.

---

## Development

```bash
make build             # UI + binaries
make test              # unit tests, no database needed
make lint              # go vet + gofmt

# Store tests against a real Postgres
createdb orch_test
ORCH_TEST_DSN="postgres:///orch_test?host=/var/run/postgresql" make test-integration

# Render every UI component against a running control plane
make test-ui

make proto             # regenerate ConnectRPC bindings (needs buf)
```

The UI dev server proxies to the control plane:

```bash
cd web && npm run dev   # http://localhost:5173
```

### Docker

`deploy/docker-compose.yml` brings up Postgres, MinIO and the control plane. Agents are not
containers — they need the host's devices, driver and container runtime — so they install as systemd
units on the machines themselves.

---

## Extension seams

Four interfaces, resolved through a registry and **compiled in**. Go's `plugin` package is
deliberately not used: dynamically loaded `.so` files require identical Go versions and dependency
trees for a benefit this project does not need.

| Interface | Methods | Implementations |
|---|---|---|
| `Executor` | `Start` / `Stop` / `Status` | simulated · *containerd pending* |
| `DeviceProvider` | `Enumerate` | simulated · *NVML pending* |
| `HealthSource` | `Stream` | simulated · *DCGM pending* |
| `Scorer` | `Score` | seven, one per dimension |

Adding a scoring dimension means adding a file with an `init()` that calls `Register`. It appears in
the API, the CLI and the UI's weight controls without touching any of them.

---
