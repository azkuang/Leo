# orch — GPU compute-sharing orchestrator

A fleet of GPU workstations owned by different teams sits idle most of the time. Teams want
guaranteed access to their own machines; the organization wants those machines busy.

This reconciles both: teams get **guaranteed capacity** in a named pool, idle capacity is **lent**
to other pools automatically, and lent capacity is **reclaimed** on demand by preempting borrowers.
Borrowed work degrades; owned work never waits.

The system is workload-agnostic. It schedules containers against GPU slots. It knows nothing about
rendering, LLMs, or CAD.

---

# System Architecture
```mermaid
flowchart TB
    subgraph Client["Client side"]
        UI["Browser UI<br/>(React, embedded in orchd)"]
        CLI["orchctl<br/>(CLI client)"]
    end

    subgraph ControlPlane["Control plane — orchd (single process)"]
        API["internal/api<br/>ConnectRPC handlers<br/>(OrchService)"]
        HUB["internal/api.Hub<br/>agent connections<br/>(AgentService, bidi stream)"]
        SCHED["internal/scheduler<br/>filter → score → reserve → commit<br/>500ms loop, single-threaded"]
        BUS["internal/events.Bus<br/>in-memory pub/sub"]
        SSE["/events (SSE)"]
        STORE["internal/store/pgstore<br/>Postgres via pgx"]
    end

    PG[("PostgreSQL 16<br/>scheduling state + job history")]
    OBJ[("MinIO / S3<br/>object store — payloads only")]

    subgraph Agents["Node agents — orchd-agent, one per machine"]
        A1["Agent (real)<br/>NVML · DCGM · containerd"]
        A2["Agent (simulated)<br/>sim.Node"]
    end

    UI -- "ConnectRPC (HTTP/JSON or gRPC)" --> API
    UI -- "SSE stream" --> SSE
    CLI -- "ConnectRPC" --> API
    SSE --> BUS
    API --> BUS
    API -- "reads/writes" --> STORE
    API -- "RequestMachineReclaim()" --> SCHED
    SCHED -- "Snapshot() / Commit()" --> STORE
    SCHED -- "Assign() / Preempt() via Dispatcher iface" --> HUB
    STORE --> PG

    HUB -- "bidi gRPC stream<br/>heartbeat up / assignment down" --> A1
    HUB -- "bidi gRPC stream" --> A2

    A1 -- "stage/fetch assets, write results" --> OBJ
    A2 -. "simulated, no real bytes" .-> OBJ
    UI -. "uploads/downloads assets directly" .-> OBJ

    style ControlPlane fill:#1f2937,color:#fff,stroke:#374151
    style Agents fill:#1f2937,color:#fff,stroke:#374151
    style Client fill:#1f2937,color:#fff,stroke:#374151
```

---
