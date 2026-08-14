-- Initial schema.
--
-- Postgres carries both the scheduling state and the job history. The reason
-- this is one store and not two is §7's gang reservation: reserving N slots on
-- one machine must be all-or-nothing, and here that is a single transaction
-- rather than a hand-rolled multi-key CAS.

-- ---------------------------------------------------------------------------
-- Pools (§8)
-- ---------------------------------------------------------------------------

CREATE TABLE pools (
    pool_id          TEXT PRIMARY KEY,
    name             TEXT        NOT NULL,
    guaranteed_slots INT         NOT NULL DEFAULT 0,
    lendable         BOOLEAN     NOT NULL DEFAULT TRUE,
    borrow_ceiling   INT         NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- An empty database is a valid state. The implicit default pool contains every
-- node, has no quota, and borrows nothing -- so `docker compose up`, start an
-- agent, submit a job works before anyone reads documentation.
INSERT INTO pools (pool_id, name, guaranteed_slots, lendable, borrow_ceiling)
VALUES ('default', 'default', 0, TRUE, 0);

-- ---------------------------------------------------------------------------
-- Fleet
-- ---------------------------------------------------------------------------

CREATE TABLE nodes (
    node_id         TEXT PRIMARY KEY,
    hostname        TEXT    NOT NULL,
    pool_id         TEXT    NOT NULL REFERENCES pools (pool_id),
    state           TEXT    NOT NULL DEFAULT 'offline',

    -- The self-reported capability document (§6). The scheduler reads only
    -- this; it contains no list of GPU models anywhere.
    capability_doc  JSONB   NOT NULL DEFAULT '{}',
    labels          JSONB   NOT NULL DEFAULT '{}',

    simulated       BOOLEAN NOT NULL DEFAULT FALSE,

    -- Cleared for the duration of a machine reclaim so the reclaim cannot
    -- livelock against new placements landing on the host it is emptying.
    lendable        BOOLEAN NOT NULL DEFAULT TRUE,

    last_heartbeat  TIMESTAMPTZ,

    host_cores      INT NOT NULL DEFAULT 0,
    host_ram_gb     INT NOT NULL DEFAULT 0,
    host_scratch_gb INT NOT NULL DEFAULT 0,
    free_scratch_gb INT NOT NULL DEFAULT 0,
    load_avg_1m     DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- Digests resident in this node's content-addressed cache (§11). Feeds the
    -- cache_residency scorer.
    cached_digests  TEXT[] NOT NULL DEFAULT '{}',

    -- Decay-weighted outcome counters behind the reliability scorer. Both are
    -- multiplied by a decay factor on every update, so old history fades
    -- rather than being trimmed by a cutoff.
    recent_successes DOUBLE PRECISION NOT NULL DEFAULT 0,
    recent_failures  DOUBLE PRECISION NOT NULL DEFAULT 0,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX nodes_pool_idx ON nodes (pool_id);
CREATE INDEX nodes_state_idx ON nodes (state);

-- The unit of allocation. Either a whole device or a partition of one; the
-- scheduler does not distinguish.
CREATE TABLE slots (
    slot_id            TEXT PRIMARY KEY,
    node_id            TEXT NOT NULL REFERENCES nodes (node_id) ON DELETE CASCADE,
    kind               TEXT NOT NULL DEFAULT 'gpu',
    device_index       INT  NOT NULL,

    vram_gb            INT     NOT NULL,
    compute_capability TEXT    NOT NULL DEFAULT '',
    encode_sessions    INT     NOT NULL DEFAULT 0,
    decode_sessions    INT     NOT NULL DEFAULT 0,
    ecc_present        BOOLEAN NOT NULL DEFAULT FALSE,

    state              TEXT NOT NULL DEFAULT 'free',
    lease_id           TEXT,

    -- Latest fast-path sample. Written on every heartbeat, read by the
    -- thermal and reliability scorers.
    health             JSONB NOT NULL DEFAULT '{}',

    UNIQUE (node_id, device_index)
);

CREATE INDEX slots_node_idx ON slots (node_id);
CREATE INDEX slots_state_idx ON slots (state);
CREATE INDEX slots_lease_idx ON slots (lease_id);

-- ---------------------------------------------------------------------------
-- Work
-- ---------------------------------------------------------------------------

CREATE TABLE jobs (
    job_id       TEXT PRIMARY KEY,
    name         TEXT NOT NULL DEFAULT '',
    owner        TEXT NOT NULL DEFAULT '',
    pool_id      TEXT NOT NULL REFERENCES pools (pool_id),
    image_digest TEXT NOT NULL,
    -- Command, env, assets and the resource request. Opaque to the scheduler
    -- apart from the request, which is read as capability predicates.
    spec         JSONB NOT NULL DEFAULT '{}',
    mode         TEXT NOT NULL DEFAULT 'batch',
    preemptible  BOOLEAN NOT NULL DEFAULT TRUE,
    state        TEXT NOT NULL DEFAULT 'pending',
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX jobs_state_idx ON jobs (state);
CREATE INDEX jobs_owner_idx ON jobs (owner);

CREATE TABLE tasks (
    task_id           TEXT PRIMARY KEY,
    job_id            TEXT NOT NULL REFERENCES jobs (job_id) ON DELETE CASCADE,
    idx               INT  NOT NULL,
    -- Opaque to the orchestrator. A render job puts a frame number here; the
    -- moment the scheduler knows what a frame is, this became a render farm.
    params            JSONB NOT NULL DEFAULT '{}',
    attempt           INT  NOT NULL DEFAULT 0,
    state             TEXT NOT NULL DEFAULT 'queued',
    assigned_lease_id TEXT,
    node_id           TEXT,
    output_prefix     TEXT NOT NULL DEFAULT '',
    preemptions       INT  NOT NULL DEFAULT 0,
    message           TEXT NOT NULL DEFAULT '',
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    enqueued_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_id, idx)
);

-- The queue. `SELECT ... FOR UPDATE SKIP LOCKED` over this index is the whole
-- dequeue mechanism; a separate broker would be an extra stateful system for
-- no gain at this scale.
CREATE INDEX tasks_queue_idx ON tasks (state, enqueued_at) WHERE state = 'queued';
CREATE INDEX tasks_job_idx ON tasks (job_id);
CREATE INDEX tasks_lease_idx ON tasks (assigned_lease_id);

-- ---------------------------------------------------------------------------
-- Leases (§3)
-- ---------------------------------------------------------------------------

CREATE TABLE leases (
    lease_id          TEXT PRIMARY KEY,
    task_id           TEXT NOT NULL,
    job_id            TEXT NOT NULL,
    pool_id           TEXT NOT NULL,
    node_id           TEXT NOT NULL REFERENCES nodes (node_id) ON DELETE CASCADE,
    slot_ids          TEXT[] NOT NULL,
    mode              TEXT NOT NULL DEFAULT 'batch',

    -- Borrowed leases are always evicted before native ones in the owning
    -- pool, which is what makes preemption ordering trivially correct.
    borrowed          BOOLEAN NOT NULL DEFAULT FALSE,
    exclusive_host    BOOLEAN NOT NULL DEFAULT FALSE,

    -- The per-slot share of the host, enforced via cgroups. The GPU is
    -- partitioned; the host is not, so this is the difference between
    -- borrowing being safe and being a support burden.
    host_share_cores  DOUBLE PRECISION NOT NULL DEFAULT 0,
    host_share_ram_gb INT NOT NULL DEFAULT 0,

    granted_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ttl_ms            BIGINT NOT NULL,
    renewed_at        TIMESTAMPTZ,
    state             TEXT NOT NULL DEFAULT 'active',
    released_reason   TEXT NOT NULL DEFAULT ''
);

CREATE INDEX leases_active_idx ON leases (state) WHERE state = 'active';
CREATE INDEX leases_node_idx ON leases (node_id);
CREATE INDEX leases_task_idx ON leases (task_id);

-- ---------------------------------------------------------------------------
-- Policy (§5) -- data, hot-reloadable, never compiled in
-- ---------------------------------------------------------------------------

CREATE TABLE policy (
    id                INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    preset            TEXT  NOT NULL DEFAULT 'throughput',
    weights           JSONB NOT NULL DEFAULT '{}',
    reload_generation BIGINT NOT NULL DEFAULT 1,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO policy (id, preset, weights) VALUES (1, 'throughput', '{}');

-- ---------------------------------------------------------------------------
-- Onboarding (§13)
-- ---------------------------------------------------------------------------

-- Single-use. The agent enumerates its own hardware on join; nothing about the
-- machine is typed by a human.
CREATE TABLE join_tokens (
    token      TEXT PRIMARY KEY,
    pool_id    TEXT NOT NULL REFERENCES pools (pool_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    used_at    TIMESTAMPTZ,
    used_by    TEXT
);

-- ---------------------------------------------------------------------------
-- Observability
-- ---------------------------------------------------------------------------

-- Why the scheduler chose what it chose, retained per task so a demo audience
-- can be shown the reasoning rather than asked to trust it.
CREATE TABLE placement_explanations (
    task_id    TEXT PRIMARY KEY,
    payload    JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
