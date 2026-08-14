import { useState } from 'react'
import { clock } from '../lib'

const EVENT_TONE = {
  'lease.granted': 'text-pool-1',
  'lease.ended': 'text-pool-2',
  'trigger.fired': 'text-heat-warm',
  'node.changed': 'text-pool-3',
  'job.changed': 'text-ink',
  'task.changed': 'text-muted',
  'policy.changed': 'text-pool-5',
  'pool.changed': 'text-pool-5',
}

/** The running commentary. During a demo this is what the audience reads. */
export function EventLog({ events }) {
  return (
    <div className="panel flex h-72 flex-col">
      <div className="legend border-b border-hairline px-3 py-2">activity</div>
      <div className="flex-1 overflow-y-auto px-3 py-2">
        {events.length === 0 ? (
          <p className="legend normal-case tracking-normal">Waiting for the first event.</p>
        ) : (
          <ul className="space-y-1">
            {events.map((e, i) => (
              <li key={i} className="readout flex gap-2 text-micro leading-relaxed">
                <span className="shrink-0 text-dim">{clock(e.at)}</span>
                <span className={EVENT_TONE[e.kind] ?? 'text-muted'}>{e.text || e.kind}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  )
}

/**
 * Scoring weights.
 *
 * Policy is data, not code, so these take effect on the next placement pass.
 * The controls are built from the scorer names the server reports, which means
 * a newly registered scoring dimension appears here without a UI change.
 */
export function WeightPanel({ policy, scorers, presets, onPreset, onWeights }) {
  const [draft, setDraft] = useState(null)
  const weights = draft ?? policy?.weights ?? {}

  const update = (name, value) => {
    setDraft({ ...weights, [name]: value })
  }

  return (
    <div className="panel p-3">
      <div className="flex items-baseline justify-between">
        <span className="legend">placement policy</span>
        <span className="legend">gen {policy?.reloadGeneration ?? 0}</span>
      </div>

      <div className="mt-2 flex flex-wrap gap-1">
        {presets.map((p) => (
          <button
            key={p}
            className={`btn ${policy?.preset === p ? 'border-pool-1 text-pool-1' : ''}`}
            onClick={() => {
              setDraft(null)
              onPreset(p)
            }}
          >
            {p}
          </button>
        ))}
      </div>

      <div className="mt-3 space-y-2">
        {scorers.map((name) => (
          <label key={name} className="block">
            <div className="legend flex justify-between normal-case tracking-normal">
              <span>{name.replace(/_/g, ' ')}</span>
              <span className="readout">{(weights[name] ?? 0).toFixed(2)}</span>
            </div>
            <input
              type="range"
              min="0"
              max="1"
              step="0.05"
              value={weights[name] ?? 0}
              onChange={(e) => update(name, Number(e.target.value))}
              className="mt-1 h-1 w-full cursor-pointer appearance-none rounded-full bg-raised accent-pool-1"
            />
          </label>
        ))}
      </div>

      <div className="mt-3 flex gap-1">
        <button
          className="btn"
          disabled={!draft}
          onClick={() => {
            onWeights(weights)
            setDraft(null)
          }}
        >
          apply weights
        </button>
        <button className="btn" disabled={!draft} onClick={() => setDraft(null)}>
          reset
        </button>
      </div>
    </div>
  )
}

/** Submit form. The fan-out count is what gives the scheduler something to do. */
export function SubmitPanel({ pools, onSubmit }) {
  const [form, setForm] = useState({
    name: 'animation',
    owner: 'laptop',
    poolId: '',
    imageDigest: 'sha256:blender-optix',
    count: 24,
    slots: 1,
    minVramGb: 16,
    minComputeCapability: '',
    exclusiveHost: false,
    durationMs: 8000,
  })
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)

  const set = (k) => (e) => {
    const v = e.target.type === 'checkbox' ? e.target.checked : e.target.value
    setForm({ ...form, [k]: v })
  }

  const submit = async (e) => {
    e.preventDefault()
    setBusy(true)
    setError(null)
    try {
      const tasks = Array.from({ length: Number(form.count) }, (_, i) => ({
        params: { frame: String(i + 1), sim_duration_ms: String(form.durationMs) },
      }))
      await onSubmit({
        name: form.name,
        owner: form.owner,
        poolId: form.poolId || (pools[0]?.poolId ?? 'default'),
        imageDigest: form.imageDigest,
        preemptible: true,
        tasks,
        request: {
          slots: Number(form.slots),
          minVramGb: Number(form.minVramGb),
          minComputeCapability: form.minComputeCapability || undefined,
          exclusiveHost: form.exclusiveHost,
        },
      })
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <form className="panel p-3" onSubmit={submit}>
      <div className="legend">submit a job</div>

      <div className="mt-2 grid grid-cols-2 gap-1.5">
        <label className="col-span-2">
          <span className="legend normal-case tracking-normal">name</span>
          <input className="field mt-0.5" value={form.name} onChange={set('name')} />
        </label>

        <label>
          <span className="legend normal-case tracking-normal">pool</span>
          <select className="field mt-0.5" value={form.poolId} onChange={set('poolId')}>
            {pools.map((p) => (
              <option key={p.poolId} value={p.poolId}>{p.poolId}</option>
            ))}
          </select>
        </label>

        <label>
          <span className="legend normal-case tracking-normal">tasks</span>
          <input className="field mt-0.5" type="number" min="1" max="500"
                 value={form.count} onChange={set('count')} />
        </label>

        <label>
          <span className="legend normal-case tracking-normal">slots per task</span>
          <input className="field mt-0.5" type="number" min="1" max="8"
                 value={form.slots} onChange={set('slots')} />
        </label>

        <label>
          <span className="legend normal-case tracking-normal">min vram (GB)</span>
          <input className="field mt-0.5" type="number" min="0" max="200"
                 value={form.minVramGb} onChange={set('minVramGb')} />
        </label>

        <label>
          <span className="legend normal-case tracking-normal">min compute cap</span>
          <input className="field mt-0.5" placeholder="any" value={form.minComputeCapability}
                 onChange={set('minComputeCapability')} />
        </label>

        <label>
          <span className="legend normal-case tracking-normal">seconds per task</span>
          <input className="field mt-0.5" type="number" min="1000" max="60000" step="1000"
                 value={form.durationMs} onChange={set('durationMs')} />
        </label>

        <label className="col-span-2 mt-1 flex items-center gap-2">
          <input type="checkbox" className="accent-pool-1"
                 checked={form.exclusiveHost} onChange={set('exclusiveHost')} />
          <span className="legend normal-case tracking-normal">
            reserve the whole machine
          </span>
        </label>
      </div>

      {error && <p className="readout mt-2 text-micro text-heat-hot">{error}</p>}

      <button className="btn mt-3 w-full" disabled={busy}>
        {busy ? 'submitting…' : 'submit'}
      </button>
    </form>
  )
}
