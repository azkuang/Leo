/**
 * Renders every UI component against live control-plane data.
 *
 * This is not a substitute for looking at the page, but it exercises the paths
 * that actually break: a component referencing a field the API does not send,
 * a map over something undefined, a bad import. Those fail here loudly rather
 * than as a blank panel during a demo.
 *
 * Usage: node render-check.mjs [server-url]
 */
import { build } from 'esbuild'
import { renderToString } from 'react-dom/server'
import React from 'react'
import { readFileSync, unlinkSync } from 'node:fs'

const SERVER = process.argv[2] || 'http://localhost:9443'

async function call(method, body = {}) {
  const res = await fetch(`${SERVER}/orch.v1.OrchService/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) throw new Error(`${method}: ${res.status} ${res.statusText}`)
  return res.json()
}

const tmp = '.render-check.bundle.mjs'

await build({
  entryPoints: ['src/render-entry.js'],
  bundle: true,
  format: 'esm',
  outfile: tmp,
  jsx: 'automatic',
  loader: { '.js': 'jsx', '.jsx': 'jsx', '.css': 'empty' },
  external: ['react', 'react-dom', 'react/jsx-runtime'],
  logLevel: 'error',
})

const mod = await import('./' + tmp)
unlinkSync(tmp)

const [nodesRes, poolsRes, jobsRes, policyRes] = await Promise.all([
  call('ListNodes'), call('ListPools'), call('ListJobs'), call('GetPolicy'),
])

const nodes = nodesRes.nodes ?? []
const pools = poolsRes.pools ?? []
const jobs = jobsRes.jobs ?? []

let job = null
let tasks = []
if (jobs.length) {
  const detail = await call('GetJob', { jobId: jobs[0].jobId })
  job = detail.job
  tasks = detail.tasks ?? []
}

let explanation = null
const placed = tasks.find((t) => t.nodeId)
if (placed) {
  try {
    explanation = (await call('Explain', { taskId: placed.taskId })).explanation
  } catch {
    // Not every task has a stored decision; the component must cope.
  }
}

const noop = () => {}
const cases = [
  ['App (initial, pre-fetch)', () => React.createElement(mod.App)],
  ['FleetGrid', () => React.createElement(mod.FleetGrid, { nodes, onTrigger: noop })],
  ['FleetGrid (empty fleet)', () => React.createElement(mod.FleetGrid, { nodes: [], onTrigger: noop })],
  ['PoolBar', () => React.createElement(mod.PoolBar, { pools, onReclaim: noop })],
  ['FrameGrid', () => React.createElement(mod.FrameGrid, { job, tasks, onSelect: noop, onCancel: noop })],
  ['FrameGrid (no job)', () => React.createElement(mod.FrameGrid, { job: null, tasks: [], onSelect: noop, onCancel: noop })],
  ['TaskDetail', () => React.createElement(mod.TaskDetail, { task: placed ?? null, explanation })],
  ['EventLog', () => React.createElement(mod.EventLog, { events: [{ kind: 'lease.granted', at: new Date().toISOString(), text: 'placed task' }] })],
  ['EventLog (empty)', () => React.createElement(mod.EventLog, { events: [] })],
  ['WeightPanel', () => React.createElement(mod.WeightPanel, {
    policy: policyRes.policy, scorers: policyRes.availableScorers ?? [],
    presets: policyRes.presets ?? [], onPreset: noop, onWeights: noop,
  })],
  ['SubmitPanel', () => React.createElement(mod.SubmitPanel, { pools, onSubmit: noop })],
]

let failed = 0
for (const [name, make] of cases) {
  try {
    const html = renderToString(make())
    if (!html || html.length < 10) throw new Error('rendered empty')
    console.log(`  ok    ${name} (${html.length} bytes)`)
  } catch (err) {
    failed++
    console.log(`  FAIL  ${name}: ${err.message}`)
  }
}

// A borrowed slot is the component that carries the system's central idea, so
// assert the rendered markup actually distinguishes it rather than trusting
// that it rendered at all.
const borrowedSlot = nodes.flatMap((n) => (n.slots ?? []).map((s) => [n, s]))
  .find(([, s]) => s.borrowed)
if (borrowedSlot) {
  const [n, s] = borrowedSlot
  const html = renderToString(React.createElement(mod.SlotTile, { slot: s, ownerPoolId: n.poolId }))
  const hatched = html.includes('repeating-linear-gradient') && html.includes('animate-drift')
  console.log(`  ${hatched ? 'ok   ' : 'FAIL '} SlotTile marks borrowed capacity with a drifting hatch`)
  if (!hatched) failed++
} else {
  console.log('  skip  SlotTile borrowed case (no borrowed slot in the fleet right now)')
}

console.log(failed === 0 ? '\nall render checks passed' : `\n${failed} render check(s) failed`)
process.exit(failed === 0 ? 0 : 1)
