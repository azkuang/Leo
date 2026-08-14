import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { api, subscribe } from './api'
import FleetGrid from './components/FleetGrid'
import PoolBar from './components/PoolBar'
import FrameGrid, { TaskDetail } from './components/FrameGrid'
import { EventLog, WeightPanel, SubmitPanel } from './components/Console'

export default function App() {
  const [nodes, setNodes] = useState([])
  const [pools, setPools] = useState([])
  const [jobs, setJobs] = useState([])
  const [policy, setPolicy] = useState(null)
  const [scorers, setScorers] = useState([])
  const [presets, setPresets] = useState([])

  const [selectedJobId, setSelectedJobId] = useState(null)
  const [jobDetail, setJobDetail] = useState(null)
  const [selectedTask, setSelectedTask] = useState(null)
  const [explanation, setExplanation] = useState(null)

  const [events, setEvents] = useState([])
  const [error, setError] = useState(null)

  // Held in a ref so the SSE subscription, which is set up once, always reads
  // the current selection rather than the one captured at mount.
  const selectedJobRef = useRef(null)
  selectedJobRef.current = selectedJobId

  const refresh = useCallback(async () => {
    try {
      const [n, p, j, pol] = await Promise.all([
        api.listNodes(), api.listPools(), api.listJobs(), api.getPolicy(),
      ])
      setNodes(n.nodes ?? [])
      setPools(p.pools ?? [])
      setJobs(j.jobs ?? [])
      setPolicy(pol.policy ?? null)
      setScorers(pol.availableScorers ?? [])
      setPresets(pol.presets ?? [])
      setError(null)
    } catch (e) {
      setError(e.message)
    }
  }, [])

  const refreshJob = useCallback(async (jobId) => {
    if (!jobId) return
    try {
      const res = await api.getJob(jobId)
      setJobDetail(res)
    } catch {
      setJobDetail(null)
    }
  }, [])

  useEffect(() => {
    refresh()
    // Events drive the interesting updates; this poll is the backstop that
    // reconciles anything a dropped frame would have missed.
    const poll = setInterval(() => {
      refresh()
      refreshJob(selectedJobRef.current)
    }, 2000)

    const unsubscribe = subscribe((e) => {
      setEvents((prev) => [e, ...prev].slice(0, 120))
    })

    return () => {
      clearInterval(poll)
      unsubscribe()
    }
  }, [refresh, refreshJob])

  useEffect(() => {
    setSelectedTask(null)
    setExplanation(null)
    refreshJob(selectedJobId)
  }, [selectedJobId, refreshJob])

  // Default to the newest job so the frame grid is never empty for no reason.
  useEffect(() => {
    if (!selectedJobId && jobs.length > 0) setSelectedJobId(jobs[0].jobId)
  }, [jobs, selectedJobId])

  const onSelectTask = async (task) => {
    setSelectedTask(task)
    setExplanation(null)
    try {
      const res = await api.explain(task.taskId)
      setExplanation(res.explanation ?? null)
    } catch {
      setExplanation(null)
    }
  }

  const fire = async (kind, nodeId, poolId) => {
    try {
      await api.trigger({ kind, nodeId, poolId, wholeMachine: true })
      refresh()
    } catch (e) {
      setError(e.message)
    }
  }

  const summary = useMemo(() => {
    let total = 0, used = 0, borrowed = 0, simulated = 0, ready = 0
    for (const n of nodes) {
      if (n.simulated) simulated++
      if (n.state === 'ready') ready++
      for (const s of n.slots ?? []) {
        total++
        if (s.state === 'allocated') used++
        if (s.borrowed) borrowed++
      }
    }
    return { total, used, borrowed, simulated, ready, nodes: nodes.length }
  }, [nodes])

  return (
    <div className="min-h-screen">
      <header className="sticky top-0 z-10 border-b border-hairline bg-ground/95 backdrop-blur">
        <div className="mx-auto flex max-w-[1800px] flex-wrap items-center gap-x-6 gap-y-2 px-4 py-2.5">
          <div>
            <h1 className="readout text-sm tracking-wide">orch</h1>
            <p className="legend">gpu compute-sharing</p>
          </div>

          <Stat label="utilisation" value={`${summary.used}/${summary.total}`} sub="slots in use" />
          <Stat label="borrowed" value={summary.borrowed} sub="slots on other pools' machines"
                tone={summary.borrowed > 0 ? 'text-pool-2' : undefined} />
          <Stat label="nodes" value={`${summary.ready}/${summary.nodes}`} sub="ready" />

          {/* Stated up front, not discovered later. */}
          <Stat label="fleet" value={`${summary.simulated} sim · ${summary.nodes - summary.simulated} real`}
                sub="what you are looking at" />

          <div className="ml-auto flex items-center gap-2">
            {error && <span className="readout text-micro text-heat-hot">{error}</span>}
            <span className="legend">{policy?.preset ?? '—'}</span>
          </div>
        </div>
      </header>

      <main className="mx-auto grid max-w-[1800px] gap-3 px-4 py-3 lg:grid-cols-[minmax(0,1fr)_340px]">
        <div className="space-y-3">
          <section>
            <h2 className="legend mb-1.5">fleet</h2>
            <FleetGrid nodes={nodes} onTrigger={(kind, nodeId) => fire(kind, nodeId, '')} />
          </section>

          <section>
            <div className="mb-1.5 flex items-center gap-2">
              <h2 className="legend">job</h2>
              <select
                className="field w-auto py-0.5"
                value={selectedJobId ?? ''}
                onChange={(e) => setSelectedJobId(e.target.value)}
              >
                {jobs.length === 0 && <option value="">no jobs yet</option>}
                {jobs.map((j) => (
                  <option key={j.jobId} value={j.jobId}>
                    {j.name || j.jobId} — {j.state}
                  </option>
                ))}
              </select>
            </div>

            <FrameGrid
              job={jobDetail?.job}
              tasks={jobDetail?.tasks ?? []}
              onSelect={onSelectTask}
              onCancel={async (id) => {
                await api.cancelJob(id)
                refresh()
              }}
            />
            <TaskDetail task={selectedTask} explanation={explanation} />
          </section>
        </div>

        <div className="space-y-3">
          <section className="panel p-3">
            <h2 className="legend mb-2">pools</h2>
            <PoolBar
              pools={pools}
              onReclaim={(poolId) => fire('TRIGGER_KIND_RECLAIM', '', poolId)}
            />
          </section>

          <SubmitPanel
            pools={pools}
            onSubmit={async (spec) => {
              const res = await api.submitJob(spec)
              setSelectedJobId(res.jobId)
              refresh()
            }}
          />

          <EventLog events={events} />

          <WeightPanel
            policy={policy}
            scorers={scorers}
            presets={presets}
            onPreset={async (preset) => {
              await api.setPolicy(preset, {})
              refresh()
            }}
            onWeights={async (weights) => {
              await api.setPolicy(policy?.preset ?? 'throughput', weights)
              refresh()
            }}
          />
        </div>
      </main>
    </div>
  )
}

function Stat({ label, value, sub, tone }) {
  return (
    <div className="min-w-[7rem]">
      <div className={`readout text-lg leading-none ${tone ?? 'text-ink'}`}>{value}</div>
      <div className="legend mt-1">{label}</div>
      {sub && <div className="legend normal-case tracking-normal text-dim">{sub}</div>}
    </div>
  )
}
