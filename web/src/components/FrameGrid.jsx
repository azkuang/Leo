import { taskState, shortId } from '../lib'

// One cell per task. The orchestrator does not know these are frames; the job
// that submitted them decided what its fan-out meant.
const STATE_STYLE = {
  done: 'bg-pool-5/80 border-pool-5',
  running: 'bg-pool-1/70 border-pool-1',
  staging: 'bg-pool-1/25 border-pool-1/50',
  assigned: 'bg-pool-1/20 border-pool-1/40',
  queued: 'bg-raised border-hairline',
  failed: 'bg-heat-hot/70 border-heat-hot',
  preempted: 'bg-pool-2/60 border-pool-2',
}

export default function FrameGrid({ job, tasks, onSelect, onCancel }) {
  if (!job) {
    return (
      <div className="panel p-6 text-center">
        <p className="readout text-sm text-muted">No job selected.</p>
        <p className="legend mt-2 normal-case tracking-normal">
          Submit one, or pick a job from the list.
        </p>
      </div>
    )
  }

  const done = job.tasksDone ?? 0
  const total = job.tasksTotal ?? 0
  const requeued = tasks.filter((t) => (t.preemptions ?? 0) > 0).length

  return (
    <div className="panel p-3">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <div>
          <span className="readout text-sm">{job.name || job.jobId}</span>
          <span className="legend ml-2">
            {job.poolId} · {job.state}
          </span>
        </div>
        <div className="flex items-center gap-3">
          <span className="readout text-tiny text-muted">
            {done}/{total} done
            {requeued > 0 && (
              <span className="ml-2 text-pool-2">{requeued} requeued after preemption</span>
            )}
          </span>
          {job.state === 'running' || job.state === 'pending' ? (
            <button className="btn btn-alarm" onClick={() => onCancel(job.jobId)}>
              cancel
            </button>
          ) : null}
        </div>
      </div>

      <div className="mt-3 flex flex-wrap gap-1">
        {tasks.map((t) => {
          const state = taskState(t.state)
          return (
            <button
              key={t.taskId}
              onClick={() => onSelect(t)}
              title={`#${t.index} ${state}${t.nodeId ? ` on ${t.nodeId}` : ''}` +
                     `${t.preemptions ? ` · preempted ${t.preemptions}×` : ''}` +
                     ` · attempt ${t.attempt ?? 0}`}
              className={`h-6 w-6 rounded-[2px] border transition-transform hover:scale-110 ${
                STATE_STYLE[state] ?? STATE_STYLE.queued
              }`}
            >
              <span className="sr-only">
                task {t.index} {state}
              </span>
            </button>
          )
        })}
      </div>

      <div className="legend mt-3 flex flex-wrap gap-x-3 gap-y-1 border-t border-hairline pt-2">
        {['queued', 'running', 'done', 'preempted', 'failed'].map((s) => (
          <span key={s} className="flex items-center gap-1">
            <span className={`inline-block h-2 w-2 rounded-[1px] border ${STATE_STYLE[s]}`} />
            {s}
          </span>
        ))}
      </div>
    </div>
  )
}

export function TaskDetail({ task, explanation }) {
  if (!task) return null

  const scorers = new Set()
  for (const c of explanation?.candidates ?? []) {
    Object.keys(c.perScorer ?? {}).forEach((k) => scorers.add(k))
  }
  const names = [...scorers].sort()

  return (
    <div className="panel mt-2.5 p-3">
      <div className="flex items-baseline justify-between">
        <span className="readout text-tiny">{shortId(task.taskId, 14)}</span>
        <span className="legend">
          attempt {task.attempt ?? 0} · {taskState(task.state)}
          {task.preemptions ? ` · preempted ${task.preemptions}×` : ''}
        </span>
      </div>

      {!explanation ? (
        <p className="legend mt-2 normal-case tracking-normal">
          No placement record for this task yet.
        </p>
      ) : (
        <>
          <p className="legend mt-2 normal-case tracking-normal">
            Chose {explanation.chosenNodeId} from {explanation.candidates?.length ?? 0} candidate(s).
          </p>

          <div className="mt-2 overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="legend">
                  <th className="pb-1 pr-3 font-normal">node</th>
                  <th className="pb-1 pr-3 font-normal">total</th>
                  {names.map((n) => (
                    <th key={n} className="pb-1 pr-3 font-normal">{n.replace(/_/g, ' ')}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="readout text-micro">
                {(explanation.candidates ?? []).map((c) => (
                  <tr key={c.nodeId} className={c.chosen ? 'text-pool-1' : 'text-muted'}>
                    <td className="py-0.5 pr-3">
                      {c.chosen ? '▸ ' : '  '}
                      {shortId(c.nodeId, 14)}
                      {c.wouldBorrow && <span className="ml-1 text-pool-2">↗</span>}
                    </td>
                    <td className="py-0.5 pr-3">{(c.total ?? 0).toFixed(3)}</td>
                    {names.map((n) => (
                      <td key={n} className="py-0.5 pr-3">
                        {(c.perScorer?.[n] ?? 0).toFixed(2)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {(explanation.filteredOut ?? []).length > 0 && (
            <details className="mt-2">
              <summary className="legend cursor-pointer normal-case tracking-normal">
                {explanation.filteredOut.length} node(s) did not fit
              </summary>
              <ul className="readout mt-1 space-y-0.5 text-micro text-dim">
                {explanation.filteredOut.map((f, i) => (
                  <li key={i}>{f}</li>
                ))}
              </ul>
            </details>
          )}
        </>
      )}
    </div>
  )
}
