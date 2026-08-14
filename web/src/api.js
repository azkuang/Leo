// Thin client over the ConnectRPC HTTP/JSON endpoints.
//
// Connect exposes every unary method as POST /<package>.<Service>/<Method>
// with a JSON body, so the UI needs no generated stubs and no runtime.

const BASE = ''

async function call(method, body = {}) {
  const res = await fetch(`${BASE}/orch.v1.OrchService/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    let detail = res.statusText
    try {
      const err = await res.json()
      detail = err.message || detail
    } catch {
      // The body was not JSON; the status line is all we have.
    }
    throw new Error(detail)
  }
  return res.json()
}

export const api = {
  listNodes: () => call('ListNodes'),
  listPools: () => call('ListPools'),
  listJobs: () => call('ListJobs'),
  getJob: (jobId) => call('GetJob', { jobId }),
  cancelJob: (jobId) => call('CancelJob', { jobId }),
  listLeases: () => call('ListLeases'),
  getPolicy: () => call('GetPolicy'),
  setPolicy: (preset, weights) => call('SetPolicy', { policy: { preset, weights } }),
  submitJob: (spec) => call('SubmitJob', { spec }),
  trigger: (req) => call('Trigger', req),
  explain: (taskId) => call('Explain', { taskId }),
}

/**
 * Subscribes to the control-plane event stream.
 *
 * EventSource reconnects on its own, which is the reason SSE was chosen over a
 * WebSocket here: nothing flows upstream, and a dropped connection during a
 * demo repairs itself without any code.
 */
export function subscribe(onEvent) {
  const source = new EventSource(`${BASE}/events`)

  const handle = (e) => {
    try {
      onEvent(JSON.parse(e.data))
    } catch {
      // A malformed frame is not worth tearing the stream down for.
    }
  }

  for (const kind of [
    'node.changed', 'slot.changed', 'lease.granted', 'lease.ended',
    'task.changed', 'job.changed', 'pool.changed', 'policy.changed',
    'placement.decided', 'trigger.fired', 'scheduler.log',
  ]) {
    source.addEventListener(kind, handle)
  }

  return () => source.close()
}
