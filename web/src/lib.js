// Pool hues are assigned by first appearance and then held stable, so a pool
// keeps its colour for the whole session. Colour is identity here, not
// decoration: the fleet view is unreadable if `cad` is amber in one panel and
// violet in another.
const POOL_HUES = ['#3ad4e0', '#f5a83c', '#a98bff', '#ff7a8a', '#a6d95b']
const assigned = new Map()

export function poolColor(poolId) {
  if (!poolId) return '#5b6491'
  if (!assigned.has(poolId)) {
    assigned.set(poolId, POOL_HUES[assigned.size % POOL_HUES.length])
  }
  return assigned.get(poolId)
}

/**
 * Maps a temperature onto the heat ramp.
 *
 * Deliberately not a smooth gradient across the whole range: an operator needs
 * to see "this one is in trouble" without reading a number, so the hot end
 * arrives abruptly rather than fading in.
 */
export function heatColor(tempC) {
  if (!tempC) return '#3b4472'
  if (tempC >= 84) return '#ff5d5d'
  if (tempC >= 72) return '#f5a83c'
  return '#4fb8f5'
}

export function isThrottled(health) {
  const reasons = health?.throttleReasons ?? []
  return reasons.some((r) => r && r !== 'none' && r !== 'gpu_idle')
}

export function isDegraded(health) {
  return (health?.xidErrors?.length ?? 0) > 0 || Number(health?.rowRemapPending ?? 0) > 0
}

export function shortId(id, n = 10) {
  if (!id) return '—'
  return id.length > n ? id.slice(0, n) : id
}

export function taskState(state) {
  return (state || '').replace('TASK_STATE_', '').toLowerCase()
}

export function clock(iso) {
  if (!iso) return ''
  return new Date(iso).toLocaleTimeString([], {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  })
}
