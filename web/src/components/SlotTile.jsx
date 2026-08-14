import { poolColor, heatColor, isThrottled, isDegraded, shortId } from '../lib'

/**
 * One GPU slot.
 *
 * This tile carries the central fact of the whole system: a machine is owned by
 * one pool but may be occupied by another. The left edge is the owner. The
 * drifting hatch is the borrower. When an owner reclaims its machine the hatch
 * stops and disappears, which is the reclaim made visible without a word of
 * explanation.
 */
export default function SlotTile({ slot, ownerPoolId }) {
  const owner = poolColor(ownerPoolId)
  const borrower = slot.borrowed ? poolColor(slot.poolId) : null

  const health = slot.health ?? {}
  const temp = health.temperatureC ?? 0
  const util = health.utilization ?? 0
  const throttled = isThrottled(health)
  const degraded = isDegraded(health)

  const allocated = slot.state === 'allocated'
  const unhealthy = slot.state === 'unhealthy' || degraded

  return (
    <div
      className="relative overflow-hidden rounded-sm border bg-ground/60 px-2.5 py-2"
      style={{
        borderColor: unhealthy ? '#ff5d5d' : allocated ? owner : '#2e3763',
        borderLeftWidth: '3px',
        borderLeftColor: unhealthy ? '#ff5d5d' : owner,
      }}
      title={
        `${slot.slotId} · ${slot.vramGb}GB · cc ${slot.computeCapability}` +
        (slot.borrowed ? ` · borrowed by ${slot.poolId}` : '') +
        (throttled ? ` · throttled: ${(health.throttleReasons || []).join(', ')}` : '')
      }
    >
      {borrower && (
        <div
          aria-hidden
          className="pointer-events-none absolute inset-0 animate-drift opacity-30"
          style={{
            backgroundImage:
              `repeating-linear-gradient(115deg, ${borrower} 0 2px, transparent 2px 14px)`,
            backgroundSize: '28px 100%',
          }}
        />
      )}

      <div className="relative flex items-baseline justify-between">
        <span className="readout text-tiny text-ink">d{slot.deviceIndex}</span>
        <span className="readout text-micro text-muted">
          {slot.vramGb}G · {slot.computeCapability}
        </span>
      </div>

      {/* Utilisation, with heat as the fill colour: one bar answers both
          "is it working" and "is it coping". */}
      <div className="relative mt-1.5 h-1 w-full rounded-full bg-raised">
        <div
          className="h-1 rounded-full transition-all duration-500"
          style={{ width: `${Math.round(util * 100)}%`, background: heatColor(temp) }}
        />
      </div>

      <div className="relative mt-1.5 flex items-center justify-between">
        <span className="readout text-micro" style={{ color: heatColor(temp) }}>
          {temp ? `${Math.round(temp)}°` : '—'}
          {throttled && <span className="ml-1 animate-pulse-ring">▲</span>}
        </span>

        {degraded ? (
          <span className="readout text-micro text-heat-hot">XID</span>
        ) : slot.borrowed ? (
          <span className="readout text-micro" style={{ color: borrower }}>
            ↗{slot.poolId}
          </span>
        ) : allocated ? (
          <span className="readout text-micro text-muted">{shortId(slot.taskId, 9)}</span>
        ) : (
          <span className="readout text-micro text-dim">free</span>
        )}
      </div>
    </div>
  )
}
