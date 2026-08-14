import { poolColor } from '../lib'

/**
 * Pool accounting.
 *
 * The bar is split into native use and borrowed use, because "how full is this
 * pool" and "how much of that is someone else's hardware" are different
 * questions and a single fill would conflate them.
 */
export default function PoolBar({ pools, onReclaim }) {
  return (
    <div className="space-y-2.5">
      {pools.map((p) => {
        const color = poolColor(p.poolId)
        const total = p.totalSlots ?? 0
        const used = p.usedSlots ?? 0
        const borrowed = p.borrowedSlots ?? 0
        const lent = p.lentSlots ?? 0
        const native = Math.max(0, used - borrowed)
        const denom = Math.max(total, used, 1)

        return (
          <div key={p.poolId}>
            <div className="flex items-baseline justify-between">
              <span className="readout text-tiny" style={{ color }}>
                {p.poolId}
              </span>
              <span className="legend">
                {used}/{total} slots
              </span>
            </div>

            <div className="mt-1 flex h-1.5 w-full overflow-hidden rounded-full bg-raised">
              <div
                style={{ width: `${(native / denom) * 100}%`, background: color }}
                title={`${native} slot(s) on its own machines`}
              />
              <div
                className="animate-drift"
                style={{
                  width: `${(borrowed / denom) * 100}%`,
                  backgroundImage:
                    `repeating-linear-gradient(115deg, ${color} 0 3px, transparent 3px 8px)`,
                  backgroundSize: '28px 100%',
                }}
                title={`${borrowed} slot(s) borrowed from other pools`}
              />
            </div>

            <div className="legend mt-1 flex items-center justify-between">
              <span>
                ceiling {p.borrowCeiling ?? 0}
                {borrowed > 0 && <span className="text-pool-2"> · borrowing {borrowed}</span>}
                {lent > 0 && <span className="text-pool-2"> · lending {lent}</span>}
              </span>

              {lent > 0 && (
                <button
                  className="btn btn-alarm px-1.5 py-0.5"
                  onClick={() => onReclaim(p.poolId)}
                  title={`Take back ${lent} slot(s) other pools are using`}
                >
                  reclaim
                </button>
              )}
            </div>
          </div>
        )
      })}
    </div>
  )
}
