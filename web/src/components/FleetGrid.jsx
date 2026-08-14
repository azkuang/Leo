import SlotTile from './SlotTile'
import { poolColor } from '../lib'

function NodeCard({ node, onTrigger }) {
  const owner = poolColor(node.poolId)
  const slots = node.slots ?? []
  const free = slots.filter((s) => s.state === 'free').length
  const lent = slots.filter((s) => s.borrowed).length

  const offline = node.state !== 'ready'

  return (
    <div className={`panel p-3 ${offline ? 'opacity-55' : ''}`}>
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <span
              className="inline-block h-2 w-2 shrink-0 rounded-full"
              style={{ background: owner }}
              aria-hidden
            />
            <span className="readout truncate text-sm text-ink">{node.hostname}</span>

            {/* Simulated nodes are labelled wherever they appear. Disclosing
                "8 simulated, 1 real" up front is stronger than letting an
                audience discover it later. */}
            {node.simulated && (
              <span className="legend rounded-sm border border-hairline px-1 py-px text-dim">
                sim
              </span>
            )}
          </div>

          <div className="legend mt-1 flex flex-wrap items-center gap-x-2 gap-y-0.5">
            <span style={{ color: owner }}>{node.poolId}</span>
            <span>·</span>
            <span className={offline ? 'text-heat-hot' : ''}>{node.state}</span>
            <span>·</span>
            <span>{free}/{slots.length} free</span>
            {lent > 0 && (
              <>
                <span>·</span>
                <span className="text-pool-2">{lent} lent</span>
              </>
            )}
            {!node.lendable && (
              <>
                <span>·</span>
                <span className="text-heat-hot">closed to borrowers</span>
              </>
            )}
          </div>
        </div>

        <div className="flex shrink-0 gap-1">
          <button
            className="btn btn-alarm px-2 py-1"
            onClick={() => onTrigger('TRIGGER_KIND_THERMAL_THROTTLE', node.nodeId)}
            title="Make this node report thermal throttling"
          >
            heat
          </button>
          <button
            className="btn btn-alarm px-2 py-1"
            onClick={() => onTrigger('TRIGGER_KIND_NODE_FAILURE', node.nodeId)}
            title="Drop this node out of the fleet"
          >
            fail
          </button>
          <button
            className="btn px-2 py-1"
            onClick={() => onTrigger('TRIGGER_KIND_NODE_RECOVER', node.nodeId)}
            title="Bring the node back and reopen it to borrowers"
          >
            heal
          </button>
        </div>
      </div>

      <div className="mt-2.5 grid grid-cols-2 gap-1.5">
        {slots.map((s) => (
          <SlotTile key={s.slotId} slot={s} ownerPoolId={node.poolId} />
        ))}
      </div>

      <div className="legend mt-2 flex justify-between border-t border-hairline pt-1.5">
        <span>
          {node.capabilities?.software?.driver || '—'} · cuda{' '}
          {node.capabilities?.software?.cuda || '—'}
        </span>
        <span>
          {Math.round(node.allocatedCores ?? 0)}/{node.hostCores ?? 0} cores
        </span>
      </div>
    </div>
  )
}

export default function FleetGrid({ nodes, onTrigger }) {
  if (!nodes.length) {
    return (
      <div className="panel p-6 text-center">
        <p className="readout text-sm text-muted">No nodes have joined yet.</p>
        <p className="legend mt-2 normal-case tracking-normal">
          Start one: orchd-agent --server http://localhost:9443 --simulated
        </p>
      </div>
    )
  }

  return (
    <div className="grid gap-2.5 sm:grid-cols-2 xl:grid-cols-3">
      {nodes.map((n) => (
        <NodeCard key={n.nodeId} node={n} onTrigger={onTrigger} />
      ))}
    </div>
  )
}
