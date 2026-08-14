#!/usr/bin/env bash
#
# The §14 demo, scripted end to end.
#
# One continuous story: borrowing raises utilisation, reclaim protects the
# owner, the borrower degrades instead of failing.
#
#   scripts/demo.sh            run it
#   scripts/demo.sh --fast     shorter tasks, for a dry run
#
# Requires a running control plane and the three simulated nodes. Start them
# with scripts/dev-up.sh.

set -euo pipefail

SERVER="${ORCH_SERVER:-http://localhost:9443}"
ORCHCTL="${ORCHCTL:-./bin/orchctl}"
DURATION=9s
FRAMES=24

if [[ "${1:-}" == "--fast" ]]; then
  DURATION=4s
  FRAMES=16
fi

ctl() { "$ORCHCTL" --server "$SERVER" "$@"; }

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
pause() { sleep "${1:-4}"; }

# ---------------------------------------------------------------------------

say "The fleet"
note "Simulated nodes are labelled. Nothing here pretends to be hardware it is not."
ctl fleet

say "Pools"
note "CAD owns two workstations and lends idle capacity but never borrows."
note "Render owns one modest box and may borrow up to four slots."
ctl bootstrap deploy/fleet.yaml
ctl pools

say "A laptop submits a render it cannot reasonably do locally"
note "$FRAMES frames, one task each, ~${DURATION} of work per frame."
ctl submit \
  --name animation --owner laptop --pool render \
  --image sha256:blender-optix \
  --count "$FRAMES" --param frame --sim-duration "$DURATION" \
  --min-vram-gb 16
pause 5

say "The scheduler borrows idle CAD capacity"
note "Render's own two slots were not enough, so it reached into the CAD pool."
ctl fleet
ctl pools

# Matching on ID prefixes rather than row numbers, so a table header or an
# extra column never turns into a lookup for a task called "TASK".
latest_job() { ctl jobs | awk '$1 ~ /^job-/ {print $1; exit}'; }
placed_task() { ctl job "$1" | awk '$2 ~ /^task-/ && $6 ~ /^node-/ {print $2; exit}'; }

say "Why did it choose those machines?"
JOB=$(latest_job)
TASK=$(placed_task "$JOB")
if [[ -n "${TASK:-}" ]]; then
  ctl explain "$TASK" || note "(no placement record retained for $TASK)"
else
  note "(nothing placed yet)"
fi
pause 6

say "Frames are completing"
ctl jobs

say "TRIGGER: the CAD user logs in"
note "The owning pool wants its machines back, intact."
BEFORE_DONE=$(ctl jobs | awk '$1 ~ /^job-/ {print $6; exit}')
ctl trigger reclaim --pool cad
pause 3

say "Reclaim done"
note "CAD machines are closed to borrowers and emptied. Native work was untouched."
ctl fleet
ctl pools

say "The render keeps going, slower"
note "Completed frames were not lost; preempted ones requeued as a new attempt."
note "Frames finished before the reclaim: ${BEFORE_DONE:-0}"
pause 8
ctl jobs

say "Preempted frames landed elsewhere and finished"
note "PREEMPTIONS > 0 marks a frame that was interrupted and rerun."
ctl job "$JOB" | awk 'NR<=3 || ($2 ~ /^task-/ && $5 > 0)'

say "Done"
note "Watch it live at $SERVER, or follow the stream with: orchctl watch"
