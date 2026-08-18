-- exit_code is reported by the agent on every terminal status update but was
-- previously dropped on the floor: the terminal UPDATE in
-- pgstore/work.go's ApplyTaskStatus wrote state/message/output_prefix/
-- finished_at only. Nullable, not defaulted to zero: "not yet finished" and
-- "exited 0" are different facts. No CHECK constraint on task state exists
-- in 0001_init.sql, so this is a clean additive migration.
ALTER TABLE tasks ADD COLUMN exit_code INT;
