-- Subscription checkpoints: each subscription's last-acked global
-- sequence, keyed by its name, so a projector restarted against this
-- database resumes where it stopped instead of replaying from the
-- origin. One upsert per ack; the last write wins (the runner only moves
-- a name forward, an operator may move it back).
CREATE TABLE checkpoints (
    name            TEXT        PRIMARY KEY,
    global_sequence BIGINT      NOT NULL CHECK (global_sequence >= 0),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
