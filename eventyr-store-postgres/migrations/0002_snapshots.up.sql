-- Snapshots: one row per stream, the newest snapshot wins
-- (`ON CONFLICT (stream_id) DO UPDATE` handles replacement in one
-- statement). The `events` table remains the only system of record --
-- snapshots are read-side shortcuts, so a dropped snapshot row costs a
-- longer delta fold, nothing more. Certainty is enforced by the schema:
-- `version` is CHECK-positive and UNIQUE per stream via the PK.

CREATE TABLE snapshots (
    stream_id   TEXT        PRIMARY KEY,
    version     BIGINT      NOT NULL CHECK (version > 0),
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Snapshots: persisted write-side read models.
CREATE TABLE snapshots (
    stream_id  TEXT PRIMARY KEY,
    version    BIGINT NOT NULL CHECK (version > 0),
    payload    JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
