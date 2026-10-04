-- The read-model rows: one per (view, view_id), newest-wins on the
-- folded sequence (DESIGN §13's 0.6.3; the same contract 0002's
-- snapshots carry). One table serves every view type; the name scopes
-- it, exactly as the projection's checkpoint key does.

CREATE TABLE views (
    view_name   TEXT        NOT NULL,
    view_id     TEXT        NOT NULL,
    version     BIGINT      NOT NULL CHECK (version >= 0),
    payload     JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (view_name, view_id)
);

-- "version" is the event's global sequence — the replay guard:
-- replayed or out-of-order saves affect nothing older than the row.
