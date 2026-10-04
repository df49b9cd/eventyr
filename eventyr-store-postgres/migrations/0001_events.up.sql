-- Single-table, stream-scoped optimistic locking (DESIGN §9).
-- The UNIQUE(stream_id, stream_version) backing index serves the
-- per-stream reads (stream ids first, versions ordered); the global
-- sequence's PRIMARY KEY index serves stream_all's keyset pagination.

CREATE TABLE events (
    global_sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream_id       TEXT        NOT NULL,
    stream_version  BIGINT      NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    metadata        JSONB       NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream_id, stream_version)
);

-- Append a whole batch transactionally, guarded by the expected
-- stream version.
--
-- The per-stream lock (`pg_advisory_xact_lock(hashtext(stream_id))`)
-- serializes writers to one stream — different streams never contend —
-- replacing the naive FOR UPDATE on zero-to-many rows (which cannot
-- lock a stream that doesn't exist yet).
--
-- Returns one row per inserted event, its server-assigned columns
-- (sequence, created_at) already populated. On an expectation mismatch
-- it raises an error whose `hint` carries the conflict's current
-- version; the store maps it to `StoreError::Conflict`.
CREATE OR REPLACE FUNCTION append_events(
    expected_kind SMALLINT,          -- 0=Any, 1=Empty, 2=Exact
    expected_version BIGINT,         -- only for 2
    stream_id TEXT,
    event_type TEXT[],
    payload JSONB[],
    causation_id TEXT[],
    correlation_id TEXT[]
)
RETURNS SETOF events AS $$
#variable_conflict use_column
DECLARE
    current_version BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(append_events.stream_id));

    SELECT COALESCE(MAX(e.stream_version), 0) INTO current_version
    FROM events AS e
    WHERE e.stream_id = append_events.stream_id;

    IF (expected_kind = 1 AND current_version <> 0) OR
       (expected_kind = 2 AND current_version <> expected_version) THEN
        RAISE EXCEPTION 'version conflict on stream %: expected % but stream is at %',
            stream_id, expected_version, current_version
        USING HINT = current_version::text;
    END IF;

    RETURN QUERY
    INSERT INTO events (stream_id, stream_version, event_type, payload, metadata)
    SELECT
        append_events.stream_id,
        current_version + n.ord,
        event_type[n.ord],
        payload[n.ord],
        COALESCE(jsonb_strip_nulls(jsonb_build_object(
            'causation_id', causation_id[n.ord],
            'correlation_id', correlation_id[n.ord]
        )), '{}')
    FROM generate_series(1, COALESCE(array_length(payload, 1), 0)) AS n(ord)
    RETURNING events.*;
END;
$$ LANGUAGE plpgsql;
