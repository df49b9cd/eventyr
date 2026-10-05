-- Stream lifecycle (0.7.6): closing a stream to appends, and
-- truncating the oldest part of its history.
--
-- A row exists only for streams that were closed or truncated. `head`
-- keeps a truncated stream's version when its rows are gone, so the
-- next append continues from it; `first_kept` is the first version
-- left, below which reads fail with StoreError::Truncated.
CREATE TABLE stream_lifecycle (
    stream_id   TEXT    PRIMARY KEY,
    closed      BOOLEAN NOT NULL DEFAULT FALSE,
    first_kept  BIGINT  NOT NULL DEFAULT 1 CHECK (first_kept >= 1),
    head        BIGINT  NOT NULL DEFAULT 0 CHECK (head >= 0)
);

CREATE OR REPLACE FUNCTION append_events(
    expected_kind SMALLINT,          -- 0=Any, 1=Empty, 2=Exact
    expected_version BIGINT,         -- only for 2
    stream_id TEXT,
    event_type TEXT[],
    payload JSONB[],
    causation_id TEXT[],
    correlation_id TEXT[],
    idempotency_key TEXT[]
)
RETURNS SETOF events AS $$
#variable_conflict use_column
DECLARE
    current_version BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(append_events.stream_id));

    -- Closed streams refuse every append (0.7.6). SQLSTATE EV001 is
    -- what the store maps to StoreError::StreamClosed.
    IF EXISTS (SELECT 1 FROM stream_lifecycle AS l
               WHERE l.stream_id = append_events.stream_id AND l.closed) THEN
        RAISE EXCEPTION 'stream % is closed', append_events.stream_id
        USING ERRCODE = 'EV001', HINT = append_events.stream_id;
    END IF;

    -- The head: the newest row, or the recorded head of a stream
    -- truncated to nothing.
    SELECT GREATEST(
        COALESCE(MAX(e.stream_version), 0),
        COALESCE((SELECT l.head FROM stream_lifecycle AS l
                  WHERE l.stream_id = append_events.stream_id), 0)
    ) INTO current_version
    FROM events AS e
    WHERE e.stream_id = append_events.stream_id;

    IF (expected_kind = 1 AND current_version <> 0) OR
       (expected_kind = 2 AND current_version <> expected_version) THEN
        RAISE EXCEPTION 'version conflict on stream %: expected % but stream is at %',
            stream_id, expected_version, current_version
        USING HINT = append_events.stream_id || ':' || current_version::text;
    END IF;

    -- The commit-order lock: held from the sequence draw to commit.
    PERFORM pg_advisory_xact_lock(7300160413598463541);

    IF COALESCE(array_length(payload, 1), 0) > 0 THEN
        PERFORM pg_notify('eventyr_commits', '');
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
            'correlation_id', correlation_id[n.ord],
            'idempotency_key', idempotency_key[n.ord]
        )), '{}')
    FROM generate_series(1, COALESCE(array_length(payload, 1), 0)) AS n(ord)
    RETURNING events.*;
END;
$$ LANGUAGE plpgsql;
