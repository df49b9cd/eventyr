-- Split `append_events` into small named steps.
--
-- Until now every migration that changed one rule of the append
-- (0004, 0006-0009) re-created the whole function, so the body exists in
-- nine copies across the up and down files, and the commit-order lock's
-- key was written into each of them and into the Rust store. From here
-- each rule is its own function: a later migration replaces the one it
-- changes, and the store calls the same functions instead of repeating
-- their keys. Behaviour is unchanged; the steps run in the same order
-- under the same locks.

-- The per-stream lock (0001): serializes writers to one stream, held
-- until the transaction ends. Also taken by close, truncate and every
-- batch, sorted, before their first append.
CREATE FUNCTION eventyr_lock_stream(stream TEXT) RETURNS void AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(stream));
END;
$$ LANGUAGE plpgsql;

-- The commit-order lock (0006): held from drawing a global sequence
-- until commit, so sequences become visible in order. The key is an
-- arbitrary constant in the single-key advisory space; this function is
-- its one definition.
CREATE FUNCTION eventyr_commit_order_lock() RETURNS void AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(7300160413598463541);
END;
$$ LANGUAGE plpgsql;

-- Closed streams refuse every append (0009). SQLSTATE EV001 is what the
-- store maps to StoreError::StreamClosed; the hint is the stream id.
CREATE FUNCTION eventyr_check_open(stream TEXT) RETURNS void AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM stream_lifecycle AS l
               WHERE l.stream_id = stream AND l.closed) THEN
        RAISE EXCEPTION 'stream % is closed', stream
        USING ERRCODE = 'EV001', HINT = stream;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- A stream's head (0009): the newest row's version, or the recorded
-- head of a stream truncated to nothing — whichever is higher.
CREATE FUNCTION eventyr_stream_head(stream TEXT) RETURNS BIGINT AS $$
    SELECT GREATEST(
        COALESCE((SELECT MAX(e.stream_version) FROM events AS e
                  WHERE e.stream_id = stream), 0),
        COALESCE((SELECT l.head FROM stream_lifecycle AS l
                  WHERE l.stream_id = stream), 0)
    );
$$ LANGUAGE sql STABLE;

-- The optimistic-concurrency check (0001, hint format from 0004): a
-- mismatch raises P0001 with the hint "{stream_id}:{version}", which the
-- store maps to StoreError::Conflict.
CREATE FUNCTION eventyr_check_expected(
    stream TEXT,
    expected_kind SMALLINT,          -- 0=Any, 1=Empty, 2=Exact
    expected_version BIGINT,         -- only for 2
    current_version BIGINT
) RETURNS void AS $$
BEGIN
    IF (expected_kind = 1 AND current_version <> 0) OR
       (expected_kind = 2 AND current_version <> expected_version) THEN
        RAISE EXCEPTION 'version conflict on stream %: expected % but stream is at %',
            stream, expected_version, current_version
        USING HINT = stream || ':' || current_version::text;
    END IF;
END;
$$ LANGUAGE plpgsql;

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
    PERFORM eventyr_lock_stream(append_events.stream_id);
    PERFORM eventyr_check_open(append_events.stream_id);
    current_version := eventyr_stream_head(append_events.stream_id);
    PERFORM eventyr_check_expected(
        append_events.stream_id, expected_kind, expected_version, current_version);
    PERFORM eventyr_commit_order_lock();

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
