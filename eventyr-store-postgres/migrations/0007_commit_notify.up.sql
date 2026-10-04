-- Commit notifications (0.7.2): every append raises
-- `NOTIFY eventyr_commits`, which Postgres delivers to listeners when
-- the transaction commits (never on rollback) and collapses to one per
-- transaction. A caught-up projector listening on the channel polls at
-- once instead of waiting out its idle sleep.
--
-- A hint only: the payload is empty, the channel is database-wide (a
-- listener may wake for another schema's commit and poll for nothing),
-- and a notification lost to a dropped connection costs latency, never
-- an event — the projector's checkpoint poll stays authoritative.
--
-- NOTIFY serializes committing transactions on a global lock; appends
-- are already commit-serialized by 0006's lock, so this adds no
-- contention.
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
            'correlation_id', correlation_id[n.ord]
        )), '{}')
    FROM generate_series(1, COALESCE(array_length(payload, 1), 0)) AS n(ord)
    RETURNING events.*;
END;
$$ LANGUAGE plpgsql;
