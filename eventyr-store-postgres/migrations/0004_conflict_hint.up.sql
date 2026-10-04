-- A version conflict's hint names the stream: `"{stream_id}:{version}"`.
--
-- 0001's `append_events` hinted the bare version, which left a batch
-- conflict unable to say which stream moved — the batch machine reloads
-- only that stream, so the store must name it. The single-stream path
-- loses nothing: the hint parser reads the stream from the text before
-- the last colon either way.
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
