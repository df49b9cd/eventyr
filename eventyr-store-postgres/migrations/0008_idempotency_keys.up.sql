-- Idempotency keys (0.7.5): `append_events` takes one more array and
-- stores each event's key in `metadata`, beside the causation and
-- correlation ids. A keyed command reads its target stream in full and
-- finds an earlier commit by the key, so no lookup index is needed: the
-- stream read is the lookup.
--
-- The seven-argument function is dropped rather than overloaded, so a
-- caller compiled against the old signature fails loudly instead of
-- silently writing events without their keys.
DROP FUNCTION append_events(SMALLINT, BIGINT, TEXT, TEXT[], JSONB[], TEXT[], TEXT[]);

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
            'correlation_id', correlation_id[n.ord],
            'idempotency_key', idempotency_key[n.ord]
        )), '{}')
    FROM generate_series(1, COALESCE(array_length(payload, 1), 0)) AS n(ord)
    RETURNING events.*;
END;
$$ LANGUAGE plpgsql;
