-- Commit-ordered global sequences.
--
-- `global_sequence` is an identity column: a value is drawn at INSERT,
-- but transactions commit in their own order. Before this migration a
-- transaction could draw sequence N, stay open, and let another commit
-- N+1 first; a reader polling in between saw N+1, checkpointed past N,
-- and never delivered N once it committed. `StreamsAll` forbids exactly
-- that: a later sequence may not become visible before an earlier one
-- that will eventually commit.
--
-- The fix serializes the window from drawing a sequence to committing:
-- every append takes one transaction-scoped advisory lock (key below)
-- right before its INSERT and holds it until commit. At most one
-- transaction holds drawn-but-uncommitted sequences, and every sequence
-- it draws is above every committed one — so a visible sequence implies
-- every lower one is committed or permanently absent (a rollback gap).
--
-- Lock order everywhere is: per-stream locks (sorted) → this lock →
-- the INSERT. Conditional appends (`append_if`) take this lock before
-- checking their condition, which makes the check see every committed
-- event and no in-flight one — it replaces the table lock 0.7.1 used.
--
-- The key is an arbitrary constant in the single-key advisory space;
-- `hashtext` values (the per-stream locks) live in the same space, so a
-- stream whose hash collides with it would only over-serialize, never
-- deadlock (the same session re-takes advisory locks freely).
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
