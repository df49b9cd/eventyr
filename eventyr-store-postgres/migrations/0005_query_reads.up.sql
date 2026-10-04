-- Query reads (0.7.1): a boundary decision reads the events of a few
-- types after a position. The index serves the `event_type = ANY(..)`
-- prefilter in global order; tags are matched on the decoded event.
CREATE INDEX events_event_type_sequence ON events (event_type, global_sequence);
