-- Projector leases (0.7.9): one row per subscription name, held by one
-- driver at a time. `holder` is the fence the lease store itself checks
-- (a renewal from a row whose holder has moved on fails `Lost`; the
-- checkpoint store keeps no token, so the lease is exclusive against
-- the row's own holder check, not against the checkpoint write
-- itself). `version` increments on every renewal, so two renewals
-- racing one row cannot both succeed.
CREATE TABLE projector_leases (
    name         TEXT        PRIMARY KEY,
    holder       UUID        NOT NULL,
    acquired_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    renewed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    ttl_ms       BIGINT      NOT NULL CHECK (ttl_ms > 0),
    grace        INTEGER     NOT NULL CHECK (grace > 0),
    max_grace    INTEGER     NOT NULL CHECK (max_grace > grace),
    version      BIGINT      NOT NULL,
    CHECK (version >= 0)
);
