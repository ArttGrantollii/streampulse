-- Baseline for the StreamPulse serving database.
-- Flyway creates the `streampulse` schema (FLYWAY_SCHEMAS) and records applied
-- migrations in streampulse.flyway_schema_history. Result tables arrive in Phase 3.

COMMENT ON SCHEMA streampulse IS
    'StreamPulse serving layer: windowed analytics written by the Flink job (idempotent upserts).';

-- All timestamps are stored as timestamptz and displayed in UTC so that event-time
-- windows read the same for every client, regardless of the client machine's zone.
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone TO %L', current_database(), 'UTC');
END
$$;
