-- Wird beim ersten Start des timescaledb-Containers automatisch ausgeführt
-- (siehe docker-entrypoint-initdb.d in docker-compose.yml)

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Aggregierte Sensordaten (Ergebnis des Tumbling-Window-Processings)
CREATE TABLE IF NOT EXISTS sensor_aggregates (
    id              BIGSERIAL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    sensor_id       TEXT NOT NULL,
    sensor_type     TEXT NOT NULL,
    unit            TEXT NOT NULL,
    avg_value       DOUBLE PRECISION NOT NULL,
    min_value       DOUBLE PRECISION NOT NULL,
    max_value       DOUBLE PRECISION NOT NULL,
    sample_count    INTEGER NOT NULL,
    PRIMARY KEY (window_start, id)
);

-- Wandelt die Tabelle in eine TimescaleDB-Hypertable um (partitioniert
-- automatisch nach Zeit -> effiziente Speicherung & Abfragen großer
-- Zeitreihenmengen; zentraler Vorteil ggü. einer normalen Postgres-Tabelle)
SELECT create_hypertable(
    'sensor_aggregates', 'window_start',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_sensor_aggregates_sensor_id
    ON sensor_aggregates (sensor_id, window_start DESC);

-- Data-Governance: technischer User für die Reporting-Applikation mit
-- ausschließlich lesenden Rechten (Prinzip der geringsten Rechte)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'reporting_readonly') THEN
        CREATE ROLE reporting_readonly WITH LOGIN PASSWORD 'change_me_too';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE sensor_data TO reporting_readonly;
GRANT USAGE ON SCHEMA public TO reporting_readonly;
GRANT SELECT ON sensor_aggregates TO reporting_readonly;

-- Automatische Datenaufbewahrung (Data Governance): Aggregate älter als
-- 90 Tage werden automatisch entfernt
SELECT add_retention_policy('sensor_aggregates', INTERVAL '90 days', if_not_exists => TRUE);
