-- Wird beim ersten Start des postgres-Containers automatisch ausgeführt
-- (siehe docker-entrypoint-initdb.d in docker-compose.yml)

-- Serving Layer: finale, aggregierte Kennzahlen für die nachgelagerte
-- Machine-Learning-Applikation zur Prognose von Flugverspätungen
CREATE TABLE IF NOT EXISTS flight_delay_aggregates (
    id                  BIGSERIAL PRIMARY KEY,
    flight_date         DATE NOT NULL,
    origin_airport      TEXT NOT NULL,
    dest_airport        TEXT,
    avg_departure_delay DOUBLE PRECISION,
    avg_arrival_delay   DOUBLE PRECISION,
    flight_count        INTEGER NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    season              TEXT NOT NULL,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (flight_date, origin_airport, dest_airport)
);

CREATE INDEX IF NOT EXISTS idx_flight_delay_date_origin
    ON flight_delay_aggregates (flight_date, origin_airport);

-- Data-Governance: technischer User für die ML-Applikation mit
-- ausschließlich lesenden Rechten (Prinzip der geringsten Rechte)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'ml_readonly') THEN
        CREATE ROLE ml_readonly WITH LOGIN PASSWORD 'change_me_too';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE flight_data TO ml_readonly;
GRANT USAGE ON SCHEMA public TO ml_readonly;
GRANT SELECT ON flight_delay_aggregates TO ml_readonly;
