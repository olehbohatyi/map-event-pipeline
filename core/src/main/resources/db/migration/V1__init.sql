-- map_events: stores all road sensor events
CREATE TABLE IF NOT EXISTS map_events (
    id           UUID        PRIMARY KEY,
    event_type   VARCHAR(32) NOT NULL,
    severity     VARCHAR(16) NOT NULL,
    status       VARCHAR(16) NOT NULL DEFAULT 'Raw',
    lat          DOUBLE PRECISION NOT NULL,
    lon          DOUBLE PRECISION NOT NULL,
    description  TEXT,
    region       VARCHAR(128),
    occurred_at  TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_map_events_status     ON map_events(status);
CREATE INDEX IF NOT EXISTS idx_map_events_event_type ON map_events(event_type);
CREATE INDEX IF NOT EXISTS idx_map_events_region     ON map_events(region);
CREATE INDEX IF NOT EXISTS idx_map_events_location   ON map_events(lat, lon);
CREATE INDEX IF NOT EXISTS idx_map_events_occurred   ON map_events(occurred_at DESC);

-- map_event_stats: aggregated statistics per region and event type (written by stat/Spark)
CREATE TABLE IF NOT EXISTS map_event_stats (
    region       VARCHAR(128) NOT NULL,
    event_type   VARCHAR(32)  NOT NULL,
    count        BIGINT       NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (region, event_type)
);
