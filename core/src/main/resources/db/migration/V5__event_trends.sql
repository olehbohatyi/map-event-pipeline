-- map_event_trends: event counts per hour, region and event type, written continuously by the stat module's
-- streaming job (TrendsApp) from map.events.enriched. Counts are absolute, so rewriting a window is idempotent.
CREATE TABLE map_event_trends (
    window_start TIMESTAMPTZ  NOT NULL,            -- start of the hour (UTC), by the gate's receive time
    region       VARCHAR(128) NOT NULL,
    event_type   VARCHAR(32)  NOT NULL,
    count        BIGINT       NOT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, region, event_type)
);

-- The API reads the last hours of one region, or of all of them
CREATE INDEX idx_map_event_trends_region ON map_event_trends (region, window_start DESC);
CREATE INDEX idx_map_event_trends_window ON map_event_trends (window_start DESC);
