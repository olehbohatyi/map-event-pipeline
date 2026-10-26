-- PostGIS: exact, indexed geo queries instead of a lat/lon bounding box.
CREATE EXTENSION IF NOT EXISTS postgis;

-- Derived from lat/lon, so writers keep inserting plain coordinates and never touch this column
ALTER TABLE map_events
    ADD COLUMN geog geography(Point, 4326)
        GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography) STORED;

CREATE INDEX idx_map_events_geog ON map_events USING gist (geog);

-- Superseded by the GiST index above
DROP INDEX IF EXISTS idx_map_events_location;

-- First-level administrative regions; rows are added by data migrations (see V3)
CREATE TABLE regions (
    code     VARCHAR(16)  PRIMARY KEY,           -- ISO 3166-2, e.g. UA-46
    name     VARCHAR(128) NOT NULL UNIQUE,
    country  CHAR(2)      NOT NULL,              -- ISO 3166-1 alpha-2
    geom     geometry(MultiPolygon, 4326) NOT NULL
);

CREATE INDEX idx_regions_geom ON regions USING gist (geom);
