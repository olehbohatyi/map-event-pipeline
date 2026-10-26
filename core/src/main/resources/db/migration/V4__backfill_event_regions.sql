-- Events enriched before the region polygons existed carry a 1-degree grid zone such as zone_50_30.
-- Give them the name of the region that contains them, so statistics do not split between the two
-- naming schemes. Points outside every region (other countries, open water) keep their grid zone.
UPDATE map_events e
SET region = COALESCE(
    (SELECT r.name FROM regions r WHERE ST_Covers(r.geom, e.geog::geometry) ORDER BY r.code LIMIT 1),
    e.region
)
WHERE e.region ~ '^zone_-?[0-9]+_-?[0-9]+$';

-- map_event_stats is derived data, rebuilt by the stat job; drop rows for regions that no longer exist in
-- map_events (the grid zones that were just renamed)
DELETE FROM map_event_stats s
WHERE NOT EXISTS (SELECT 1 FROM map_events e WHERE e.region = s.region);
