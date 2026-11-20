-- Events are closed (status Resolved or Expired) at closed_at; open events have none
ALTER TABLE map_events ADD COLUMN closed_at TIMESTAMPTZ;

-- Open events by type and age: what the pipe's expiry sweep looks for
CREATE INDEX idx_map_events_open ON map_events (event_type, created_at) WHERE status IN ('Raw', 'Enriched');
