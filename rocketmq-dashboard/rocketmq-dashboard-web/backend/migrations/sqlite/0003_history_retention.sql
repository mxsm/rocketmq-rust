CREATE TABLE IF NOT EXISTS dashboard_history_sample (
    environment_id TEXT COLLATE BINARY NOT NULL,
    metric_name TEXT COLLATE BINARY NOT NULL,
    bucket_ms INTEGER NOT NULL,
    dimensions_json TEXT COLLATE BINARY NOT NULL,
    value REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS dashboard_history_sample_query_idx
    ON dashboard_history_sample(environment_id, metric_name, dimensions_json, bucket_ms);
CREATE INDEX IF NOT EXISTS dashboard_history_sample_retention_idx
    ON dashboard_history_sample(environment_id, bucket_ms);
INSERT OR IGNORE INTO dashboard_schema_migration (version, applied_at_ms) VALUES (3, 0);
