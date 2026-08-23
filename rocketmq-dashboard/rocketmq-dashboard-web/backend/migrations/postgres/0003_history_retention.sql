CREATE TABLE IF NOT EXISTS dashboard_history_sample (
    environment_id VARCHAR(36) COLLATE "C" NOT NULL,
    metric_name VARCHAR(64) COLLATE "C" NOT NULL,
    bucket_ms BIGINT NOT NULL,
    dimensions_json VARCHAR(512) COLLATE "C" NOT NULL,
    value DOUBLE PRECISION NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS dashboard_history_sample_query_idx
    ON dashboard_history_sample(environment_id, metric_name, dimensions_json, bucket_ms);
CREATE INDEX IF NOT EXISTS dashboard_history_sample_retention_idx
    ON dashboard_history_sample(environment_id, bucket_ms);
INSERT INTO dashboard_schema_migration (version, applied_at_ms) VALUES (3, 0)
ON CONFLICT (version) DO NOTHING;
