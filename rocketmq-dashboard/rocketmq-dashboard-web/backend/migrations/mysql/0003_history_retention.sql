CREATE TABLE IF NOT EXISTS dashboard_history_sample (
    environment_id VARBINARY(36) NOT NULL,
    metric_name VARBINARY(64) NOT NULL,
    bucket_ms BIGINT NOT NULL,
    dimensions_json VARBINARY(512) NOT NULL,
    value DOUBLE NOT NULL,
    UNIQUE KEY dashboard_history_sample_query_idx(environment_id, metric_name, dimensions_json, bucket_ms),
    KEY dashboard_history_sample_retention_idx(environment_id, bucket_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT IGNORE INTO dashboard_schema_migration (version, applied_at_ms) VALUES (3, 0);
