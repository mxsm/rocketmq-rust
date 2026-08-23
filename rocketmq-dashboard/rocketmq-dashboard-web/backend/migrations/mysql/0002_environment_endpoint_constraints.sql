ALTER TABLE dashboard_endpoint ADD COLUMN role VARCHAR(32) NOT NULL DEFAULT 'secondary';
UPDATE dashboard_endpoint SET role = 'primary' WHERE is_active = TRUE;
ALTER TABLE dashboard_endpoint ADD COLUMN is_enabled BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE dashboard_endpoint
    ADD COLUMN active_endpoint_type VARCHAR(32)
    GENERATED ALWAYS AS (CASE WHEN is_active THEN endpoint_type ELSE NULL END) STORED;
CREATE UNIQUE INDEX dashboard_endpoint_one_active_per_type_uq
    ON dashboard_endpoint(environment_id, active_endpoint_type);
INSERT IGNORE INTO dashboard_schema_migration (version, applied_at_ms) VALUES (2, 0);
