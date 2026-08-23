ALTER TABLE dashboard_endpoint ADD COLUMN IF NOT EXISTS role VARCHAR(32) NOT NULL DEFAULT 'secondary';
UPDATE dashboard_endpoint SET role = 'primary' WHERE is_active;
ALTER TABLE dashboard_endpoint ADD COLUMN IF NOT EXISTS is_enabled BOOLEAN NOT NULL DEFAULT TRUE;
CREATE UNIQUE INDEX IF NOT EXISTS dashboard_endpoint_one_active_per_type_uq
    ON dashboard_endpoint(environment_id, endpoint_type)
    WHERE is_active;
INSERT INTO dashboard_schema_migration (version, applied_at_ms) VALUES (2, 0)
ON CONFLICT (version) DO NOTHING;
