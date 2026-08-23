ALTER TABLE dashboard_endpoint ADD COLUMN role TEXT NOT NULL DEFAULT 'secondary';
UPDATE dashboard_endpoint SET role = 'primary' WHERE is_active = 1;
ALTER TABLE dashboard_endpoint ADD COLUMN is_enabled INTEGER NOT NULL DEFAULT 1;
CREATE UNIQUE INDEX IF NOT EXISTS dashboard_endpoint_one_active_per_type_uq
    ON dashboard_endpoint(environment_id, endpoint_type)
    WHERE is_active = 1;
INSERT OR IGNORE INTO dashboard_schema_migration (version, applied_at_ms) VALUES (2, 0);
