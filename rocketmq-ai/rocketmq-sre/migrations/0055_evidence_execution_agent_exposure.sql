-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

ALTER TABLE evidence_snapshots
    DROP CONSTRAINT evidence_snapshots_exposure_check;

ALTER TABLE evidence_snapshots
    ADD CONSTRAINT evidence_snapshots_exposure_check
    CHECK (
        exposure IN (
            'unknown',
            'mcp_tool',
            'mcp_resource',
            'admin_rpc',
            'prometheus_api',
            'alertmanager_api',
            'loki_api',
            'tempo_api',
            'kubernetes_api',
            'runtime_diagnostics',
            'execution_agent_api',
            'required_signals',
            'synthetic',
            'unsupported'
        )
    );
