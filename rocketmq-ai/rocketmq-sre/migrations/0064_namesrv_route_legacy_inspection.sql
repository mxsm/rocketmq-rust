-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Keep explicit historical Boolean route evaluations addressable while new
-- inspections select the current-context, exact-peer observation version.
ALTER TABLE inspection_runs
    DROP CONSTRAINT inspection_runs_template_check,
    ADD CONSTRAINT inspection_runs_template_check CHECK (
        template IN (
            'cluster_health', 'consumer', 'broker', 'telemetry', 'full_cluster',
            'producer_consumer', 'store_ha', 'routing_proxy', 'security',
            'upgrade', 'disaster_recovery', 'store_pressure_legacy_v1',
            'send_latency_legacy_v1', 'namesrv_route_legacy_v1'
        )
    );
