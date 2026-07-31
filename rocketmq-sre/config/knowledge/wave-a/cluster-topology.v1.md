# Cluster topology diagnosis

Use this runbook when NameServer routes, Broker membership, Controller metadata, or Proxy discovery disagree.

## Required evidence

- MCP cluster overview and topic route metadata.
- NameServer registration freshness and route-error counters.
- Controller quorum health, leader term, and stale-Broker summary.
- Bounded Kubernetes workload identity, readiness, owner reference, and restart metadata.

## Interpretation

A missing component is not proof of failure. Mark the diagnosis partial until its required source is available. Treat a route that references a non-ready Broker, a Controller term disagreement, or registration age beyond the configured freshness window as supporting evidence. A healthy quorum and fresh, mutually consistent routes are counter-evidence.

## Read-only recommendation

Identify the inconsistent component and compare its last successful registration or reconciliation time with deployment events. Do not delete routes, restart pods, or change quorum membership from Phase 01.
