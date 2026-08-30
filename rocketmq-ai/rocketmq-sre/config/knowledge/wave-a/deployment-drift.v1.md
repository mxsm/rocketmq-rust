# Deployment drift diagnosis

Use this runbook when observed image, configuration, feature, or topology fingerprints differ from the approved baseline.

## Required evidence

- Kubernetes workload generation, image digest, owner, readiness, and restart metadata.
- Sanitized configuration and feature fingerprints, never raw secrets.
- RocketMQ component version and capability manifest.
- Recent rollout events and evidence freshness.

## Interpretation

A digest or feature fingerprint mismatch supports drift only when both observations cover the same declared baseline and cluster scope. An intentional rollout event is context, not automatic root cause. Stale or local-only observations cap confidence and keep the result partial.

## Read-only recommendation

Show the exact non-secret fingerprint differences and deployment correlation. Do not roll back workloads or overwrite configuration in Phase 01.
