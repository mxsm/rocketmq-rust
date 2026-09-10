---
title: "Find the right diagnostic procedure"
---

The operational runbook is [Troubleshoot by symptom](../operations/troubleshooting.md). It separates startup, discovery, request security, persistence/replication, and consumer completion.

1. Start with [first diagnosis](../operations/first-diagnosis.md) and record one failed operation.
2. Use [Admin observations](../operations/admin.md) to compare routes, Broker state, and consumer progress.
3. Use [monitoring](../operations/monitoring.md) to correlate controlled logs, metrics, and traces.
4. Preserve data before a [recovery decision](../operations/backup-recovery.md); use [maintenance](../operations/maintenance.md) for a planned restart.

Rust processes do not expose JVM heap/thread-dump interfaces. Use the actual Rust process, supported service log filters, and platform observations. Collect stable public errors and non-secret context; keep credentials and message bodies out of reports.
