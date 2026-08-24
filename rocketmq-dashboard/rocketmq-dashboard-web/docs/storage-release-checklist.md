# Storage release checklist

Use this checklist for every dashboard release that changes persistence,
storage operations, deployment configuration, or storage observability.

## Build and contract verification

- [ ] Backend format, Clippy, build, and unit/integration tests pass.
- [ ] Frontend dependency installation, tests, and production build pass.
- [ ] The project-scoped Docker matrix passes File, mounted-file SQLite,
  MySQL 8.4, and PostgreSQL 15 contract tests.
- [ ] The matrix covers empty initialization, a second startup, CRUD,
  transaction/CAS behavior where applicable, restart recovery, and sanitized
  storage status.
- [ ] Docker cleanup removes only the generated Compose project resources.
- [ ] No test output, artifact, documentation, Compose variable, or metric
  label exposes a production credential, token, TLS private material, or
  database URL.

## Deployment readiness

- [ ] The selected backend is explicit and matches the intended topology:
  File/SQLite single-node, MySQL/PostgreSQL multi-node capable.
- [ ] File and SQLite volumes are durable, private, and mounted by only one
  dashboard process.
- [ ] SQL URL uses `DASHBOARD_WEB_DATABASE_URL_FILE`; no production password is
  stored in an environment file or repository configuration.
- [ ] MySQL uses verified identity TLS and PostgreSQL uses verified full TLS;
  the CA file and host identity were tested from the deployment network.
- [ ] The database application account is restricted to dashboard data and
  forward migrations.
- [ ] Container liveness, readiness, authenticated OPS status, and alert
  routing are tested after deployment.
- [ ] Replica pool limits fit the database connection budget.

## Backup and recovery rehearsal

- [ ] A package is created as `manifest.json` plus the fixed NDJSON
  collections and passes `verify`.
- [ ] Backup storage permissions and retention meet the platform policy.
- [ ] File backup was performed with dashboard processes offline; all restore
  exercises were performed offline.
- [ ] Restore succeeds only into an empty new target of the same backend type.
- [ ] The restored target completes startup, readiness, authentication, and
  OPS status checks before traffic is enabled.
- [ ] A restore rejection for a non-empty target, malformed package, missing
  collection, or backend mismatch was observed and did not alter target data.

## Release notes and known limitations

Record the following in the release note and change record:

- [ ] Supported backend versions: File, SQLite file, MySQL 8.4+, PostgreSQL
  15+.
- [ ] File and SQLite are single-node only; they are not shared-disk
  replication solutions.
- [ ] MySQL and PostgreSQL require external TLS, verified server identity, and
  least-privilege database accounts in production.
- [ ] Backup packages are ordinary manifest/NDJSON directories with no
  checksum, fingerprint, CRC, custom framing, archive encryption, or legacy
  layout compatibility.
- [ ] Restore is offline, same-backend only, and accepts only a new empty
  target; it does not merge or overwrite existing storage.
- [ ] Backup packages contain no plaintext session tokens but remain sensitive
  operational data and require access controls.
- [ ] No experimental legacy dashboard layout is migrated or read by this
  release.
- [ ] Metrics and sanitized OPS status are dashboard aids, not a replacement
  for database, filesystem, backup, or security monitoring.
