# Optional local governance storage

This fixture adds the existing Web Backend to the [local source cluster](README.md).
It is optional: native GPUI still starts independently and keeps its connection
profiles, History, Monitor rules, and local Audit in its own File storage. A
governance connection manages the Backend's Sessions, Audit, and storage status.
Direct Broker operations do not automatically enter the Backend's Audit.

The Backend owns File, SQLite, MySQL, and PostgreSQL persistence. GPUI does not
link those SQL drivers or store a server database URL. Backend storage is selected
at startup and never falls back to a different engine when that engine fails.

## Start one engine

Run from this directory with Docker Desktop using Linux containers. Keep the
source cluster's image available as described in its README. Generate local
passwords without printing them, then select exactly one profile:

```powershell
$env:GPUI_GOVERNANCE_PASSWORD = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(24))
$env:GPUI_GOVERNANCE_DB_PASSWORD = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(24))
$governanceProfile = 'governance-sqlite' # governance-file, governance-mysql, governance-postgres
$governanceCompose = @('-f', 'compose.yaml', '-f', 'compose.governance.yaml', '--profile', $governanceProfile)
docker compose @governanceCompose config --quiet
docker compose @governanceCompose build $governanceProfile
docker compose @governanceCompose up -d --no-build --wait --wait-timeout 180
docker compose @governanceCompose ps
```

The build reuses `rocketmq-dashboard-web/deploy/backend.Dockerfile` and the
Backend's default Cargo features. To use a locally prebuilt image, set
`GPUI_GOVERNANCE_IMAGE` to its tag, skip `build`, and use `up --no-build` as above.
The packaged image must contain both the Backend and `rocketmq-dashboard-storage`.
Passwords must be kept for a retained database volume; changing the environment
does not rotate an existing database account. Do not place credentials in source
files or publish the output of an expanded `docker compose config`.

The overlay adds two loopback publications to the NameServer's shared network
namespace. Enabling it can recreate that namespace: start the complete project
together so Broker, Proxy, and Backend join the current namespace. Do not recreate
only NameServer while its dependants are still attached to an old namespace.

| Profile | Backend | Host API | Persistence / instances |
| --- | --- | --- | --- |
| `governance-file` | File | `http://127.0.0.1:38085` | Separate named volume; one Backend. |
| `governance-sqlite` | SQLite file | `http://127.0.0.1:38085` | Separate named volume; one Backend. |
| `governance-mysql` | MySQL 8.4 | `38085`, peer `38086` | One local database volume; two independent Backends. |
| `governance-postgres` | PostgreSQL 15 | `38085`, peer `38086` | One local database volume; two independent Backends. |

The default username is `gpui-governance-admin`; `GPUI_GOVERNANCE_USERNAME` can
override it. Login is required. Existing Backend authentication and permission
checks remain authoritative. The fixture explicitly uses HTTP cookies on host
loopback; this is not a production TLS deployment. SQL credentials are generated
local fixture values, databases publish no host port, and database TLS is disabled
only inside this local Docker network. Use the Web [production deployment guide](../../../rocketmq-dashboard-web/docs/storage-deployment.md)
for verified TLS, secret files, account grants, and managed SQL databases.

## Observe the selected storage safely

Use the governance address in GPUI and sign in with the separate Backend login.
The Backend endpoint `GET /api/ops/storage/status` returns its selected `backend`,
`mode`, `status`, optional bounded `reason`, schema/format version, observation
start, last process-observed successful write, and available capacity/pool facts.
It does not return server paths, SQL URLs, database usernames, credentials, tokens,
or raw driver errors. An unavailable status remains unavailable; a failed refresh
must not be presented as a fresh healthy probe.

`GET /api/health/live` establishes process liveness only. `GET /api/health/ready`
checks storage readiness. Neither replaces the authenticated status endpoint.
Use that API while a File Backend is running: it owns the exclusive directory
lock, so a second storage CLI process cannot open the same File store. The
packaged utility initializes storage and requires exclusive File access; stop
the File Backend before using it against that directory. SQL profiles support
the following status command while their Backends are running:

```powershell
docker compose @governanceCompose exec $governanceProfile rocketmq-dashboard-storage status --json
```

For SQL sharing, the API acceptance check retains one issued session credential
in memory and queries it through both addresses. Both instances must see the same
Session and Audit records; revocation through one must invalidate that session
through the other. Changing the configured governance address in GPUI does not
implicitly transfer its authenticated session. A fresh login after revocation is
a new session, not a retry of an old mutation. File and SQLite have no peer because
their single-node storage must not be mounted into multiple Backend processes.

Stop/start or recreate only the selected Backend services with their existing
volumes to verify that Sessions, revocations, and Audit survive reopening. When
using the shared namespace, preserve NameServer ownership as described above.
`docker compose @governanceCompose down` preserves the project volumes. Do not
remove volumes or prune Docker to rebuild this fixture. Stop the complete selected
profile before selecting another engine; the engines use distinct volumes and
are not an automatic migration or replication mechanism.

## Reusable storage contracts

The Web [storage integration runner](../../../rocketmq-dashboard-web/docker-compose.storage-test.yml)
already owns the isolated engine test environment. Run it from
`rocketmq-dashboard/rocketmq-dashboard-web`, in a new disposable Compose project,
separate from this retained governance fixture:

```powershell
$storageTestProject = 'dashboard-storage-' + [guid]::NewGuid().ToString('N').Substring(0, 12)
$storageTestCompose = @('-p', $storageTestProject, '-f', 'docker-compose.storage-test.yml')
docker compose @storageTestCompose config --quiet
docker compose @storageTestCompose up -d --wait --wait-timeout 180 mysql postgres
docker compose @storageTestCompose run --build --rm storage-test-runner
```

That existing runner executes library and binary targets, including ignored
engine tests, serially. Tests intentionally alter migration metadata in their
disposable databases. The relevant contracts cover:

- File and SQLite Session/Audit reopen; MySQL/PostgreSQL shared Session/Audit
  visibility and revocation, followed by reopen.
- Environment and Monitor concurrent revision conflicts and durable reload.
- SQL Session/Environment/Monitor plus Audit transaction rollback; File prepared
  and committed transaction recovery.
- SQLite/MySQL partial migration recovery, PostgreSQL mid-migration rollback,
  and concurrent SQL migration startup.
- Real bounded SQL connection failure without a File fallback; sanitized status
  and authentication boundaries.

For a focused run after the runner image is built, override its entrypoint. These
tests are in the Backend library, so `--bin` would miss them:

```powershell
docker compose @storageTestCompose run --rm --entrypoint cargo storage-test-runner test --locked --manifest-path rocketmq-dashboard/rocketmq-dashboard-web/backend/Cargo.toml --lib docker_session_audit_contracts_cover_file_sqlite_mysql_and_postgres -- --ignored --nocapture --test-threads=1
docker compose @storageTestCompose run --rm --entrypoint cargo storage-test-runner test --locked --manifest-path rocketmq-dashboard/rocketmq-dashboard-web/backend/Cargo.toml --lib postgres_interrupted_migration_rolls_back_and_reopens -- --ignored --nocapture --test-threads=1
```

Do not run these against retained governance or production data. After collecting
the result, only the newly created disposable test project can be removed with
`docker compose @storageTestCompose down --volumes --remove-orphans`. This explicit
test reset is separate from the retained local cluster's lifecycle.
