# Operations and Lifecycle

## Build and configuration checks

Inspect the selected toolchain and build prerequisites before launching anything. Proxy protobuf generation uses `protoc` on PATH or `PROTOC` (`rocketmq-proxy-core/build.rs`); Controller uses vendored protoc (`rocketmq-controller/build.rs`). Controller's default RocksDB build needs the platform's supported native C/C++ and bindgen prerequisites. Report the actual missing prerequisite, use a documented local setup path, and do not switch HA storage or upgrade dependencies to make a build pass.

The generated `manifest.json` contains:

- `build_commands`: argument arrays, executed with the repository root as cwd. Package and binary names differ for some components; use these arrays directly.
- `start_groups`: dependencies in order. Start every node within a group before waiting for group readiness, especially Controllers and synchronous Brokers.
- `nodes`: binary name, config arguments, per-process environment, cwd, log destinations, and readiness URL.
- `ports`: all configured or reserved ports, including Broker fast and HA ports and per-process health probes.

Build once, then execute the binaries directly for every replica. Reuse matching binaries only when they reflect the current source/features; existence alone is insufficient. Honor `CARGO_TARGET_DIR`, `.cargo/config.toml`, configured build target, and the selected profile when locating executables; `cargo metadata --format-version 1 --no-deps` and Cargo JSON executable artifacts can resolve nonstandard locations. Windows binaries have `.exe`. Do not run concurrent Cargo builds against the same target directory just to start replicas.

For example, the minimal environment needs:

```text
cargo build -p rocketmq-namesrv --bin rocketmq-namesrv-rust
cargo build -p rocketmq-broker --bin rocketmq-broker-rust
cargo build -p rocketmq-admin-cli --bin rocketmq-admin-cli
```

Use each node's environment and cwd for configuration checks. Append its `print_config_flag` to its `argv`: NameServer/Broker/Controller use `-c <file> -p`, Proxy uses `-c <file> --printConfig`. Inspect `--help` if options changed. Check exit codes; printing configuration does not verify listeners, storage initialization, quorum, or message delivery. Do not publish configuration dumps containing credentials.

## Start native processes

Choose the platform's existing process supervisor or managed terminal sessions. When background processes are necessary, keep their ownership records under `<environment>/run/` and provide reusable start/status/stop scripts there. The configuration generator intentionally does not manage processes. Implement those environment-specific commands using the following rules, instead of claiming that it already offers a `start` or `stop` subcommand.

1. Inspect the existing manifest and process records. An already-running node with matching identity should be reported/reused, not launched twice. Reject changed configuration for a live node.
2. Execute the exact binary with the manifest's argument array, per-node cwd, and environment. Overlay only the intended variables; inspect inherited `ROCKETMQ_*`, telemetry/health bindings, `NAMESRV_ADDR`, `rocketmq.namesrv.addr`, legacy `rocketmq.rocketmq-namesrv.addr`, and `ROCKETMQ_CONTROLLER_RAFT_BIND_ADDR` for conflicts. Do not save secrets into the run manifest. Do not interpolate arguments into shell code.
3. Redirect stdout and stderr to the corresponding node logs. Preserve earlier logs on restart through append or per-start filenames.
4. Immediately record PID, executable path, config arguments, process creation time, and the managed session/container ID when available. Never hold only the PID from a shell wrapper.
5. Launch each `start_groups` group, then poll its readiness with short request timeouts and an overall deadline (for example, 90 seconds after startup, extended with evidence for first initialization). Stop waiting early if a process exits. Capture its exit status and relevant log tail.
6. After service readiness, run the topology checks in [verification](verification.md). Controller election may need additional convergence time. A timeout is a failure to diagnose, not permission to start a duplicate.

PowerShell: use `Start-Process -WindowStyle Hidden -PassThru` for background launches, with `-WorkingDirectory`, separate `-RedirectStandardOutput` / `-RedirectStandardError`, and correctly quoted config paths in `-ArgumentList`. If setting process-level environment temporarily, save and restore it with `try/finally`; apply Controller bootstrap variables only to their intended nodes. `Get-Process` provides `StartTime`/`Path`; use `Get-CimInstance Win32_Process` when command-line verification is needed. Do not use `$PID`, `$HOME`, or `$CODEX_HOME` as task variables.

Bash: use explicit argv or an array and a subshell to scope cwd/environment. For persistent local background work, `nohup` with stdin closed and redirected logs is an option; capture the actual executable PID (use `exec` in wrappers). Record process start time and arguments using the host's `ps`, and handle platform differences between Linux and macOS. Keep all paths quoted.

For a one-node foreground check, after substituting absolute paths from the manifest:

```bash
cd "$NODE_HOME"
ROCKETMQ_HOME="$NODE_HOME" \
ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback \
ROCKETMQ_HEALTH_BIND_ADDR=127.0.0.1:18000 \
"$BIN_DIR/rocketmq-namesrv-rust" -c "$CONFIG_FILE"
```

The health address above illustrates the first zero-offset NameServer only. Use each node's actual manifest values, including all Controller environment entries, for real launches. A foreground command is not the complete cluster launcher.

## Status, stop, and restart

Status must distinguish process existence, readiness, route registration, and topology health. `GET /livez` and `GET /readyz` are non-mutating probes. Do not probe `/drainz` during a status check: it requests shutdown.

For a normal stop, verify ownership, stop Proxy ingress first, then Brokers, Controllers, and NameServers. For ordinary HA groups, stop the master before its slave so the master's final drain can retain its replica. For a full Controller HA shutdown, keep quorum available while Brokers drain. Process groups should share bounded deadlines rather than waiting forever for each individual node.

The current service lifecycle supports a graceful HTTP drain, including on Windows:

```powershell
Invoke-WebRequest -UseBasicParsing -Method Post -Uri $ownedDrainUrl -TimeoutSec 3
```

```bash
curl --fail --silent --show-error --max-time 3 --request POST "$OWNED_DRAIN_URL"
```

Derive that URL from the verified node's health endpoint by replacing `/readyz` with `/drainz`. Confirm the listener still belongs to that node before sending it. Await exit within the configured shutdown deadline (the helper uses 45 seconds), and inspect shutdown errors. A successful drain HTTP response only means the request was accepted. On Unix, SIGTERM to a verified PID is a fallback when the probe is unavailable. Windows `Stop-Process` is forceful, not equivalent to graceful drain; use it only for a requested crash exercise or a stuck owned process after a bounded graceful attempt, and report that outcome.

Restart with the same configuration, data, identities, and ports. Controllers may reuse the generated bootstrap environment: existing committed state is not reinitialized. Revalidate registration, synchronization, and a message round trip. Reset is a separate destructive operation; require the user's actual reset request and never delete data as routine startup repair.

## Containers and local Kubernetes

Use this branch only when requested or chosen with the user's existing context. The helper generates native loopback environments, not container configuration.

- Inspect `distribution/container/core-service.Dockerfile`, `distribution/helm/rocketmq-rust-core/README.md`, its values/schema/templates, and `distribution/kubernetes/README.md`. Check actual image availability and binary contents; do not invent an official image tag. Prefer building this checkout for source-based development.
- For Compose, put every node on a named project network, use service DNS names for inter-service addresses, separate volumes, and unique host port publications bound to `127.0.0.1`. Internal ports can repeat across containers; native host ports cannot. Controller Raft peer addresses and Broker-facing discovery addresses remain distinct.
- Loopback inside a container is that container. Choose bind and advertised addresses separately, and ensure client-visible route addresses are reachable from the user's host or client container. Publishing only NameServer or Proxy ports does not automatically make direct Broker clients work.
- `development-insecure-loopback` rejects public binds inside containers. Choose a repository-supported container security setup and inspect the relevant guide; do not silently turn off security to force `0.0.0.0` to bind. Host-only publication alone does not change this runtime check.
- For Kind/Helm, explicitly select the intended local Kubernetes context and namespace. Do not send an install to an inherited remote context. Adapt development values and resources for the local machine; the production chart's host-separation rules and TLS settings are not evidence of local HA.
- Validate generated Compose/Helm configuration, then start and verify the actual workload. Do not invoke release publication, supply-chain promotion, full SLO collection, or destructive fault scripts merely to create a development cluster.

WSL has its own filesystem, toolchain, processes, and network namespace. Keep binaries, data, and lifecycle ownership on one side of the boundary, and verify the endpoint the user's client actually reaches.
