# RocketMQ Dashboard (GPUI)

Native RocketMQ desktop dashboard built with GPUI and gpui-component. This is a
**standalone Cargo workspace**, using Rust 2024 and its checked-in toolchain.
It is not a member of the repository's root Cargo workspace.

## Run and build

Run these commands from the repository root, then remain in the GPUI directory:

```bash
cd rocketmq-dashboard/rocketmq-dashboard-gpui
cargo run
# Optimized executable
cargo build --release
```

A graphical desktop is required to run the application. Install the native build
prerequisites for the target platform: Xcode command line tools on macOS, the
MSVC C++ toolchain and Windows SDK on Windows, or the Linux GUI development
libraries described in [AGENTS.md](AGENTS.md). Build timings and memory usage
depend on the platform and workload.

The application starts in native fullscreen. Use the **Exit fullscreen** / **Enter fullscreen**
button to switch between fullscreen and the normal window. The button is keyboard accessible;
**Escape** continues to close dialogs and navigation drawers.

The Windows executable embeds its application icon from [assets/app-icon.ico](assets/app-icon.ico).
The original artwork and generation prompts are kept in [assets/](assets/app-icon.md).

Logging uses `RUST_LOG`. For example, in PowerShell from this directory:

```powershell
$env:RUST_LOG = "info,rocketmq_dashboard_gpui=debug"
cargo run
```

## Architecture and configuration

[src/main.rs](src/main.rs) initializes the GPUI component library and application
root, installs logging, and composes the desktop services. The layers are:

```text
GPUI views
  -> application services
  -> GpuiAdminProvider / configuration, history and monitor stores
  -> rocketmq-admin-core client adapter
  -> application-owned ClientRuntime
```

The shared dashboard models and admin facade come from
[rocketmq-dashboard-common](../rocketmq-dashboard-common). The desktop provider
implements live cluster queries and administration; configuration, history and
monitor persistence remain local to this application. See
[src/services](src/services) and [src/infrastructure](src/infrastructure) for
the implemented operations and their lifecycle owners.

The private [DashboardBrokerOperations](src/services/dashboard_broker.rs) boundary forwards
Dashboard overview/current/history and Broker inventory/runtime/config reads, plus guarded
Broker config patches. Its [Admin provider](src/infrastructure/admin_provider/dashboard_broker.rs)
keeps raw Admin responses inside infrastructure and returns allowlisted DTOs to the services.

Configuration defaults to `rocketmq-dashboard/gpui/config.json` beneath the OS
user configuration directory. Set `ROCKETMQ_DASHBOARD_GPUI_CONFIG_PATH` to
override the complete file path. NameServer selection and connection settings
are stored in this document.

OPS → Connection manages named environment profiles. Creating or selecting a
profile explicitly changes the environment; editing its NameServer, Proxy,
TLS, or VIP settings preserves its persistent identity. History and Monitor
rules belong to that identity, while the configuration revision identifies a
particular connection generation.

The version 3 configuration document holds profiles, global settings, Monitor
rules, local operation records, and the desktop audit journal in one atomic replacement. Metric samples
remain in the separately versioned `history.json`. Version 1 configuration and
history files, version 2 profile documents, and legacy Monitor arrays are migrated on startup. Legacy points
and rules without environment evidence remain unassigned and are excluded from
the selected environment. Legacy rules also require thresholds before they can
be evaluated; the Monitor lifecycle still performs no evaluation. The original
`monitors.json` is preserved after its rules are imported into the configuration
document. Unsupported schemas or failed migration writes preserve their source
files and report a recoverable startup error.

One application instance owns the local data directory for its lifetime through
an OS file lock. A second instance using that directory must wait until the first
closes. An interrupted write leaves either the previous complete document or the
new complete document; unfinished candidate files are not treated as committed
state. Configuration and Monitor changes carry their operation record in the
same commit as their terminal audit event.

Accepted Broker, Topic, Consumer, and configuration changes belong to the application,
so closing a Sheet or signing out only drops the old view's result receiver. External
changes first save an accepted-operation record and then save one terminal outcome;
these two writes do not provide a transaction with RocketMQ. Shutdown drains owned
work for the existing five-second budget while local storage and the provider remain
available. Interrupted requests stay pending for authoritative verification after restart,
and are never automatically replayed. Local login and logout also record safe outcomes.

Audit events contain trusted local actor and environment metadata, controlled resource
identities, and bounded result facts. They exclude message bodies, diagnostics,
credentials, and configuration values. A failed terminal audit write preserves the
business result and displays an application warning; **Retry audit write** only retries
persistence. The journal retains at most 10,000 terminal events within 31 days and
128 pending requests. Service queries support bounded filtering and stable cursors;
the Audit query page and future Message, DLQ, ACL, and Monitor editor hooks are later
deliveries. These records describe this desktop's operations and are not a tamper-proof
or organization-wide audit log.

Login events preserve their initiating actor and include the verified operator as a
separate terminal fact after successful authentication. Failed form usernames are not
retained. Exhausting the terminal-write recovery queue still permits local sign out;
one reserved logout fact remains visible and retryable without retaining a command.
Cursor continuation expires after journal retention evicts rows, including partial
eviction of unread results.

Authentication has two separate settings in the configuration:

- When local login is enabled, `ROCKETMQ_DASHBOARD_USERNAME` and
  `ROCKETMQ_DASHBOARD_PASSWORD` must be present. Only the local session marker
  is retained after authentication.
- When the Admin credential source is `environment`,
  `ROCKETMQ_ADMIN_ACCESS_KEY` and `ROCKETMQ_ADMIN_SECRET_KEY` are required;
  `ROCKETMQ_ADMIN_SECURITY_TOKEN` is optional. These credentials authenticate
  outbound RocketMQ requests.

The [auth owner](src/infrastructure/auth_state.rs) resolves secrets for immediate
use; the configuration file stores the credential-source choice. Background
admin and persistence work uses injected child scopes from the desktop runtime.
The application closes this runtime when the GPUI event loop exits.

## Development

From this directory, select the checks relevant to a change:

```bash
cargo fmt --all -- --check
cargo check --locked
cargo test --locked <test_name>
```

See [AGENTS.md](AGENTS.md) for platform setup and graphical smoke checks.

## License

Apache License, Version 2.0.
