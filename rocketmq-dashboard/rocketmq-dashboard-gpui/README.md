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

Configuration defaults to `rocketmq-dashboard/gpui/config.json` beneath the OS
user configuration directory. Set `ROCKETMQ_DASHBOARD_GPUI_CONFIG_PATH` to
override the complete file path. NameServer selection and connection settings
are stored in this document.

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
