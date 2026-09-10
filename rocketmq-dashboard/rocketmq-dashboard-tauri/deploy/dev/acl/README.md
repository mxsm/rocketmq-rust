# Isolated RocketMQ Rust ACL fixture

This fixture uses the local source-built images from the [main debugging
fixture](../README.md). Start from this directory:

```powershell
docker compose config --quiet
docker compose up -d --wait --wait-timeout 120
```

Docker Desktop shows a separate `rocketmq-tauri-acl-debug` group. Its NameServer
is `127.0.0.1:29876`, Broker is `127.0.0.1:22911`, and health endpoints are
`28088` and `28090`. Disable VIP and TLS. It uses separate data volumes and only
publishes loopback ports. The public `tauri-dev-admin` / `tauri-dev-secret` seed is
solely for local testing. The Broker enables authentication and authorization;
the NameServer is a discovery service without ACL enforcement in this fixture.

From the app root, start the Windows app with the fixture credentials:

```powershell
$env:DASHBOARD_TAURI_ROCKETMQ_ACCESS_KEY = 'tauri-dev-admin'
$env:DASHBOARD_TAURI_ROCKETMQ_SECRET_KEY = 'tauri-dev-secret'
npm run tauri dev
```

Select the ACL NameServer address in connection settings. Restart the app to
change credentials. From `src-tauri`, run the opt-in real Broker query:

```powershell
cargo test --lib local_acl_query_accepts_configured_credentials_and_rejects_invalid_credentials -- --ignored
```

The test uses the desktop's common AdminBuilder and checks the seeded user can be
queried with correct credentials, while anonymous and incorrectly signed queries
are rejected. It shuts down each session and the owned client runtime.

Stop with `docker compose down`; volumes are retained. This fixture tests ACL,
not TLS or token-provider behavior.
