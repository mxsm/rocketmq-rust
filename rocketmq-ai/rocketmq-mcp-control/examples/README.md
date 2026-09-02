# Secure startup example

1. Copy `../conf/mcp-control.example.toml` outside the source tree.
2. Set the TLS certificate/key, HTTPS issuer/JWKS, exact audience, closed allowlists, and a protected audit path.
3. Set `ROCKETMQ_MCP_CONTROL_CONFIG` to that file and start `rocketmq-mcp-control`.

From this standalone project on PowerShell, after provisioning the referenced TLS/JWKS trust files and creating
the audit file's parent directory:

```powershell
$env:ROCKETMQ_MCP_CONTROL_CONFIG = (Resolve-Path .\conf\mcp-control.example.toml).Path
cargo run --locked
```

On a Unix shell:

```bash
ROCKETMQ_MCP_CONTROL_CONFIG="$(pwd)/conf/mcp-control.example.toml" cargo run --locked
```

The example intentionally leaves `mutations_enabled = false` and both allowlists empty. It also demonstrates a
private logical cluster registry whose credential values come only from environment variables. The default
build never links Admin Core. To make reviewed tools discoverable, compile with `--features write-tools`, enable
the runtime policy, and intersect one or both of `topic_upsert` and `consumer_group_upsert` plus the logical
cluster in both the server policy and authenticated claims. Any other operation remains unavailable.
