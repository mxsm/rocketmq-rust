# RocketMQ MCP

`rocketmq-mcp` is the Model Context Protocol server for RocketMQ-Rust AI SRE and diagnostics workflows.

The initial crate only defines the package boundary, feature flags, configuration examples, and binary entry point. Later staged changes add stdio transport, read-only tools, resources, prompts, guard logic, and optional HTTP transport.

## Scope

- Default mode is read-only diagnostics over stdio.
- Change tools are disabled unless both compile-time features and runtime configuration allow them.
- The server runs as an independent tool process and does not enter the broker, namesrv, or store runtime path.

## Build

```bash
cargo check -p rocketmq-mcp
```
