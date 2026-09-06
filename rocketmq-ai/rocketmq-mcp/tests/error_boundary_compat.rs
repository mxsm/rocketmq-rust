// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;

use rocketmq_mcp::app;
use rocketmq_mcp::app::McpApp;
use rocketmq_mcp::app::ValidatedMcpBootstrap;
use rocketmq_mcp::config::McpConfig;
use rocketmq_mcp::transport;
use rocketmq_mcp::McpError;
use rocketmq_mcp::McpResult;

#[test]
fn canonical_public_error_boundary_compiles() {
    assert_error::<McpError>();

    let _ = bootstrap_returns_mcp_result;
    let _ = bootstrap_validated_returns_mcp_result;
    let _ = init_tracing_returns_mcp_result;
    let _ = stdio_serve_returns_mcp_result;
    let _ = stdio_serve_with_lifecycle_returns_mcp_result;

    #[cfg(feature = "streamable-http")]
    {
        let _ = streamable_http_serve_returns_mcp_result;
        let _ = streamable_http_serve_with_lifecycle_returns_mcp_result;
        let _ = build_router_returns_mcp_result;
    }
}

#[test]
fn readme_documents_only_the_canonical_error_boundary() {
    const README: &str = include_str!("../README.md");

    for documented_contract in [
        "`McpResult<T>`",
        "`McpError` is the single opaque operational-error facade",
        "There are no alternate or compatibility error-return APIs.",
        "fixed, redacted public errors",
        "stdout contains MCP JSON-RPC protocol frames only",
    ] {
        assert!(
            README.contains(documented_contract),
            "README must document {documented_contract:?}"
        );
    }
}

fn assert_error<T: std::error::Error>() {}

fn assert_mcp_result_future<T>(_: impl Future<Output = McpResult<T>>) {}

fn bootstrap_returns_mcp_result(
    config: McpConfig,
    process_telemetry: rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig,
    security_bootstrap: rocketmq_security_api::SecurityBootstrapOutcome,
    service_context: rocketmq_runtime::ChildServiceContext,
) {
    assert_mcp_result_future(McpApp::bootstrap(
        config,
        process_telemetry,
        security_bootstrap,
        service_context,
    ));
}

fn bootstrap_validated_returns_mcp_result(
    handoff: ValidatedMcpBootstrap,
    service_context: rocketmq_runtime::ChildServiceContext,
) {
    assert_mcp_result_future(McpApp::bootstrap_validated(handoff, service_context));
}

fn init_tracing_returns_mcp_result(
    config: &McpConfig,
    process_telemetry: &rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig,
    service_context: &rocketmq_runtime::ChildServiceContext,
) {
    assert_mcp_result_future(app::init_tracing(config, process_telemetry, service_context));
}

fn stdio_serve_returns_mcp_result(app: McpApp) {
    assert_mcp_result_future(transport::stdio::serve(app));
}

fn stdio_serve_with_lifecycle_returns_mcp_result(app: McpApp, lifecycle: rocketmq_runtime::ServiceLifecycle) {
    assert_mcp_result_future(transport::stdio::serve_with_lifecycle(app, lifecycle));
}

#[cfg(feature = "streamable-http")]
fn streamable_http_serve_returns_mcp_result(app: McpApp) {
    assert_mcp_result_future(transport::streamable_http::serve(app));
}

#[cfg(feature = "streamable-http")]
fn streamable_http_serve_with_lifecycle_returns_mcp_result(app: McpApp, lifecycle: rocketmq_runtime::ServiceLifecycle) {
    assert_mcp_result_future(transport::streamable_http::serve_with_lifecycle(app, lifecycle));
}

#[cfg(feature = "streamable-http")]
fn build_router_returns_mcp_result(app: McpApp, cancellation: tokio_util::sync::CancellationToken) {
    let _: McpResult<_> = transport::streamable_http::build_router(app, cancellation);
}
