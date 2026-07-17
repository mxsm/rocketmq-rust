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

use rocketmq_mcp::app;
use rocketmq_mcp::app::McpApp;
use rocketmq_mcp::transport;

#[test]
#[allow(
    deprecated,
    reason = "compile fixture proves the R0 compatibility surface remains exported"
)]
fn typed_and_legacy_error_boundaries_are_both_exported() {
    let _ = McpApp::bootstrap_typed;
    let _ = McpApp::bootstrap;
    let _ = app::init_tracing_typed;
    let _ = app::init_tracing;
    let _ = transport::stdio::serve_typed;
    let _ = transport::stdio::serve;

    #[cfg(feature = "streamable-http")]
    {
        let _ = transport::streamable_http::serve_typed;
        let _ = transport::streamable_http::serve;
        let _ = transport::streamable_http::build_router_typed;
        let _ = transport::streamable_http::build_router;
    }
}
