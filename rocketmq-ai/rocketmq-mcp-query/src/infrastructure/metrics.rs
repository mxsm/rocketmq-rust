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

use std::fmt;
use std::ops::Deref;

use rocketmq_observability::metrics::mcp::McpMetricsRecorder;

/// MCP metric recorder owned by one component instance.
///
/// The default records nothing, so the cache, rate limiter, and audit log can be built before
/// telemetry exists and still derive `Debug` and `Default`. The application binds them to the
/// telemetry-backed recorder through their `with_metrics` constructors at startup; nothing
/// reads process-global telemetry state.
#[derive(Clone)]
pub(crate) struct ComponentMetrics(McpMetricsRecorder);

impl ComponentMetrics {
    pub(crate) fn new(recorder: McpMetricsRecorder) -> Self {
        Self(recorder)
    }
}

impl Default for ComponentMetrics {
    fn default() -> Self {
        Self(McpMetricsRecorder::noop())
    }
}

impl Deref for ComponentMetrics {
    type Target = McpMetricsRecorder;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Debug for ComponentMetrics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ComponentMetrics").finish_non_exhaustive()
    }
}
