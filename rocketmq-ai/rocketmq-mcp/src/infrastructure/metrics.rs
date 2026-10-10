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

#[cfg(test)]
mod tests {
    /// The free functions in `rocketmq_observability::metrics::mcp` never read telemetry state,
    /// so a call to one of them compiles and silently records nothing.
    #[test]
    fn sources_never_call_the_no_op_metric_helpers() {
        let helper_path = ["metrics::mcp", "::record_"].concat();
        let mut pending = vec![std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src")];
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(&directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    let source = std::fs::read_to_string(&path).unwrap();
                    assert!(
                        !source.contains(&helper_path),
                        "{} calls a no-op MCP metric helper; use a bound recorder",
                        path.display()
                    );
                }
            }
        }
    }
}
