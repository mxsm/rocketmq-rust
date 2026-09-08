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

use std::time::Duration;

use rocketmq_proxy::{ProxyConfig, ProxyRuntime};
use rocketmq_runtime::RuntimeContext;

#[tokio::test]
async fn invalid_health_policy_fails_before_backend_activation() {
    let runtime = RuntimeContext::from_current("invalid-health-policy");
    let context = runtime.service_context("proxy");
    let mut config = ProxyConfig::default();
    config.dependency_health.failure_threshold = 0;
    let result = ProxyRuntime::new(config, context.clone(), rocketmq_observability::TelemetryHandle::noop());
    assert!(result.is_err());
    assert_eq!(context.task_group().component_count(), 0);
    assert!(runtime.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
}

#[cfg(not(all(feature = "local-mode", feature = "cluster-mode")))]
#[tokio::test]
async fn missing_backend_features_fail_explicitly_without_starting_work() {
    use rocketmq_proxy::{ProxyError, ProxyMode};
    let runtime = RuntimeContext::from_current("missing-backend-feature");
    for (mode, available) in [
        (ProxyMode::Local, cfg!(feature = "local-mode")),
        (ProxyMode::Cluster, cfg!(feature = "cluster-mode")),
    ] {
        if available {
            continue;
        }
        let context = runtime.service_context(match mode {
            ProxyMode::Local => "proxy-local",
            ProxyMode::Cluster => "proxy-cluster",
        });
        let result = ProxyRuntime::new(
            ProxyConfig {
                mode,
                ..Default::default()
            },
            context.clone(),
            rocketmq_observability::TelemetryHandle::noop(),
        );
        assert!(matches!(result, Err(ProxyError::NotImplemented { .. })));
        assert_eq!(context.task_group().component_count(), 0);
    }
    assert!(runtime.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
}
