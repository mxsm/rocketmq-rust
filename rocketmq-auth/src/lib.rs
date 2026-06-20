// Copyright 2023 The RocketMQ Rust Authors
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

#![allow(dead_code)]

pub mod acl;
pub mod authentication;
pub mod authorization;
pub mod config;
pub mod migration;
pub mod permission;
pub mod runtime;
pub(crate) mod runtime_bridge;

// Re-export commonly used authentication types
pub use authentication::acl_signer::SignatureAlgorithm;
pub use authentication::context::default_authentication_context::DefaultAuthenticationContext;
pub use authentication::evaluator::AuthenticationEvaluator;
pub use authentication::factory::AuthenticationFactory;
pub use authentication::provider::AuthenticationMetadataProvider;
pub use authentication::provider::AuthenticationProvider;
pub use authentication::provider::DefaultAuthenticationProvider;
pub use authentication::strategy::AllowAllAuthenticationStrategy;
pub use authentication::strategy::AuthenticationStrategy;

// Re-export commonly used authorization types
pub use authorization::context::default_authorization_context::DefaultAuthorizationContext;
pub use authorization::evaluator::AuthorizationEvaluator;
pub use authorization::factory::AuthorizationFactory;
pub use authorization::provider::AuthorizationProvider;
pub use authorization::strategy::abstract_authorization_strategy::AuthorizationStrategy;
pub use rocketmq_observability::metrics::auth::AuthMetricSample;
pub use rocketmq_observability::metrics::auth::AuthMetrics;
pub use rocketmq_observability::metrics::auth::AuthMetricsSnapshot;
pub use runtime::AuthRuntime;
pub use runtime::AuthRuntimeBuilder;
pub use runtime::AuthenticationService;
pub use runtime::AuthorizationService;
pub use runtime::ProviderRegistry;

#[doc(hidden)]
pub mod bench_support {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use std::time::Instant;

    use cheetah_string::CheetahString;
    use rocketmq_error::RocketMQResult;
    use rocketmq_runtime::ShutdownReport;
    use serde::Serialize;

    use crate::authentication::provider::authentication_metadata_provider::AuthenticationMetadataProvider;
    use crate::config::AuthConfig;
    use crate::runtime::AuthRuntimeBuilder;

    static NEXT_ACL_WATCHER_PROBE_ID: AtomicU64 = AtomicU64::new(0);

    #[derive(Clone, Copy, Debug, Default, Serialize)]
    pub struct AuthSyncBridgeCounterSnapshot {
        pub sync_bridge_calls: u64,
        pub multi_thread_block_in_place: u64,
        pub current_thread_handoffs: u64,
        pub fallback_runtime_calls: u64,
        pub shared_runtime_acquires: u64,
        pub shared_runtime_created: u64,
        pub shared_runtime_reused: u64,
        pub shared_runtime_available: bool,
    }

    #[derive(Clone, Copy, Debug, Default, Serialize)]
    pub struct AuthSyncBridgeCounterDelta {
        pub sync_bridge_calls: u64,
        pub multi_thread_block_in_place: u64,
        pub current_thread_handoffs: u64,
        pub fallback_runtime_calls: u64,
        pub shared_runtime_acquires: u64,
        pub shared_runtime_created: u64,
        pub shared_runtime_reused: u64,
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct AuthSyncBridgeProbe {
        pub case: &'static str,
        pub call_count: usize,
        pub elapsed_us: u128,
        pub before: AuthSyncBridgeCounterSnapshot,
        pub after: AuthSyncBridgeCounterSnapshot,
        pub delta: AuthSyncBridgeCounterDelta,
        pub healthy: bool,
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct AuthAclWatcherLifecycleProbe {
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub reload_success: bool,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: Option<ShutdownReport>,
        pub healthy: bool,
    }

    pub async fn run_auth_acl_watcher_lifecycle_probe() -> RocketMQResult<AuthAclWatcherLifecycleProbe> {
        let root = unique_acl_watcher_probe_root();
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).map_err(|error| rocketmq_error::RocketMQError::Internal(error.to_string()))?;
        let acl_file = root.join("plain_acl.yml");
        write_acl_file(&acl_file, "first")?;

        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            acl_file_watch_enabled: true,
            acl_file_watch_interval_millis: 5,
            ..AuthConfig::default()
        })
        .build()
        .await?;
        let authn_provider = runtime.provider_registry().authentication_metadata_provider();
        write_acl_file(&acl_file, "second")?;

        let deadline = Instant::now() + Duration::from_secs(2);
        let reload_success = loop {
            let user = authn_provider.get_user("alice").await?;
            if user.password().map(|value| value.as_str()) == Some("second") {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };

        let mut snapshots = runtime.acl_file_watcher_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = runtime.acl_file_watcher_snapshot();
        }
        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();

        let shutdown_started_at = Instant::now();
        let shutdown_report = runtime.shutdown_with_report().await?;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = reload_success
            && scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && shutdown_healthy;

        let _ = fs::remove_dir_all(root);
        Ok(AuthAclWatcherLifecycleProbe {
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            reload_success,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        })
    }

    pub fn run_auth_sync_bridge_no_runtime_probe(call_count: usize) -> AuthSyncBridgeProbe {
        run_probe("no_runtime_shared_fallback", call_count, || {
            run_sync_bridge_calls(call_count);
        })
    }

    pub fn run_auth_sync_bridge_current_thread_probe(call_count: usize) -> AuthSyncBridgeProbe {
        run_probe("current_thread_handoff", call_count, || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("auth sync bridge current-thread probe runtime should build");
            let _guard = runtime.enter();
            run_sync_bridge_calls(call_count);
        })
    }

    pub fn run_auth_sync_bridge_multi_thread_probe(call_count: usize) -> AuthSyncBridgeProbe {
        run_probe("multi_thread_block_in_place", call_count, || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("auth sync bridge multi-thread probe runtime should build");
            runtime.block_on(async move {
                tokio::spawn(async move {
                    run_sync_bridge_calls(call_count);
                })
                .await
                .expect("auth sync bridge multi-thread probe task should join");
            });
        })
    }

    fn run_probe(case: &'static str, call_count: usize, run: impl FnOnce()) -> AuthSyncBridgeProbe {
        let before = snapshot();
        let started_at = Instant::now();
        run();
        let elapsed_us = started_at.elapsed().as_micros();
        let after = snapshot();
        let delta = after.delta_since(before);
        let healthy = match case {
            "no_runtime_shared_fallback" => {
                delta.sync_bridge_calls == call_count as u64
                    && delta.fallback_runtime_calls == call_count as u64
                    && delta.shared_runtime_acquires == call_count as u64
                    && delta.shared_runtime_created <= 1
                    && after.shared_runtime_available
            }
            "current_thread_handoff" => {
                delta.sync_bridge_calls == call_count as u64
                    && delta.current_thread_handoffs == call_count as u64
                    && delta.multi_thread_block_in_place == 0
                    && delta.shared_runtime_acquires == call_count as u64
                    && after.shared_runtime_available
            }
            "multi_thread_block_in_place" => {
                delta.sync_bridge_calls == call_count as u64
                    && delta.multi_thread_block_in_place == call_count as u64
                    && delta.current_thread_handoffs == 0
                    && delta.shared_runtime_acquires == 0
            }
            _ => false,
        };

        AuthSyncBridgeProbe {
            case,
            call_count,
            elapsed_us,
            before,
            after,
            delta,
            healthy,
        }
    }

    fn run_sync_bridge_calls(call_count: usize) {
        for task_index in 0..call_count {
            let value = crate::runtime_bridge::block_on_sync_bridge(
                || async move { Ok::<usize, String>(task_index) },
                |error| error,
                || "auth sync bridge thread panicked".to_string(),
            )
            .expect("auth sync bridge probe call should complete");
            assert_eq!(value, task_index);
        }
    }

    fn snapshot() -> AuthSyncBridgeCounterSnapshot {
        crate::runtime_bridge::auth_sync_bridge_snapshot().into()
    }

    fn unique_acl_watcher_probe_root() -> PathBuf {
        let id = NEXT_ACL_WATCHER_PROBE_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("rocketmq-auth-acl-watcher-{}-{id}", std::process::id()))
    }

    fn write_acl_file(path: &std::path::Path, secret: &str) -> RocketMQResult<()> {
        fs::write(
            path,
            format!(
                r#"
accounts:
  - accessKey: alice
    secretKey: {secret}
"#
            ),
        )
        .map_err(|error| rocketmq_error::RocketMQError::Internal(error.to_string()))
    }

    impl From<crate::runtime_bridge::AuthSyncBridgeSnapshot> for AuthSyncBridgeCounterSnapshot {
        fn from(snapshot: crate::runtime_bridge::AuthSyncBridgeSnapshot) -> Self {
            Self {
                sync_bridge_calls: snapshot.sync_bridge_calls,
                multi_thread_block_in_place: snapshot.multi_thread_block_in_place,
                current_thread_handoffs: snapshot.current_thread_handoffs,
                fallback_runtime_calls: snapshot.fallback_runtime_calls,
                shared_runtime_acquires: snapshot.shared_runtime_acquires,
                shared_runtime_created: snapshot.shared_runtime_created,
                shared_runtime_reused: snapshot.shared_runtime_reused,
                shared_runtime_available: snapshot.shared_runtime_available,
            }
        }
    }

    impl AuthSyncBridgeCounterSnapshot {
        fn delta_since(self, before: Self) -> AuthSyncBridgeCounterDelta {
            AuthSyncBridgeCounterDelta {
                sync_bridge_calls: self.sync_bridge_calls.saturating_sub(before.sync_bridge_calls),
                multi_thread_block_in_place: self
                    .multi_thread_block_in_place
                    .saturating_sub(before.multi_thread_block_in_place),
                current_thread_handoffs: self
                    .current_thread_handoffs
                    .saturating_sub(before.current_thread_handoffs),
                fallback_runtime_calls: self
                    .fallback_runtime_calls
                    .saturating_sub(before.fallback_runtime_calls),
                shared_runtime_acquires: self
                    .shared_runtime_acquires
                    .saturating_sub(before.shared_runtime_acquires),
                shared_runtime_created: self
                    .shared_runtime_created
                    .saturating_sub(before.shared_runtime_created),
                shared_runtime_reused: self.shared_runtime_reused.saturating_sub(before.shared_runtime_reused),
            }
        }
    }
}

#[cfg(test)]
mod bench_support_tests {
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auth_acl_watcher_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_auth_acl_watcher_lifecycle_probe()
            .await
            .expect("auth ACL watcher lifecycle probe should run");

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.reload_success, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }
}
