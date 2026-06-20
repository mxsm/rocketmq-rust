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
    use std::time::Instant;

    use serde::Serialize;

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
