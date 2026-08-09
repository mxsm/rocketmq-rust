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

#![recursion_limit = "256"]

use std::sync::Arc;
use std::time::Duration;

use rocketmq_client_rust::ClientConfig;
use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_client_rust::TelemetryHandle;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

fn runtime_owner(name: &str) -> RuntimeOwner {
    RuntimeOwner::new(RuntimeConfig {
        thread_name: name.to_string(),
        ..Default::default()
    })
    .expect("test runtime owner should start")
}

fn client_runtime(owner: &RuntimeOwner, telemetry_handle: TelemetryHandle) -> Arc<ClientRuntime> {
    ClientRuntime::try_new(
        owner.root_context().component("client"),
        ClientRuntimeConfig::default(),
        telemetry_handle,
    )
    .expect("test client runtime should be valid")
}

#[test]
fn transient_tasks_share_component_owner() {
    const TASKS: usize = 1_024;

    let owner = runtime_owner("client-runtime-transient-owner");
    let runtime = client_runtime(&owner, TelemetryHandle::noop());
    let component = runtime.component("transient-tasks");
    let task_group = component.task_group().clone();
    let baseline_children = task_group.component_count();

    owner.block_on(async {
        let mut task_ids = Vec::with_capacity(TASKS);
        for _ in 0..TASKS {
            task_ids.push(
                task_group
                    .spawn_service("transient-client-task", async {})
                    .expect("transient task should spawn"),
            );
        }
        for task_id in task_ids {
            assert!(task_group.wait_task(task_id, Duration::from_secs(1)).await);
        }

        assert_eq!(task_group.task_count(), 0);
        assert_eq!(task_group.component_count(), baseline_children);
        let report = runtime.shutdown().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
    owner
        .shutdown_runtime_blocking()
        .expect("runtime should shut down cleanly");
}

#[test]
fn noop_telemetry_keeps_runtime_metrics_disabled() {
    let owner = runtime_owner("client-runtime-noop-telemetry");
    let runtime = client_runtime(&owner, TelemetryHandle::noop());
    let lease = runtime
        .pool()
        .get_or_create(ClientConfig::default(), None)
        .expect("client lease should be created");

    assert!(!runtime.telemetry_handle().is_active());
    assert!(!runtime.client_metrics().is_enabled());

    owner.block_on(async {
        assert!(runtime.pool().release(lease.into_parts().1).await);
        let report = runtime.shutdown().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
    owner
        .shutdown_runtime_blocking()
        .expect("runtime should shut down cleanly");
}

#[cfg(feature = "observability-metrics")]
#[test]
fn two_client_runtimes_keep_metrics_lifecycle_isolated() {
    use std::time::Duration;

    use rocketmq_observability::MetricsExporter;
    use rocketmq_observability::ObservabilityConfig;

    fn metrics_config() -> ObservabilityConfig {
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.metrics.enabled = true;
        config.metrics.exporter = MetricsExporter::Disable;
        config
    }

    let first_guard =
        rocketmq_observability::init_observability(&metrics_config()).expect("first telemetry runtime should start");
    let second_guard =
        rocketmq_observability::init_observability(&metrics_config()).expect("second telemetry runtime should start");
    let first_owner = runtime_owner("client-metrics-isolation-first");
    let second_owner = runtime_owner("client-metrics-isolation-second");
    let first_runtime = client_runtime(&first_owner, first_guard.handle());
    let second_runtime = client_runtime(&second_owner, second_guard.handle());
    let first_lease = first_runtime
        .pool()
        .get_or_create(ClientConfig::default(), None)
        .expect("first client lease should be created");
    let second_lease = second_runtime
        .pool()
        .get_or_create(ClientConfig::default(), None)
        .expect("second client lease should be created");

    assert!(first_runtime.client_metrics().is_enabled());
    assert!(second_runtime.client_metrics().is_enabled());
    first_runtime.client_metrics().record_send(Duration::from_millis(1));
    second_runtime.client_metrics().record_send(Duration::from_millis(1));

    first_guard
        .shutdown()
        .into_result()
        .expect("first telemetry runtime should shut down");

    assert!(!first_runtime.client_metrics().is_enabled());
    assert!(second_runtime.client_metrics().is_enabled());
    second_runtime.client_metrics().record_send(Duration::from_millis(1));

    first_owner.block_on(async {
        assert!(first_runtime.pool().release(first_lease.into_parts().1).await);
        let report = first_runtime.shutdown().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
    second_owner.block_on(async {
        assert!(second_runtime.pool().release(second_lease.into_parts().1).await);
        let report = second_runtime.shutdown().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
    second_guard
        .shutdown()
        .into_result()
        .expect("second telemetry runtime should shut down");
    first_owner
        .shutdown_runtime_blocking()
        .expect("first runtime should shut down cleanly");
    second_owner
        .shutdown_runtime_blocking()
        .expect("second runtime should shut down cleanly");
}

#[test]
fn client_pool_reuses_instances_only_within_one_explicit_runtime() {
    let first_owner = runtime_owner("client-runtime-isolation-first");
    let second_owner = runtime_owner("client-runtime-isolation-second");
    let first_runtime = client_runtime(&first_owner, TelemetryHandle::noop());
    let second_runtime = client_runtime(&second_owner, TelemetryHandle::noop());
    let config = ClientConfig::default();

    let first_lease = first_runtime
        .pool()
        .get_or_create(config.clone(), None)
        .expect("first client lease should be created");
    let shared_lease = first_runtime
        .pool()
        .get_or_create(config.clone(), None)
        .expect("matching client config should reuse the runtime-local instance");
    let isolated_lease = second_runtime
        .pool()
        .get_or_create(config, None)
        .expect("a second explicit runtime should create an isolated instance");

    assert!(Arc::ptr_eq(first_lease.instance(), shared_lease.instance()));
    assert!(!Arc::ptr_eq(first_lease.instance(), isolated_lease.instance()));

    first_owner.block_on(async {
        assert!(!first_runtime.pool().release(first_lease.into_parts().1).await);
        assert!(first_runtime.pool().release(shared_lease.into_parts().1).await);
        let report = first_runtime.shutdown().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
    second_owner.block_on(async {
        assert!(second_runtime.pool().release(isolated_lease.into_parts().1).await);
        let report = second_runtime.shutdown().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });

    first_owner
        .shutdown_runtime_blocking()
        .expect("first runtime should shut down cleanly");
    second_owner
        .shutdown_runtime_blocking()
        .expect("second runtime should shut down cleanly");
}

#[test]
fn client_pool_rejects_conflicting_configuration_for_the_same_client_id() {
    let owner = runtime_owner("client-runtime-conflict");
    let runtime = client_runtime(&owner, TelemetryHandle::noop());
    let first_config = ClientConfig::default();
    let mut conflicting_config = first_config.clone();
    conflicting_config.namesrv_addr = Some("127.0.0.1:19876".into());

    let lease = runtime
        .pool()
        .get_or_create(first_config, None)
        .expect("first client lease should be created");
    let error = match runtime.pool().get_or_create(conflicting_config, None) {
        Ok(_) => panic!("a conflicting owner for the same client id must be rejected"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("configuration conflicts"));

    owner.block_on(async {
        assert!(runtime.pool().release(lease.into_parts().1).await);
        let report = runtime.shutdown().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
    owner
        .shutdown_runtime_blocking()
        .expect("runtime should shut down cleanly");
}

#[test]
fn shutting_down_client_runtime_closes_pool_admission() {
    let owner = runtime_owner("client-runtime-shutdown");
    let runtime = client_runtime(&owner, TelemetryHandle::noop());

    owner.block_on(async {
        let report = runtime.shutdown().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });

    let error = match runtime.pool().get_or_create(ClientConfig::default(), None) {
        Ok(_) => panic!("shutdown must close new client admission"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("shutting down"));
    assert!(
        runtime
            .pool()
            .get_or_create_produce_accumulator(ClientConfig::default())
            .is_none(),
        "shutdown must close accumulator admission"
    );

    owner
        .shutdown_runtime_blocking()
        .expect("runtime should shut down cleanly");
}
