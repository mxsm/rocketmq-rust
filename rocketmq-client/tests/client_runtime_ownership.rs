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

use std::sync::Arc;

use rocketmq_client_rust::base::client_config::ClientConfig;
use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

fn runtime_owner(name: &str) -> RuntimeOwner {
    RuntimeOwner::new(RuntimeConfig {
        thread_name: name.to_string(),
        ..Default::default()
    })
    .expect("test runtime owner should start")
}

#[test]
fn client_pool_reuses_instances_only_within_one_explicit_runtime() {
    let first_owner = runtime_owner("client-runtime-isolation-first");
    let second_owner = runtime_owner("client-runtime-isolation-second");
    let first_runtime = ClientRuntime::new(
        first_owner.root_context().child("client"),
        ClientRuntimeConfig::default(),
    );
    let second_runtime = ClientRuntime::new(
        second_owner.root_context().child("client"),
        ClientRuntimeConfig::default(),
    );
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
    let runtime = ClientRuntime::new(owner.root_context().child("client"), ClientRuntimeConfig::default());
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
    let runtime = ClientRuntime::new(owner.root_context().child("client"), ClientRuntimeConfig::default());

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
