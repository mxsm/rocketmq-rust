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

use rocketmq_client_rust::ClientConfig;
use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_client_rust::TelemetryHandle;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::remoting_command::SERIALIZE_TYPE_PROPERTY;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

#[test]
fn client_pool_rejects_invalid_remoting_serialization_before_admission() {
    // SAFETY: this integration-test binary contains one test, so no concurrent thread can read or
    // mutate this process environment key while the assertion runs.
    unsafe {
        std::env::set_var(SERIALIZE_TYPE_PROPERTY, "CBOR");
    }

    let owner = RuntimeOwner::new(RuntimeConfig {
        thread_name: "remoting-defaults-startup-test".to_string(),
        ..Default::default()
    })
    .expect("test runtime owner should start");
    let runtime = ClientRuntime::try_new(
        owner.root_context().component("client"),
        ClientRuntimeConfig::default(),
        TelemetryHandle::noop(),
    )
    .expect("client runtime should be valid");

    let error = match runtime.pool().get_or_create(ClientConfig::default(), None) {
        Ok(_) => panic!("invalid remoting serialization must reject client admission"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        RocketMQError::ConfigParseFailed {
            key: "remoting.command.defaults",
            ..
        }
    ));
    assert_eq!(runtime.pool().instance_count(), 0);

    owner
        .shutdown_runtime_blocking()
        .expect("test runtime should shut down cleanly");
}
