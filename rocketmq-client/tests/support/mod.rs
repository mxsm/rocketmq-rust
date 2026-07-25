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
use std::sync::LazyLock;

use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

static TEST_RUNTIME_OWNER: LazyLock<RuntimeOwner> = LazyLock::new(|| {
    RuntimeOwner::new(RuntimeConfig {
        thread_name: "rocketmq-client-integration-test".to_string(),
        ..Default::default()
    })
    .expect("client integration-test runtime owner should start")
});

pub fn client_runtime(scope: impl Into<rocketmq_runtime::ScopeId>) -> Arc<ClientRuntime> {
    ClientRuntime::new(
        TEST_RUNTIME_OWNER.root_context().child(scope),
        ClientRuntimeConfig::default(),
    )
}
