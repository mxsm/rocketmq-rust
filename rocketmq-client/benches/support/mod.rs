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

use std::future::Future;
use std::sync::Arc;

use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_client_rust::TelemetryHandle;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

pub struct BenchClientRuntime {
    owner: RuntimeOwner,
    client_runtime: Arc<ClientRuntime>,
}

#[allow(
    dead_code,
    reason = "each standalone benchmark target uses only the fixture methods it needs"
)]
impl BenchClientRuntime {
    pub fn new(scope: &str) -> Self {
        let owner = RuntimeOwner::new(RuntimeConfig {
            thread_name: format!("rocketmq-client-bench-{scope}"),
            ..Default::default()
        })
        .expect("benchmark runtime owner should start");
        let client_runtime = ClientRuntime::try_new(
            owner.root_context().child("client"),
            ClientRuntimeConfig::default(),
            TelemetryHandle::noop(),
        )
        .expect("benchmark client runtime should be valid");
        Self { owner, client_runtime }
    }

    pub fn client_runtime(&self) -> Arc<ClientRuntime> {
        Arc::clone(&self.client_runtime)
    }

    pub fn child(&self, scope: impl Into<rocketmq_runtime::ScopeId>) -> ChildServiceContext {
        self.client_runtime.child(scope)
    }

    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.owner.block_on(future)
    }

    pub fn shutdown(self) {
        let client_report = self.owner.block_on(self.client_runtime.shutdown());
        assert!(client_report.is_healthy(), "{}", client_report.to_json());
        let owner_report = self
            .owner
            .shutdown_runtime_blocking()
            .expect("benchmark runtime should shut down");
        assert!(owner_report.is_healthy(), "{}", owner_report.to_json());
    }
}
