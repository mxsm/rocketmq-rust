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

use crate::config::backend::LocalBackendConfig;
use crate::message_store::cleanup::CleanupPolicy;
use crate::message_store::lifecycle::LocalStoreLifecycle;
use crate::message_store::query::QueryPolicy;
use crate::message_store::reput::ReputPolicy;

/// Runtime-neutral composition root for the local-file message store.
///
/// Concrete CommitLog, ConsumeQueue, scheduler, and broker adapters remain in
/// the facade crate. This root owns only local-store state and decisions.
pub struct LocalStoreComposition {
    config: LocalBackendConfig,
    lifecycle: LocalStoreLifecycle,
    query: QueryPolicy,
    reput: ReputPolicy,
    cleanup: CleanupPolicy,
}

impl LocalStoreComposition {
    pub fn new(config: LocalBackendConfig) -> Self {
        let query = QueryPolicy::new(config.query);
        let reput = ReputPolicy::new(config.reput);
        let cleanup = CleanupPolicy::new(config.cleanup);
        Self {
            config,
            lifecycle: LocalStoreLifecycle::new(),
            query,
            reput,
            cleanup,
        }
    }

    pub const fn config(&self) -> &LocalBackendConfig {
        &self.config
    }

    pub fn lifecycle(&self) -> &LocalStoreLifecycle {
        &self.lifecycle
    }

    pub const fn query(&self) -> QueryPolicy {
        self.query
    }

    pub const fn reput(&self) -> ReputPolicy {
        self.reput
    }

    pub const fn cleanup(&self) -> CleanupPolicy {
        self.cleanup
    }
}
