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

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::ProxyError;
use crate::ProxyResult;
use crate::RuntimeConfig;

/// Admission-control permits shared by the neutral gRPC handlers.
#[derive(Clone)]
pub struct ExecutionGuards {
    route: Arc<Semaphore>,
    producer: Arc<Semaphore>,
    consumer: Arc<Semaphore>,
    client_manager: Arc<Semaphore>,
}

impl ExecutionGuards {
    pub fn from_config(config: &RuntimeConfig) -> Self {
        Self {
            route: Arc::new(Semaphore::new(config.route_permits)),
            producer: Arc::new(Semaphore::new(config.producer_permits)),
            consumer: Arc::new(Semaphore::new(config.consumer_permits)),
            client_manager: Arc::new(Semaphore::new(config.client_manager_permits)),
        }
    }

    pub fn try_route(&self) -> ProxyResult<OwnedSemaphorePermit> {
        self.acquire(&self.route, "route")
    }

    pub fn try_producer(&self) -> ProxyResult<OwnedSemaphorePermit> {
        self.acquire(&self.producer, "producer")
    }

    pub fn try_consumer(&self) -> ProxyResult<OwnedSemaphorePermit> {
        self.acquire(&self.consumer, "consumer")
    }

    pub fn try_client_manager(&self) -> ProxyResult<OwnedSemaphorePermit> {
        self.acquire(&self.client_manager, "client-manager")
    }

    fn acquire(&self, semaphore: &Arc<Semaphore>, boundary: &'static str) -> ProxyResult<OwnedSemaphorePermit> {
        semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| ProxyError::too_many_requests(boundary))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_boundary_returns_too_many_requests() {
        let guards = ExecutionGuards::from_config(&RuntimeConfig {
            route_permits: 1,
            ..RuntimeConfig::default()
        });
        let _permit = guards.try_route().expect("first route permit");

        assert!(matches!(guards.try_route(), Err(ProxyError::TooManyRequests { .. })));
    }
}
