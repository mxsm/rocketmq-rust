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

use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::time::Duration;

use rocketmq_runtime::BudgetCapacity;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ProcessMemoryLimit;
use rocketmq_runtime::RateLimit;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_runtime::ResourcePermit;

use crate::proto::v2;
use crate::ProxyError;
use crate::ProxyResult;
use crate::RuntimeConfig;

/// Admission-control permits shared by the neutral gRPC handlers.
#[derive(Clone)]
pub struct ExecutionGuards {
    route: ResourceBudget,
    producer: ResourceBudget,
    consumer: ResourceBudget,
    client_manager: ResourceBudget,
    telemetry_parent: ResourceBudget,
    telemetry_limits: TelemetryQueueLimits,
}

#[derive(Clone, Copy)]
struct TelemetryQueueLimits {
    count: usize,
    bytes: usize,
    rate_per_second: u64,
    max_age: Duration,
}

impl ExecutionGuards {
    /// Builds Proxy execution budgets from validated runtime configuration.
    ///
    /// # Panics
    ///
    /// Panics if automatic process-memory detection fails or a configured
    /// capacity is invalid. Production composition should validate
    /// `RuntimeConfig` before constructing the service.
    pub fn from_config(config: &RuntimeConfig) -> Self {
        Self::try_from_config(config).unwrap_or_else(|error| panic!("invalid proxy runtime resource limits: {error}"))
    }

    pub fn try_from_config(config: &RuntimeConfig) -> ProxyResult<Self> {
        let process_limit = if config.process_memory_limit_bytes == 0 {
            ProcessMemoryLimit::detect()
        } else {
            ProcessMemoryLimit::configured(config.process_memory_limit_bytes)
        }
        .map_err(|error| ProxyError::invalid_metadata(error.to_string()))?;
        let managed_bytes = process_limit
            .fraction(1, 8)
            .map_err(|error| ProxyError::invalid_metadata(error.to_string()))?;
        let managed_bytes = usize::try_from(managed_bytes).unwrap_or(usize::MAX);
        let telemetry_bytes = config.telemetry_queue_bytes.min(managed_bytes.saturating_sub(1)).max(1);
        let control_reserve_bytes = (managed_bytes / 16).max(1).min(telemetry_bytes);
        let data_permits = config
            .route_permits
            .saturating_add(config.producer_permits)
            .saturating_add(config.consumer_permits);
        let control_permits = config
            .client_manager_permits
            .saturating_add(config.telemetry_queue_capacity);
        let total_permits = data_permits.saturating_add(control_permits);
        let tree = ResourceBudgetTree::new(
            "proxy",
            BudgetLimit::new(total_permits, managed_bytes.max(1), FullPolicy::Reject)
                .with_control_reserve(BudgetCapacity::new(control_permits, control_reserve_bytes)),
        )
        .map_err(|error| ProxyError::invalid_metadata(error.to_string()))?;
        let root = tree.root();

        let route = execution_budget(&root, "route", config.route_permits, managed_bytes)?;
        let producer = execution_budget(&root, "producer", config.producer_permits, managed_bytes)?;
        let consumer = execution_budget(&root, "consumer", config.consumer_permits, managed_bytes)?;
        let client_manager = execution_budget(&root, "client-manager", config.client_manager_permits, managed_bytes)?;
        let telemetry_limits = TelemetryQueueLimits {
            count: config.telemetry_queue_capacity,
            bytes: telemetry_bytes,
            rate_per_second: config.telemetry_queue_rate_per_second.max(1),
            max_age: Duration::from_millis(config.telemetry_queue_max_age_ms.max(1)),
        };
        let telemetry_parent = root
            .child(
                "telemetry",
                BudgetLimit::new(telemetry_limits.count, telemetry_limits.bytes, FullPolicy::Reject)
                    .with_rate(RateLimit::new(
                        telemetry_limits.rate_per_second,
                        telemetry_limits.rate_per_second,
                    ))
                    .with_max_age(telemetry_limits.max_age),
            )
            .map_err(|error| ProxyError::invalid_metadata(error.to_string()))?;

        Ok(Self {
            route,
            producer,
            consumer,
            client_manager,
            telemetry_parent,
            telemetry_limits,
        })
    }

    pub fn try_route(&self, retained_bytes: usize) -> ProxyResult<ResourcePermit> {
        self.acquire_data(&self.route, "route", retained_bytes)
    }

    pub fn try_producer(&self, retained_bytes: usize) -> ProxyResult<ResourcePermit> {
        self.acquire_data(&self.producer, "producer", retained_bytes)
    }

    pub fn try_consumer(&self, retained_bytes: usize) -> ProxyResult<ResourcePermit> {
        self.acquire_data(&self.consumer, "consumer", retained_bytes)
    }

    pub fn try_client_manager(&self, retained_bytes: usize) -> ProxyResult<ResourcePermit> {
        self.client_manager
            .try_acquire_control(retained_bytes)
            .map_err(|_| ProxyError::too_many_requests("client-manager"))
    }

    pub fn try_telemetry_command(&self, retained_bytes: usize) -> ProxyResult<ResourcePermit> {
        self.telemetry_parent
            .try_acquire_control(retained_bytes)
            .map_err(|_| ProxyError::too_many_requests("telemetry"))
    }

    pub fn telemetry_queue(&self, client_id: &str) -> ProxyResult<BudgetedQueue<v2::TelemetryCommand>> {
        let mut hasher = DefaultHasher::new();
        client_id.hash(&mut hasher);
        let budget = self
            .telemetry_parent
            .child(
                format!("client-{:016x}", hasher.finish()),
                BudgetLimit::new(
                    self.telemetry_limits.count,
                    self.telemetry_limits.bytes,
                    FullPolicy::CloseSlowConsumer,
                )
                .with_rate(RateLimit::new(
                    self.telemetry_limits.rate_per_second,
                    self.telemetry_limits.rate_per_second,
                ))
                .with_max_age(self.telemetry_limits.max_age),
            )
            .map_err(|error| ProxyError::invalid_metadata(error.to_string()))?;
        Ok(BudgetedQueue::new(budget))
    }

    fn acquire_data(
        &self,
        budget: &ResourceBudget,
        boundary: &'static str,
        retained_bytes: usize,
    ) -> ProxyResult<ResourcePermit> {
        budget
            .try_acquire_data(retained_bytes)
            .map_err(|_| ProxyError::too_many_requests(boundary))
    }
}

#[must_use]
pub fn estimated_protobuf_retained_bytes<M: prost::Message>(message: &M) -> usize {
    std::mem::size_of_val(message).saturating_add(message.encoded_len())
}

fn execution_budget(
    root: &ResourceBudget,
    name: &'static str,
    permits: usize,
    managed_bytes: usize,
) -> ProxyResult<ResourceBudget> {
    root.child(
        name,
        BudgetLimit::new(permits, managed_bytes.max(1), FullPolicy::Reject).with_rate(RateLimit::new(
            u64::try_from(permits).unwrap_or(u64::MAX).max(1),
            u64::try_from(permits).unwrap_or(u64::MAX).max(1),
        )),
    )
    .map_err(|error| ProxyError::invalid_metadata(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn exhausted_boundary_returns_too_many_requests() {
        let guards = ExecutionGuards::from_config(&RuntimeConfig {
            route_permits: 1,
            ..RuntimeConfig::default()
        });
        let _permit = guards.try_route(0).expect("first route permit");

        assert!(matches!(guards.try_route(0), Err(ProxyError::TooManyRequests { .. })));
    }

    #[test]
    fn closing_one_slow_telemetry_consumer_does_not_close_other_clients() {
        let guards = ExecutionGuards::from_config(&RuntimeConfig {
            process_memory_limit_bytes: 64 * 1024 * 1024,
            telemetry_queue_capacity: 1,
            telemetry_queue_bytes: 1024,
            ..RuntimeConfig::default()
        });
        let slow = guards.telemetry_queue("slow-client").expect("slow client queue");
        let fast = guards.telemetry_queue("fast-client").expect("fast client queue");
        let command = || v2::TelemetryCommand {
            status: None,
            command: None,
        };

        slow.try_push_control(command(), 1).expect("first slow command");
        assert!(slow.try_push_control(command(), 1).is_err());
        assert!(slow.is_closed());
        fast.try_push_control(command(), 1)
            .expect("closing the slow client should release the shared parent permit");
        assert!(!fast.is_closed());
    }

    #[test]
    fn protobuf_estimate_includes_encoded_payload_bytes() {
        let command = v2::TelemetryCommand {
            status: None,
            command: Some(v2::telemetry_command::Command::ReconnectEndpointsCommand(
                v2::ReconnectEndpointsCommand {
                    nonce: "nonce-with-retained-bytes".to_owned(),
                },
            )),
        };

        assert!(
            estimated_protobuf_retained_bytes(&command)
                >= std::mem::size_of::<v2::TelemetryCommand>() + command.encoded_len()
        );
    }
}
