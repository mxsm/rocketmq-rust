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

//! Deterministic safe-DTO fake for Topic product-path tests.

use std::{collections::VecDeque, sync::Mutex};

use rocketmq_dashboard_common::{
    TopicConfigView, TopicConsumersView, TopicIdentity, TopicInventory, TopicMessageType, TopicPartialOutcome,
    TopicPermission, TopicRouteView, TopicStatsView, TopicTargetIdentity,
};

use super::{
    BackendTopicQueuePatchResult, ServiceFuture, TopicBackend, TopicCreateCommand, TopicDeleteBrokerCommand,
    TopicDeleteCommand, TopicOffsetCommand, TopicQueuePatchCommand, TopicRequestScope, TopicSendCommand,
};
use crate::state::{UiError, UiErrorCode};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TopicCreateCall {
    pub scope: TopicRequestScope,
    pub topic: TopicIdentity,
    pub targets: Vec<TopicTargetIdentity>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub permission: TopicPermission,
    pub ordered: bool,
    pub message_type: TopicMessageType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TopicPatchCall {
    pub scope: TopicRequestScope,
    pub topic: TopicIdentity,
    pub target: TopicTargetIdentity,
    pub expected_version: u64,
    pub read_queue_count: Option<u32>,
    pub write_queue_count: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TopicSendCall {
    pub scope: TopicRequestScope,
    pub topic: TopicIdentity,
    pub has_key: bool,
    pub has_tag: bool,
    pub body_length: usize,
    pub trace_enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TopicOffsetCall {
    pub scope: TopicRequestScope,
    pub topic: TopicIdentity,
    pub consumer_group: String,
    pub cluster_name: String,
    pub timestamp: Option<u64>,
    pub force: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TopicCalls {
    pub inventory: Vec<TopicRequestScope>,
    pub route: Vec<(TopicRequestScope, TopicIdentity)>,
    pub stats: Vec<(TopicRequestScope, TopicIdentity)>,
    pub config: Vec<(TopicRequestScope, TopicIdentity)>,
    pub consumers: Vec<(TopicRequestScope, TopicIdentity)>,
    pub create: Vec<TopicCreateCall>,
    pub patch: Vec<TopicPatchCall>,
    pub delete: Vec<(TopicRequestScope, TopicIdentity, Vec<String>)>,
    pub delete_broker: Vec<(TopicRequestScope, TopicIdentity, TopicTargetIdentity)>,
    pub send: Vec<TopicSendCall>,
    pub reset: Vec<TopicOffsetCall>,
    pub skip: Vec<TopicOffsetCall>,
}

#[derive(Default)]
struct Queues {
    inventory: VecDeque<Result<TopicInventory, UiError>>,
    route: VecDeque<Result<TopicRouteView, UiError>>,
    stats: VecDeque<Result<TopicStatsView, UiError>>,
    config: VecDeque<Result<TopicConfigView, UiError>>,
    consumers: VecDeque<Result<TopicConsumersView, UiError>>,
    create: VecDeque<Result<TopicPartialOutcome, UiError>>,
    patch: VecDeque<Result<BackendTopicQueuePatchResult, UiError>>,
    delete: VecDeque<Result<TopicPartialOutcome, UiError>>,
    delete_broker: VecDeque<Result<TopicPartialOutcome, UiError>>,
    send: VecDeque<Result<(), UiError>>,
    reset: VecDeque<Result<TopicPartialOutcome, UiError>>,
    skip: VecDeque<Result<TopicPartialOutcome, UiError>>,
}

#[derive(Default)]
pub(crate) struct FakeTopicBackend {
    queues: Mutex<Queues>,
    calls: Mutex<TopicCalls>,
}

macro_rules! queue_method {
    ($name:ident, $field:ident, $value:ty) => {
        pub fn $name(&self, result: Result<$value, UiError>) {
            self.queues
                .lock()
                .expect("Topic fake queues")
                .$field
                .push_back(result);
        }
    };
}

impl FakeTopicBackend {
    queue_method!(queue_inventory, inventory, TopicInventory);
    queue_method!(queue_route, route, TopicRouteView);
    queue_method!(queue_stats, stats, TopicStatsView);
    queue_method!(queue_config, config, TopicConfigView);
    queue_method!(queue_consumers, consumers, TopicConsumersView);
    queue_method!(queue_create, create, TopicPartialOutcome);
    queue_method!(queue_patch, patch, BackendTopicQueuePatchResult);
    queue_method!(queue_delete, delete, TopicPartialOutcome);
    queue_method!(queue_delete_broker, delete_broker, TopicPartialOutcome);
    queue_method!(queue_send, send, ());
    queue_method!(queue_reset, reset, TopicPartialOutcome);
    queue_method!(queue_skip, skip, TopicPartialOutcome);

    pub fn calls(&self) -> TopicCalls {
        self.calls.lock().expect("Topic fake calls").clone()
    }
}

macro_rules! pop_result {
    ($self:ident, $field:ident, $operation:literal) => {
        $self
            .queues
            .lock()
            .expect("Topic fake queues")
            .$field
            .pop_front()
            .unwrap_or_else(|| Err(unexpected_call($operation)))
    };
}

impl TopicBackend for FakeTopicBackend {
    fn inventory(&self, scope: TopicRequestScope) -> ServiceFuture<'_, Result<TopicInventory, UiError>> {
        self.calls.lock().expect("Topic fake calls").inventory.push(scope);
        Box::pin(std::future::ready(pop_result!(self, inventory, "inventory")))
    }

    fn route(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> ServiceFuture<'_, Result<TopicRouteView, UiError>> {
        self.calls.lock().expect("Topic fake calls").route.push((scope, topic));
        Box::pin(std::future::ready(pop_result!(self, route, "route")))
    }

    fn stats(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> ServiceFuture<'_, Result<TopicStatsView, UiError>> {
        self.calls.lock().expect("Topic fake calls").stats.push((scope, topic));
        Box::pin(std::future::ready(pop_result!(self, stats, "stats")))
    }

    fn config(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> ServiceFuture<'_, Result<TopicConfigView, UiError>> {
        self.calls.lock().expect("Topic fake calls").config.push((scope, topic));
        Box::pin(std::future::ready(pop_result!(self, config, "configuration")))
    }

    fn consumers(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> ServiceFuture<'_, Result<TopicConsumersView, UiError>> {
        self.calls
            .lock()
            .expect("Topic fake calls")
            .consumers
            .push((scope, topic));
        Box::pin(std::future::ready(pop_result!(self, consumers, "consumers")))
    }

    fn create(
        &self,
        scope: TopicRequestScope,
        command: TopicCreateCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>> {
        self.calls
            .lock()
            .expect("Topic fake calls")
            .create
            .push(TopicCreateCall {
                scope,
                topic: command.topic,
                targets: command.targets,
                read_queue_count: command.read_queue_count,
                write_queue_count: command.write_queue_count,
                permission: command.permission,
                ordered: command.ordered,
                message_type: command.message_type,
            });
        Box::pin(std::future::ready(pop_result!(self, create, "create")))
    }

    fn patch_queue_counts(
        &self,
        scope: TopicRequestScope,
        command: TopicQueuePatchCommand,
    ) -> ServiceFuture<'_, Result<BackendTopicQueuePatchResult, UiError>> {
        self.calls.lock().expect("Topic fake calls").patch.push(TopicPatchCall {
            scope,
            topic: command.topic,
            target: command.target,
            expected_version: command.expected_version,
            read_queue_count: command.read_queue_count,
            write_queue_count: command.write_queue_count,
        });
        Box::pin(std::future::ready(pop_result!(self, patch, "queue patch")))
    }

    fn delete(
        &self,
        scope: TopicRequestScope,
        command: TopicDeleteCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>> {
        self.calls
            .lock()
            .expect("Topic fake calls")
            .delete
            .push((scope, command.topic, command.cluster_names));
        Box::pin(std::future::ready(pop_result!(self, delete, "delete Topic")))
    }

    fn delete_broker(
        &self,
        scope: TopicRequestScope,
        command: TopicDeleteBrokerCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>> {
        self.calls
            .lock()
            .expect("Topic fake calls")
            .delete_broker
            .push((scope, command.topic, command.target));
        Box::pin(std::future::ready(pop_result!(self, delete_broker, "delete Broker")))
    }

    fn send(&self, scope: TopicRequestScope, mut command: TopicSendCommand) -> ServiceFuture<'_, Result<(), UiError>> {
        self.calls.lock().expect("Topic fake calls").send.push(TopicSendCall {
            scope,
            topic: command.topic.clone(),
            has_key: !command.key.is_empty(),
            has_tag: !command.tag.is_empty(),
            body_length: command.body.len(),
            trace_enabled: command.trace_enabled,
        });
        command.body.clear();
        Box::pin(std::future::ready(pop_result!(self, send, "send")))
    }

    fn reset(
        &self,
        scope: TopicRequestScope,
        command: TopicOffsetCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>> {
        self.calls
            .lock()
            .expect("Topic fake calls")
            .reset
            .push(offset_call(scope, command));
        Box::pin(std::future::ready(pop_result!(self, reset, "reset offset")))
    }

    fn skip(
        &self,
        scope: TopicRequestScope,
        command: TopicOffsetCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>> {
        self.calls
            .lock()
            .expect("Topic fake calls")
            .skip
            .push(offset_call(scope, command));
        Box::pin(std::future::ready(pop_result!(self, skip, "skip accumulated")))
    }
}

fn offset_call(scope: TopicRequestScope, command: TopicOffsetCommand) -> TopicOffsetCall {
    TopicOffsetCall {
        scope,
        topic: command.topic,
        consumer_group: command.consumer_group,
        cluster_name: command.cluster_name,
        timestamp: command.timestamp,
        force: command.force,
    }
}

fn unexpected_call(operation: &str) -> UiError {
    UiError::new(
        format!("Unexpected Topic {operation} test call."),
        UiErrorCode::CapabilityUnavailable,
        false,
    )
}
