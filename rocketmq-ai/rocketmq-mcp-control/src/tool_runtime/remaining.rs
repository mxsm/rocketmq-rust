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

use rocketmq_admin_core::core::supervised_mutation as admin;

use crate::tools;

#[path = "remaining/broker.rs"]
mod broker;
#[path = "remaining/offset.rs"]
mod offset;
#[path = "remaining/request_mode.rs"]
mod request_mode;

pub(super) use broker::run_broker;
pub(super) use offset::run_offset;
pub(super) use request_mode::run_request_mode;

fn dry_status(success_count: usize, failure_count: usize) -> tools::MutationStatus {
    if failure_count == 0 {
        tools::MutationStatus::Planned
    } else if success_count == 0 {
        tools::MutationStatus::Failed
    } else {
        tools::MutationStatus::Partial
    }
}

fn status_from_failures(failures: impl Iterator<Item = Option<tools::FailureCode>>) -> tools::MutationStatus {
    let failures = failures.collect::<Vec<_>>();
    let succeeded = failures.iter().filter(|failure| failure.is_none()).count();
    if succeeded == failures.len() {
        tools::MutationStatus::Applied
    } else if succeeded > 0 {
        tools::MutationStatus::Partial
    } else if !failures.is_empty()
        && failures
            .iter()
            .all(|failure| *failure == Some(tools::FailureCode::Conflict))
    {
        tools::MutationStatus::Conflict
    } else {
        tools::MutationStatus::Failed
    }
}

fn map_broker_state(state: admin::BrokerMutationConfigState) -> tools::BrokerConfigState {
    tools::BrokerConfigState {
        generation: state.generation,
        auto_create_topic_enable: state.auto_create_topic_enable,
        auto_create_subscription_group: state.auto_create_subscription_group,
        broker_permission: state.broker_permission,
        default_topic_queue_nums: state.default_topic_queue_nums,
        message_index_enable: state.message_index_enable,
        trace_topic_enable: state.trace_topic_enable,
    }
}

fn map_broker_patch(patch: tools::BrokerConfigPatch) -> admin::BrokerMutationConfigPatch {
    admin::BrokerMutationConfigPatch {
        auto_create_topic_enable: patch.auto_create_topic_enable,
        auto_create_subscription_group: patch.auto_create_subscription_group,
        broker_permission: patch.broker_permission,
        default_topic_queue_nums: patch.default_topic_queue_nums,
        message_index_enable: patch.message_index_enable,
        trace_topic_enable: patch.trace_topic_enable,
    }
}

fn broker_patch_changes(state: tools::BrokerConfigState, patch: tools::BrokerConfigPatch) -> bool {
    patch
        .auto_create_topic_enable
        .is_some_and(|value| value != state.auto_create_topic_enable)
        || patch
            .auto_create_subscription_group
            .is_some_and(|value| value != state.auto_create_subscription_group)
        || patch
            .broker_permission
            .is_some_and(|value| value != state.broker_permission)
        || patch
            .default_topic_queue_nums
            .is_some_and(|value| value != state.default_topic_queue_nums)
        || patch
            .message_index_enable
            .is_some_and(|value| value != state.message_index_enable)
        || patch
            .trace_topic_enable
            .is_some_and(|value| value != state.trace_topic_enable)
}

fn map_request_mode(value: tools::RequestModeValue) -> admin::RequestModeValue {
    admin::RequestModeValue {
        mode: match value.mode {
            tools::ConsumerRequestMode::Pull => admin::RequestMode::Pull,
            tools::ConsumerRequestMode::Pop => admin::RequestMode::Pop,
        },
        pop_share_queue_num: value.pop_share_queue_num,
    }
}

fn map_request_mode_from_admin(value: admin::RequestModeValue) -> tools::RequestModeValue {
    tools::RequestModeValue {
        mode: match value.mode {
            admin::RequestMode::Pull => tools::ConsumerRequestMode::Pull,
            admin::RequestMode::Pop => tools::ConsumerRequestMode::Pop,
        },
        pop_share_queue_num: value.pop_share_queue_num,
    }
}
