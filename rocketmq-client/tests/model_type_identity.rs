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

use rocketmq_client_rust::base::query_result::QueryResult as LegacyQueryResult;
use rocketmq_client_rust::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy as LegacyAllocationStrategy;
use rocketmq_client_rust::consumer::pull_result::PullOutcome;
use rocketmq_client_rust::consumer::pull_result::PullOutcomeAdapterError;
use rocketmq_client_rust::consumer::pull_result::PullResult;
use rocketmq_client_rust::consumer::pull_status::PullStatus as LegacyPullStatus;
use rocketmq_client_rust::consumer::rebalance_strategy::allocate_message_queue_averagely::AllocateMessageQueueAveragely as LegacyAverage;
use rocketmq_client_rust::producer::send_result::SendResult as LegacySendResult;
use rocketmq_client_rust::producer::send_status::SendStatus as LegacySendStatus;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_model::allocation::AllocateMessageQueueAveragely;
use rocketmq_model::result::PullStatus;
use rocketmq_model::result::QueryResult;
use rocketmq_model::result::SendResult;
use rocketmq_model::result::SendStatus;

#[test]
fn legacy_client_results_are_canonical_model_types() {
    fn send_identity(value: SendResult) -> LegacySendResult {
        value
    }
    fn send_status_identity(value: SendStatus) -> LegacySendStatus {
        value
    }
    fn pull_status_identity(value: PullStatus) -> LegacyPullStatus {
        value
    }
    fn query_identity(value: QueryResult<MessageExt>) -> LegacyQueryResult {
        value
    }

    assert_eq!(send_identity(SendResult::default()).get_queue_offset(), 0);
    assert_eq!(send_status_identity(SendStatus::SendOk), LegacySendStatus::SendOk);
    assert_eq!(pull_status_identity(PullStatus::Found), LegacyPullStatus::Found);
    assert_eq!(
        query_identity(QueryResult::new(1, Vec::new())).index_last_update_timestamp(),
        1
    );
}

#[test]
fn legacy_allocation_contract_is_the_canonical_model_contract() {
    fn concrete_identity(value: AllocateMessageQueueAveragely) -> LegacyAverage {
        value
    }
    fn trait_identity(_value: &dyn LegacyAllocationStrategy) {}

    let strategy = concrete_identity(AllocateMessageQueueAveragely);
    trait_identity(&strategy);
}

#[test]
fn pull_result_adapter_preserves_message_presence_and_order() {
    let absent = PullResult::new(PullStatus::Found, 12, 1, 20, None);
    let absent_round_trip = PullResult::try_from(PullOutcome::from(&absent)).unwrap();
    assert!(absent_round_trip.msg_found_list().is_none());

    let present_empty = PullResult::new(PullStatus::Found, 12, 1, 20, Some(Vec::new()));
    let present_empty_round_trip = PullResult::try_from(PullOutcome::from(&present_empty)).unwrap();
    assert_eq!(
        present_empty_round_trip
            .msg_found_list()
            .expect("the present empty message collection should remain present")
            .len(),
        0
    );

    let mut first = MessageExt::default();
    first.set_queue_offset(10);
    let mut second = MessageExt::default();
    second.set_queue_offset(11);
    let present = PullOutcome::new(PullStatus::Found, 12, 1, 20, Some(vec![first, second]));
    assert!(matches!(
        PullResult::try_from(present),
        Err(PullOutcomeAdapterError::SharedMutableMessagesUnsupported { message_count: 2 })
    ));
}
