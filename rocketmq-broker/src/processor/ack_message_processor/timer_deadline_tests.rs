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

use rocketmq_store::BrokerReadStore;

use super::*;
use crate::processor::processor_service::pop_buffer_merge_service::retirement_tests::Fixture;

#[tokio::test]
async fn fallback_ack_records_an_absolute_timer_deadline_in_the_real_store() {
    let mut fixture = Fixture::new().await;
    let retired = fixture.retire_unacknowledged_checkpoint().await;
    fixture.timer_fields.lock().clear();
    let before = fixture
        .broker
        .runtime_state_mut()
        .message_store()
        .unwrap()
        .get_max_phy_offset();
    let state = fixture.broker.runtime_state_mut();
    let processor = AckMessageProcessor::new(AckMessageProcessorContext::new(
        AckMessagePolicy::from_config(&state.broker_config(), state.store_host()),
        state.topic_config_manager_handle(),
        AckMessageOffsetCapability::new(&state.consumer_offset_manager_handle()),
        AckMessageOrderCapability::new(&state.consumer_order_info_manager_handle()),
        AckMessageStoreCapability::new(&state.escape_bridge()),
        state.pop_inflight_message_counter().clone(),
        AckMessagePopCapability::new(&fixture.processor),
        Vec::new(),
    ));
    let pop_time = retired.get_ck().pop_time;
    let invisible_time = 60_000;
    let extra_info = ExtraInfoUtil::build_extra_info_with_offset(
        100,
        pop_time,
        invisible_time,
        0,
        "retirement-topic",
        "retirement-broker",
        0,
        100,
    );
    let header = AckMessageRequestHeader {
        topic: "retirement-topic".into(),
        consumer_group: "retirement-group".into(),
        queue_id: 0,
        offset: 100,
        extra_info: extra_info.into(),
        lite_topic: None,
        topic_request_header: None,
    };
    let mut response = application_remoting_command_factory().create_success_response_command();
    let result = processor
        .append_ack(Some(header), &mut response, None, &"test".into(), None)
        .await;
    fixture.flush();
    let fields = fixture.timer_fields.lock().clone();
    let appended = fixture
        .broker
        .runtime_state_mut()
        .message_store()
        .unwrap()
        .get_max_phy_offset();
    drop(processor);
    fixture.close().await;

    assert!(result.is_ok());
    assert!(
        appended > before,
        "a late ACK after retirement must take the real durable fallback"
    );
    assert_eq!(fields, vec![((pop_time + invisible_time) as u64, 0)]);
}
