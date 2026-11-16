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

use super::*;
use crate::processor::processor_service::pop_buffer_merge_service::retirement_tests::Fixture;

#[tokio::test]
async fn changed_visibility_checkpoint_and_original_ack_use_absolute_timer_deadlines() {
    let mut fixture = Fixture::new().await;
    let state = fixture.broker.runtime_state_mut();
    let processor = ChangeInvisibleTimeProcessor::new(ChangeInvisibleTimeProcessorContext::new(
        ChangeInvisibleTimePolicy::from_config(&state.broker_config(), state.store_host()),
        state.topic_config_manager_handle(),
        state.consumer_offset_manager_handle().query_capability(),
        ChangeInvisibleTimeOrderCapability::new(&state.consumer_order_info_manager_handle()),
        ChangeInvisibleTimeLiteCapability { processor: Weak::new() },
        state.broker_stats_manager_handle(),
        ChangeInvisibleTimeStoreCapability::new(&state.escape_bridge()),
        ChangeInvisibleTimePopCapability::new(&fixture.merge),
        fixture.processor.queue_lock_manager().clone(),
    ));
    let original_pop_time = current_millis() as i64;
    let original_invisible_time = 60_000;
    let next_pop_time = original_pop_time as u64 + 1_000;
    let header = ChangeInvisibleTimeRequestHeader {
        topic: "retirement-topic".into(),
        consumer_group: "retirement-group".into(),
        queue_id: 0,
        offset: 100,
        invisible_time: 90_000,
        extra_info: ExtraInfoUtil::build_extra_info_with_offset(
            100,
            original_pop_time,
            original_invisible_time,
            0,
            "retirement-topic",
            "retirement-broker",
            0,
            100,
        )
        .into(),
        ..Default::default()
    };
    let checkpoint = processor
        .append_check_point(&header, 0, 0, 100, next_pop_time, "retirement-broker".into())
        .await
        .unwrap()
        .unwrap();
    let ack = processor
        .ack_origin(&header, &ExtraInfoUtil::split(header.extra_info.as_str()))
        .await;
    fixture.flush();
    let fields = fixture.timer_fields.lock().clone();
    drop(processor);
    fixture.close().await;

    assert_eq!(checkpoint.put_message_status(), PutMessageStatus::PutOk);
    assert!(ack.is_ok());
    assert_eq!(
        fields,
        vec![
            (
                next_pop_time + header.invisible_time as u64 - PopAckConstants::ACK_TIME_INTERVAL as u64,
                0
            ),
            ((original_pop_time + original_invisible_time) as u64, 0),
        ]
    );
}
