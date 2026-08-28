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

#[tokio::test]
async fn pop_lite_v2_resume_core_real_store_causality_advances_offset_once() {
    let mut runtime = new_lite_test_runtime("pop-lite-v2-core-consume").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_message(&mut runtime, "child-a", b"lite-v2-body").await;
    let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));
    let client_id = CheetahString::from_static_str("client-1");
    let group = CheetahString::from_static_str("group-a");
    runtime.composition.state.lite_event_dispatcher().do_full_dispatch(
        &client_id,
        &group,
        &HashSet::from([lmq_name.clone()]),
    );

    let _ = runtime.init_processor();
    let processor = runtime
        .composition
        .state
        .pop_lite_message_processor()
        .expect("PopLite processor should initialize")
        .clone();
    let batch = runtime
        .composition
        .state
        .lite_event_dispatcher()
        .reserve_pending_events(&client_id)
        .expect("V2 seam should reserve the dispatched event")
        .commit();
    let request_header = PopLiteMessageRequestHeader {
        client_id: client_id.clone(),
        consumer_group: group.clone(),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 60_000,
        poll_time: 30_000,
        born_time: current_millis() as i64,
        attempt_id: None,
        rpc: None,
    };

    let result = processor.execute_pop_lite_batch(&request_header, batch).await;
    let mut body = result.body.expect("V2 shared core should return the stored message");
    let message =
        MessageDecoder::decode(&mut body, true, false, false, false, false).expect("decode V2 shared core message");
    assert_eq!(message.body(), Some(Bytes::from_static(b"lite-v2-body")));
    assert_eq!(
        runtime
            .composition
            .state
            .consumer_offset_manager()
            .query_offset(&group, &lmq_name, 0),
        1
    );
    assert!(runtime
        .composition
        .state
        .lite_event_dispatcher()
        .pending_events(&client_id)
        .is_empty());
    assert_eq!(
        runtime
            .composition
            .state
            .lite_event_dispatcher()
            .reservation_snapshot()
            .events,
        0
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn pop_lite_v2_real_store_preserves_attempt_order_and_requeues_until_drained() {
    let mut runtime = new_lite_test_runtime("pop-lite-v2-attempt-requeue").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_message(&mut runtime, "child-a", b"lite-v2-first").await;
    seed_lmq_message(&mut runtime, "child-a", b"lite-v2-second").await;
    let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));
    let client_id = CheetahString::from_static_str("client-1");
    let group = CheetahString::from_static_str("group-a");
    let dispatcher = runtime.composition.state.lite_event_dispatcher().clone();
    dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([lmq_name.clone()]));

    let _ = runtime.init_processor();
    let processor = runtime
        .composition
        .state
        .pop_lite_message_processor()
        .expect("PopLite processor should initialize")
        .clone();
    let header = |attempt_id: &'static str| PopLiteMessageRequestHeader {
        client_id: client_id.clone(),
        consumer_group: group.clone(),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 60_000,
        poll_time: 30_000,
        born_time: current_millis() as i64,
        attempt_id: Some(CheetahString::from_static_str(attempt_id)),
        rpc: None,
    };

    let first = processor
        .execute_pop_lite_batch(
            &header("attempt-a"),
            dispatcher
                .reserve_pending_events(&client_id)
                .expect("reserve first V2 event")
                .commit(),
        )
        .await;
    let mut first_body = first.body.expect("first V2 attempt reads the first message");
    let first_message = MessageDecoder::decode(&mut first_body, true, false, false, false, false)
        .expect("decode first V2 ordered message");
    assert_eq!(first_message.body(), Some(Bytes::from_static(b"lite-v2-first")));
    assert_eq!(
        runtime
            .composition
            .state
            .consumer_offset_manager()
            .query_offset(&group, &lmq_name, 0),
        1
    );
    assert_eq!(dispatcher.pending_events(&client_id), vec![lmq_name.clone()]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 1);

    let blocked = processor
        .execute_pop_lite_batch(
            &header("attempt-b"),
            dispatcher
                .reserve_pending_events(&client_id)
                .expect("reserve event for blocked V2 attempt")
                .commit(),
        )
        .await;
    assert!(blocked.body.is_none(), "different attempt remains FIFO-blocked");
    assert_eq!(
        runtime
            .composition
            .state
            .consumer_offset_manager()
            .query_offset(&group, &lmq_name, 0),
        1
    );
    assert_eq!(dispatcher.pending_events(&client_id), vec![lmq_name.clone()]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 1);

    let second = processor
        .execute_pop_lite_batch(
            &header("attempt-a"),
            dispatcher
                .reserve_pending_events(&client_id)
                .expect("reserve event for matching V2 attempt")
                .commit(),
        )
        .await;
    let mut second_body = second.body.expect("matching V2 attempt reads the second message");
    let second_message = MessageDecoder::decode(&mut second_body, true, false, false, false, false)
        .expect("decode second V2 ordered message");
    assert_eq!(second_message.body(), Some(Bytes::from_static(b"lite-v2-second")));
    assert_eq!(
        runtime
            .composition
            .state
            .consumer_offset_manager()
            .query_offset(&group, &lmq_name, 0),
        2
    );
    assert!(dispatcher.pending_events(&client_id).is_empty());
    assert_eq!(dispatcher.budget_snapshot().current_count, 0);
    assert_eq!(dispatcher.reservation_snapshot().events, 0);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn pop_lite_v2_real_store_applies_reset_and_consumes_empty_event() {
    let mut runtime = new_lite_test_runtime("pop-lite-v2-offset-correction").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_message(&mut runtime, "child-a", b"lite-v2-reset-first").await;
    seed_lmq_message(&mut runtime, "child-a", b"lite-v2-reset-second").await;
    let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));
    let empty_lmq = CheetahString::from_string(to_lmq_name("parent-topic", "child-b").expect("child-b lmq"));
    let client_id = CheetahString::from_static_str("client-1");
    let group = CheetahString::from_static_str("group-a");
    let dispatcher = runtime.composition.state.lite_event_dispatcher().clone();

    let _ = runtime.init_processor();
    let processor = runtime
        .composition
        .state
        .pop_lite_message_processor()
        .expect("PopLite processor should initialize")
        .clone();
    runtime
        .composition
        .state
        .consumer_offset_manager()
        .assign_reset_offset(&lmq_name, &group, 0, 1);
    runtime.composition.state.consumer_offset_manager().commit_offset(
        CheetahString::from_static_str("PopLiteV2ResetConcurrentAdvance"),
        &group,
        &lmq_name,
        0,
        99,
    );
    let durable_reset_key = CheetahString::from_string(format!("{}@{}", lmq_name, group));
    assert_eq!(
        runtime
            .composition
            .state
            .consumer_offset_manager()
            .offset_table_snapshot()
            .get(&durable_reset_key)
            .and_then(|offsets| offsets.get(&0))
            .copied(),
        Some(99),
        "the reset path must conditionally replace the durable high-water mark"
    );
    dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([lmq_name.clone()]));
    let header = PopLiteMessageRequestHeader {
        client_id: client_id.clone(),
        consumer_group: group.clone(),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 60_000,
        poll_time: 30_000,
        born_time: current_millis() as i64,
        attempt_id: None,
        rpc: None,
    };
    let reset = processor
        .execute_pop_lite_batch(
            &header,
            dispatcher
                .reserve_pending_events(&client_id)
                .expect("reserve reset-offset V2 event")
                .commit(),
        )
        .await;
    let mut reset_body = reset.body.expect("reset offset reads the second stored message");
    let reset_message = MessageDecoder::decode(&mut reset_body, true, false, false, false, false)
        .expect("decode reset-offset V2 message");
    assert_eq!(reset_message.body(), Some(Bytes::from_static(b"lite-v2-reset-second")));
    assert_eq!(
        runtime
            .composition
            .state
            .consumer_offset_manager()
            .query_offset(&group, &lmq_name, 0),
        2
    );

    dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([empty_lmq]));
    let empty = processor
        .execute_pop_lite_batch(
            &header,
            dispatcher
                .reserve_pending_events(&client_id)
                .expect("reserve empty-store V2 event")
                .commit(),
        )
        .await;
    assert!(empty.body.is_none());
    assert_eq!(empty.fetched_count, 0);
    assert!(dispatcher.pending_events(&client_id).is_empty());
    assert_eq!(dispatcher.budget_snapshot().current_count, 0);
    assert_eq!(dispatcher.reservation_snapshot().events, 0);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn pop_lite_v2_real_store_corrects_bad_offset_before_reread() {
    let mut runtime = new_lite_test_runtime("pop-lite-v2-bad-offset-correction").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_message(&mut runtime, "child-a", b"lite-v2-first").await;
    seed_lmq_message(&mut runtime, "child-a", b"lite-v2-second").await;
    let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));
    let client_id = CheetahString::from_static_str("client-1");
    let group = CheetahString::from_static_str("group-a");
    let dispatcher = runtime.composition.state.lite_event_dispatcher().clone();
    runtime.composition.state.consumer_offset_manager().commit_offset(
        CheetahString::from_static_str("PopLiteV2BadOffsetTest"),
        &group,
        &lmq_name,
        0,
        99,
    );
    let direct_bad_offset = runtime
        .composition
        .state
        .message_store()
        .expect("message store should remain initialized")
        .get_message(&group, &lmq_name, 0, 99, 1, None)
        .await
        .expect("bad-offset direct read returns a correction result");
    assert_eq!(direct_bad_offset.status(), Some(GetMessageStatus::OffsetOverflowBadly));
    assert_eq!(direct_bad_offset.next_begin_offset(), 2);

    let _ = runtime.init_processor();
    let processor = runtime
        .composition
        .state
        .pop_lite_message_processor()
        .expect("PopLite processor should initialize")
        .clone();
    dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([lmq_name.clone()]));
    let header = PopLiteMessageRequestHeader {
        client_id: client_id.clone(),
        consumer_group: group.clone(),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 60_000,
        poll_time: 30_000,
        born_time: current_millis() as i64,
        attempt_id: None,
        rpc: None,
    };
    let corrected = processor
        .execute_pop_lite_batch(
            &header,
            dispatcher
                .reserve_pending_events(&client_id)
                .expect("reserve bad-offset V2 event")
                .commit(),
        )
        .await;

    assert!(corrected.body.is_none(), "corrected end offset has no unread message");
    assert_eq!(
        runtime
            .composition
            .state
            .consumer_offset_manager()
            .query_offset(&group, &lmq_name, 0),
        2
    );
    assert!(dispatcher.pending_events(&client_id).is_empty());
    assert_eq!(dispatcher.budget_snapshot().current_count, 0);
    assert_eq!(dispatcher.reservation_snapshot().events, 0);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn pop_lite_v2_real_store_initializes_missing_offset_from_retained_minimum() {
    let mut runtime = new_lite_test_runtime("pop-lite-v2-missing-offset-minimum").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_offsets(&mut runtime, &[("child-a", 5)]);
    seed_lmq_message(&mut runtime, "child-a", b"lite-v2-retained-minimum").await;
    let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));
    let client_id = CheetahString::from_static_str("client-1");
    let group = CheetahString::from_static_str("group-a");
    let direct = runtime
        .composition
        .state
        .message_store()
        .expect("message store remains initialized")
        .get_message(&group, &lmq_name, 0, 0, 1, None)
        .await
        .expect("retained-minimum read returns a correction result");
    assert_eq!(direct.status(), Some(GetMessageStatus::OffsetTooSmall));
    assert_eq!(direct.next_begin_offset(), 5);
    assert_eq!(
        runtime
            .composition
            .state
            .consumer_offset_manager()
            .query_offset(&group, &lmq_name, 0),
        -1
    );

    let _ = runtime.init_processor();
    let processor = runtime
        .composition
        .state
        .pop_lite_message_processor()
        .expect("PopLite processor should initialize")
        .clone();
    let dispatcher = runtime.composition.state.lite_event_dispatcher().clone();
    dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([lmq_name.clone()]));
    let result = processor
        .execute_pop_lite_batch(
            &PopLiteMessageRequestHeader {
                client_id: client_id.clone(),
                consumer_group: group.clone(),
                topic: CheetahString::from_static_str("parent-topic"),
                max_msg_num: 1,
                invisible_time: 60_000,
                poll_time: 30_000,
                born_time: current_millis() as i64,
                attempt_id: None,
                rpc: None,
            },
            dispatcher
                .reserve_pending_events(&client_id)
                .expect("reserve retained-minimum event")
                .commit(),
        )
        .await;

    let mut body = result.body.expect("corrected initial read returns retained message");
    let message = MessageDecoder::decode(&mut body, true, false, false, false, false)
        .expect("decode retained-minimum V2 message");
    assert_eq!(message.body(), Some(Bytes::from_static(b"lite-v2-retained-minimum")));
    assert_eq!(
        runtime
            .composition
            .state
            .consumer_offset_manager()
            .query_offset(&group, &lmq_name, 0),
        6
    );
    assert!(dispatcher.pending_events(&client_id).is_empty());
    assert_eq!(dispatcher.budget_snapshot().current_count, 0);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn pop_lite_v2_unavailable_store_consumes_event_without_offset_commit() {
    let mut runtime = new_lite_test_runtime("pop-lite-v2-store-unavailable").await;
    seed_lite_query_state(&mut runtime);
    let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));
    let client_id = CheetahString::from_static_str("client-1");
    let group = CheetahString::from_static_str("group-a");
    let dispatcher = runtime.composition.state.lite_event_dispatcher().clone();
    let _ = runtime.init_processor();
    let processor = runtime
        .composition
        .state
        .pop_lite_message_processor()
        .expect("PopLite processor should initialize")
        .clone();
    runtime.shutdown_message_store_for_test().await;
    dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([lmq_name.clone()]));
    let header = PopLiteMessageRequestHeader {
        client_id: client_id.clone(),
        consumer_group: group.clone(),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 60_000,
        poll_time: 30_000,
        born_time: current_millis() as i64,
        attempt_id: None,
        rpc: None,
    };
    let unavailable = processor
        .execute_pop_lite_batch(
            &header,
            dispatcher
                .reserve_pending_events(&client_id)
                .expect("reserve unavailable-store V2 event")
                .commit(),
        )
        .await;

    assert!(unavailable.body.is_none());
    assert_eq!(unavailable.fetched_count, 0);
    assert_eq!(
        runtime
            .composition
            .state
            .consumer_offset_manager()
            .query_offset(&group, &lmq_name, 0),
        -1
    );
    assert!(dispatcher.pending_events(&client_id).is_empty());
    assert_eq!(dispatcher.budget_snapshot().current_count, 0);
    assert_eq!(dispatcher.reservation_snapshot().events, 0);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}
