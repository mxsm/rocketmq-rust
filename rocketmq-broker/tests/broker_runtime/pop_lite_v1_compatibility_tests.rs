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

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use super::*;
use rocketmq_model::common::key_builder::POP_ORDER_REVIVE_QUEUE;

async fn create_test_channel_pair(
    label: &str,
) -> (
    Channel,
    tokio::net::TcpStream,
    rocketmq_runtime::TaskGroup,
    std::net::TcpStream,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind paired test listener");
    let local_addr = listener.local_addr().expect("paired listener address");
    let peer = tokio::net::TcpStream::connect(local_addr)
        .await
        .expect("connect paired test peer");
    let (server_stream, peer_addr) = listener.accept().await.expect("accept paired test peer");
    let server_stream = server_stream.into_std().expect("convert paired server stream");
    let server_shutdown = server_stream.try_clone().expect("clone paired server stream");
    let server_stream = tokio::net::TcpStream::from_std(server_stream).expect("restore paired server stream");
    let connection = Connection::new(server_stream);
    let channel_tasks = crate::test_task_group(label);
    let channel = rocketmq_transport::test_support::TestChannelBuilder::new(connection, channel_tasks.clone())
        .addresses(local_addr, peer_addr)
        .build()
        .expect("build paired test channel");
    (channel, peer, channel_tasks, server_shutdown)
}

async fn read_raw_remoting_frame(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut length_prefix = [0_u8; 4];
    stream
        .read_exact(&mut length_prefix)
        .await
        .expect("read raw RocketMQ frame length");
    let payload_len = i32::from_be_bytes(length_prefix);
    assert!(payload_len >= 4, "invalid raw RocketMQ frame length {payload_len}");
    let payload_len = usize::try_from(payload_len).expect("positive raw RocketMQ frame length");
    let mut frame = vec![0_u8; payload_len.saturating_add(length_prefix.len())];
    frame[..length_prefix.len()].copy_from_slice(&length_prefix);
    stream
        .read_exact(&mut frame[length_prefix.len()..])
        .await
        .expect("read complete raw RocketMQ frame");
    frame
}

fn assert_shutdown_tree_drained(report: &rocketmq_runtime::ShutdownReport) {
    assert_eq!(report.leaked, 0, "{}", report.to_json());
    assert_eq!(report.blocking_still_running, 0, "{}", report.to_json());
    assert_eq!(report.detached_still_running, 0, "{}", report.to_json());
    assert!(report.remaining_tasks.is_empty(), "{}", report.to_json());
    assert!(report.blocking_tasks.is_empty(), "{}", report.to_json());
    for child in &report.children {
        assert_shutdown_tree_drained(child);
    }
}

#[tokio::test]
async fn pop_lite_v1_wake_compatibility_trigger_dispatch_advances_offset() {
    let mut runtime = new_lite_test_runtime("pop-lite-trigger-wakeup").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_message(&mut runtime, "child-a", b"lite-body").await;
    let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));

    let (mut processor, _) = runtime.init_processor();
    runtime
        .composition
        .state
        .pop_lite_message_processor
        .as_ref()
        .expect("pop lite processor should be initialized")
        .start()
        .await;

    let (pop_channel, mut pop_peer, pop_channel_tasks, pop_server_shutdown) =
        create_test_channel_pair("pop-lite-v1-raw-channel").await;
    let pop_ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(pop_channel.clone()));
    let pop_header = PopLiteMessageRequestHeader {
        client_id: CheetahString::from_static_str("client-1"),
        consumer_group: CheetahString::from_static_str("group-a"),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 60_000,
        poll_time: 3_000,
        born_time: current_millis() as i64,
        attempt_id: None,
        rpc: None,
    };
    let mut pop_request =
        RemotingCommand::create_request_command(RequestCode::PopLiteMessage, pop_header).set_opaque(98_335);
    pop_request.make_custom_header_to_net();
    let response = processor
        .process_request(pop_channel.clone(), pop_ctx, &mut pop_request)
        .await
        .expect("pop lite should suspend cleanly");
    assert!(
        response.is_none(),
        "suspended pop-lite should not produce an immediate response"
    );

    let trigger_channel = create_test_channel().await;
    let trigger_ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(trigger_channel.clone()));
    let trigger_header = TriggerLiteDispatchRequestHeader {
        group: CheetahString::from_static_str("group-a"),
        client_id: Some(CheetahString::from_static_str("client-1")),
    };
    let mut trigger_request = RemotingCommand::create_request_command(RequestCode::TriggerLiteDispatch, trigger_header);
    trigger_request.make_custom_header_to_net();
    let trigger_response = processor
        .process_request(trigger_channel, trigger_ctx, &mut trigger_request)
        .await
        .expect("trigger lite dispatch should succeed")
        .expect("trigger lite dispatch should return a response");
    assert_eq!(ResponseCode::from(trigger_response.code()), ResponseCode::Success);

    let raw_frame = tokio::time::timeout(Duration::from_secs(3), read_raw_remoting_frame(&mut pop_peer))
        .await
        .expect("legacy PopLite wake must emit one bounded raw frame");
    let announced = i32::from_be_bytes(raw_frame[..4].try_into().expect("complete raw PopLite frame prefix"));
    assert_eq!(
        usize::try_from(announced).expect("positive raw PopLite frame length"),
        raw_frame.len() - 4
    );
    let mut encoded = BytesMut::from(raw_frame.as_slice());
    let mut wake_response = RemotingCommand::decode(&mut encoded)
        .expect("decode legacy PopLite wake frame")
        .expect("complete legacy PopLite wake frame");
    assert!(encoded.is_empty(), "legacy PopLite wake emits one frame");
    assert_eq!(wake_response.opaque(), 98_335);
    assert_eq!(wake_response.code(), ResponseCode::Success as i32);
    assert_eq!(wake_response.remark().map(CheetahString::as_str), Some("FOUND"));
    wake_response.make_custom_header_to_net();
    let response_header = wake_response
        .decode_command_custom_header::<PopLiteMessageResponseHeader>()
        .expect("decode legacy PopLite wake response header");
    assert!(response_header.pop_time > 0);
    assert_eq!(response_header.invisible_time, 60_000);
    assert_eq!(response_header.revive_qid, POP_ORDER_REVIVE_QUEUE);
    assert_eq!(response_header.start_offset_info, None);
    assert_eq!(response_header.msg_offset_info, None);
    assert_eq!(response_header.order_count_info.as_deref(), Some("0"));
    let mut response_body = wake_response.body().cloned().expect("legacy PopLite wake body");
    let message = MessageDecoder::decode(&mut response_body, true, false, false, false, false)
        .expect("decode legacy PopLite wake body");
    assert_eq!(message.body(), Some(Bytes::from_static(b"lite-body")));
    assert!(
        response_body.is_empty(),
        "legacy PopLite wake body has no trailing bytes after the exact message payload"
    );

    assert_eq!(
        runtime.composition.state.consumer_offset_manager().query_offset(
            &CheetahString::from_static_str("group-a"),
            &lmq_name,
            0,
        ),
        1,
        "legacy wake commits the consumed offset before writing"
    );

    assert_eq!(
        runtime
            .composition
            .state
            .pop_lite_message_processor
            .as_ref()
            .expect("pop lite processor should be initialized")
            .pop_lite_long_polling_service()
            .get_polling_num("client-1"),
        0
    );
    assert!(runtime
        .composition
        .state
        .lite_event_dispatcher()
        .pending_events(&CheetahString::from_static_str("client-1"))
        .is_empty());
    runtime
        .composition
        .state
        .pop_lite_message_processor
        .as_ref()
        .expect("pop lite processor should be initialized")
        .shutdown()
        .await;
    let broker_tasks = runtime
        .composition
        .state
        .broker_service_task_group()
        .expect("lite test runtime has an owned broker task group");
    let broker_report = broker_tasks.shutdown(Duration::from_secs(1)).await;
    assert_shutdown_tree_drained(&broker_report);
    let drained = runtime
        .composition
        .state
        .pop_lite_message_processor
        .as_ref()
        .expect("pop lite processor should remain initialized until drain assertion")
        .pop_lite_long_polling_service()
        .resource_snapshot();
    assert_eq!(drained.waking_client_count, 0);
    drop(processor);
    drop(runtime.composition.state.pop_lite_message_processor.take());
    pop_peer.shutdown().await.expect("half-close legacy PopLite raw peer");
    let channel_report = pop_channel.close_with_report(Duration::from_secs(1)).await;
    assert!(channel_report.is_healthy(), "{}", channel_report.to_json());
    drop(pop_channel);
    let channel_tasks_report = pop_channel_tasks.shutdown(Duration::from_secs(1)).await;
    assert_shutdown_tree_drained(&channel_tasks_report);
    let store_path_root_dir = runtime.message_store_config().store_path_root_dir.to_string();
    drop(runtime);
    // The V1 SkipSet retires removed PopRequest nodes through epoch reclamation, so the
    // test-owned duplicate socket handle closes the transport deterministically without
    // claiming that the retired node has already been reclaimed.
    pop_server_shutdown
        .shutdown(std::net::Shutdown::Both)
        .expect("shutdown legacy PopLite server socket");
    let mut trailing = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), pop_peer.read_to_end(&mut trailing))
        .await
        .expect("legacy PopLite raw peer reaches EOF")
        .expect("read legacy PopLite trailing bytes");
    assert!(trailing.is_empty(), "legacy PopLite wake emits no trailing frame");
    let _ = std::fs::remove_dir_all(store_path_root_dir);
}
