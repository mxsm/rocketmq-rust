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
async fn consumer_send_back_persists_original_identity_in_dlq_commit_log() {
    let mut runtime = new_phase3_test_runtime("send-back-origin-metadata").await;
    let topic = CheetahString::from_static_str("send-back-origin-topic");
    let group = CheetahString::from_static_str("send-back-origin-group");
    runtime
        .runtime_state_mut()
        .topic_config_manager()
        .update_topic_config(TopicConfig::with_queues(topic.clone(), 1, 1), 0);
    runtime
        .runtime_state_mut()
        .subscription_group_manager_mut()
        .update_subscription_group_config(&mut SubscriptionGroupConfig::new(group.clone()));
    runtime.start_message_store_for_test().await.expect("start local Store");
    let (processor, _) = runtime.init_processor_checked().expect("initialize real processors");

    for existing_origin in [None, Some("first-delivery-physical-id")] {
        let mut original = MessageExtBrokerInner::default();
        original.set_topic(topic.clone());
        original.message_ext_inner.set_queue_id(0);
        original.set_body(Bytes::from_static(b"send-back-body"));
        for (key, value) in [
            (MessageConst::PROPERTY_RETRY_TOPIC, topic.as_str()),
            (
                MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                "producer-unique-id",
            ),
            (MessageConst::PROPERTY_TAGS, "OriginalTag"),
            (MessageConst::PROPERTY_KEYS, "order-key"),
        ] {
            original.put_property(key.into(), value.into());
        }
        if let Some(origin) = existing_origin {
            original.put_property(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID.into(), origin.into());
        }
        original.properties_string = MessageDecoder::message_properties_to_string(original.get_properties());
        let append = runtime
            .composition
            .data_plane
            .escape_bridge_owner
            .store_capability()
            .append_message(original)
            .await
            .expect("seed reaches local Store");
        assert!(append.result().is_ok());
        let source_offset = append.result().append_message_result().unwrap().wrote_offset;
        let store = runtime.composition.state.message_store().unwrap();
        let source = store
            .look_message_by_offset(source_offset)
            .expect("read encoded source");
        let expected_origin = existing_origin
            .map(CheetahString::from)
            .unwrap_or_else(|| source.msg_id.clone());
        let dlq_offset = store.get_max_phy_offset();

        let mut request = RemotingCommand::create_request_command(
            RequestCode::ConsumerSendMsgBack,
            ConsumerSendMsgBackRequestHeader {
                offset: source_offset,
                group: group.clone(),
                delay_level: -1,
                origin_msg_id: None,
                origin_topic: Some(topic.clone()),
                unit_mode: false,
                max_reconsume_times: None,
                rpc_request_header: None,
            },
        );
        request.make_custom_header_to_net();
        let response = process_broker_request(&processor, &mut request).await;
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);

        // Read through the commit-log decoder: checking the in-memory property map would miss the
        // stale properties_string that caused the original identity to disappear on disk.
        let stored = runtime
            .composition
            .state
            .message_store()
            .unwrap()
            .look_message_by_offset(dlq_offset)
            .expect("read persisted DLQ message");
        assert_eq!(stored.topic().as_str(), mix_all::get_dlq_topic(group.as_str()));
        assert_eq!(
            stored.property(&MessageConst::PROPERTY_ORIGIN_MESSAGE_ID.into()),
            Some(expected_origin)
        );
        for key in [
            MessageConst::PROPERTY_RETRY_TOPIC,
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            MessageConst::PROPERTY_TAGS,
            MessageConst::PROPERTY_KEYS,
        ] {
            assert_eq!(stored.property(&key.into()), source.property(&key.into()));
        }
        assert_eq!(stored.get_body(), source.get_body());
    }

    drop(processor);
    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}
