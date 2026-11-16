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

const CLIENT_ID: &str = "0123456789ABCDEF0123456789ABCDEF";
const BROKER_ID: &str = "7F00000100002A9F000000000000007B";

fn request(message_id: &str) -> ForwardMessageToDeadLetterQueueRequest {
    ForwardMessageToDeadLetterQueueRequest {
        group: ResourceIdentity::new("", "GroupA"),
        topic: ResourceIdentity::new("", "TopicA"),
        receipt_handle: ExtraInfoUtil::build_extra_info_with_offset(0, 1, 30_000, 0, "TopicA", "broker-a", 0, 7),
        message_id: message_id.to_owned(),
        delivery_attempt: 2,
        max_delivery_attempts: 3,
        lite_topic: None,
    }
}

fn original_message() -> MessageExt {
    let mut message = found_message(BROKER_ID, b"original-body");
    message.set_queue_offset(7);
    message.set_commit_log_offset(123);
    message.put_property(
        MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX.into(),
        CLIENT_ID.into(),
    );
    message
}

fn client_with_message(message: Option<MessageExt>) -> Arc<ScriptedClientIo> {
    let client = Arc::new(ScriptedClientIo::new(Arc::new(Mutex::new(Vec::new()))));
    let status = if message.is_some() {
        PullStatus::Found
    } else {
        PullStatus::NoNewMsg
    };
    client.push_pull(PullResponse::new(status, 8, 0, 8, message.map(|message| vec![message])));
    client.send_backs.lock().unwrap().push_back(Ok(()));
    client
}

#[tokio::test]
async fn public_receive_id_is_resolved_from_the_exact_receipt_message_before_explicit_dlq() {
    for requested_id in [CLIENT_ID, BROKER_ID] {
        let client = client_with_message(Some(original_message()));
        let mut state = ClusterWorkerState::new();
        state.client = Some(client.clone());
        forward_message_to_dead_letter_queue_inner(&ClusterConfig::default(), &mut state, request(requested_id), None)
            .await
            .expect("public and legacy IDs must refer to the same exact receipt source");
        assert_eq!(client.pull_calls.load(Ordering::Acquire), 1);
        let pulls = client.pull_requests.lock().unwrap();
        let header = &pulls[0].0;
        assert_eq!(header.topic.as_str(), "TopicA");
        assert_eq!(header.consumer_group.as_str(), "GroupA");
        assert_eq!((header.queue_id, header.queue_offset, header.max_msg_nums), (0, 7, 1));
        assert_eq!(
            header.sys_flag & 1,
            0,
            "the identity read cannot commit a consumer offset"
        );
        let sent = client.send_back_messages.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0.msg_id(), BROKER_ID);
        assert_eq!(sent[0].0.commit_log_offset(), 123);
        assert_eq!(sent[0].0.queue_offset(), 7);
        assert!(
            sent[0].1 <= pulls[0].1,
            "the extra read cannot reset the shared deadline"
        );
    }
}

#[tokio::test]
async fn missing_or_foreign_receipt_source_never_writes_a_dead_letter_message() {
    let original = original_message();
    let mut wrong_topic = original.clone();
    wrong_topic.set_topic("OtherTopic".into());
    let mut wrong_queue = original.clone();
    wrong_queue.set_queue_id(1);
    let mut wrong_offset = original.clone();
    wrong_offset.set_queue_offset(8);
    let mut wrong_identity = original.clone();
    wrong_identity.put_property(
        MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX.into(),
        "other-client-id".into(),
    );
    let mut wrong_storage_identity = original.clone();
    wrong_storage_identity.set_msg_id("not-a-broker-id".into());
    for message in [
        None,
        Some(wrong_topic),
        Some(wrong_queue),
        Some(wrong_offset),
        Some(wrong_identity),
        Some(wrong_storage_identity),
    ] {
        let client = client_with_message(message);
        let mut state = ClusterWorkerState::new();
        state.client = Some(client.clone());
        assert!(forward_message_to_dead_letter_queue_inner(
            &ClusterConfig::default(),
            &mut state,
            request(CLIENT_ID),
            None
        )
        .await
        .is_err());
        assert_eq!(client.pull_calls.load(Ordering::Acquire), 1);
        assert!(client.send_back_messages.lock().unwrap().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn receipt_source_read_cannot_extend_the_original_deadline_or_start_a_late_write() {
    let mut scripted = ScriptedClientIo::new(Arc::new(Mutex::new(Vec::new())));
    scripted.pull_delay = Some(Duration::from_millis(100));
    scripted.push_pull(PullResponse::new(PullStatus::Found, 8, 0, 8, vec![original_message()]));
    scripted.send_backs.lock().unwrap().push_back(Ok(()));
    let client = Arc::new(scripted);
    let mut state = ClusterWorkerState::new();
    state.client = Some(client.clone());
    assert!(forward_message_to_dead_letter_queue_inner(
        &ClusterConfig::default(),
        &mut state,
        request(CLIENT_ID),
        Some(Duration::from_millis(100)),
    )
    .await
    .is_err());
    assert_eq!(client.pull_calls.load(Ordering::Acquire), 1);
    assert!(client.send_back_messages.lock().unwrap().is_empty());
}
