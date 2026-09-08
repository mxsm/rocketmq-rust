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

use std::sync::Barrier;

use parking_lot::Mutex;
use rocketmq_model::common::message::message_single::Message;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;

use super::*;
use crate::mqtrace::send_message_context::SendMessageContext;
use crate::mqtrace::send_message_hook::SendMessageHook;

#[derive(Default)]
struct HookObservations {
    before: Vec<CheetahString>,
    after: Vec<(CheetahString, i32)>,
}

struct PolicyHook {
    first: AtomicBool,
    captured: Barrier,
    resume: Barrier,
    observations: Mutex<HookObservations>,
}

impl PolicyHook {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            first: AtomicBool::new(true),
            captured: Barrier::new(2),
            resume: Barrier::new(2),
            observations: Mutex::new(HookObservations::default()),
        })
    }
}

impl SendMessageHook for Arc<PolicyHook> {
    fn hook_name(&self) -> &'static str {
        "request-policy-generation"
    }

    fn send_message_before(&self, context: &SendMessageContext) {
        self.observations.lock().before.push(context.broker_region_id.clone());
        if self.first.swap(false, Ordering::AcqRel) {
            self.captured.wait();
            self.resume.wait();
        }
    }

    fn send_message_after(&self, context: &SendMessageContext) {
        self.observations
            .lock()
            .after
            .push((context.broker_region_id.clone(), context.commercial_send_times));
    }
}

fn request(code: RequestCode, topic: &str, transactional: bool) -> RemotingCommand {
    let batch = code == RequestCode::SendBatchMessage;
    let header = SendMessageRequestHeader {
        producer_group: "policy-producer".into(),
        topic: topic.into(),
        default_topic: "TBW102".into(),
        default_topic_queue_nums: 1,
        queue_id: 0,
        sys_flag: 0,
        born_timestamp: current_millis() as i64,
        flag: 0,
        properties: transactional.then(|| {
            MessageDecoder::message_properties_to_string(&HashMap::from([(
                MessageConst::PROPERTY_TRANSACTION_PREPARED.into(),
                "true".into(),
            )]))
        }),
        reconsume_times: None,
        unit_mode: Some(false),
        batch: Some(batch),
        max_reconsume_times: None,
        topic_request_header: None,
    };
    let body = if batch {
        let mut message = Message::default();
        message.set_body(Some(Bytes::from_static(b"policy-body")));
        MessageDecoder::encode_messages(&[message])
    } else {
        Bytes::from_static(b"policy-body")
    };
    let mut command = if batch {
        RemotingCommand::create_request_command(
            code,
            SendMessageRequestHeaderV2::create_send_message_request_header_v2(&header),
        )
    } else {
        RemotingCommand::create_request_command(code, header)
    }
    .set_body(body);
    command.make_custom_header_to_net();
    command
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_policy_generation_survives_configuration_update_through_store_and_hooks() {
    for (code, transactional) in [
        (RequestCode::SendMessage, false),
        (RequestCode::SendBatchMessage, false),
        (RequestCode::SendReplyMessage, false),
        (RequestCode::SendMessage, true),
    ] {
        let mut runtime = new_phase3_test_runtime(&format!("send-policy-{}-{transactional}", code.to_i32())).await;
        let mut initial = runtime.composition.state.broker_config().as_ref().clone();
        initial.region_id = "before-region".into();
        initial.trace_on = false;
        initial.broker_identity.broker_cluster_name = "before-cluster".into();
        initial.commercial_size_per_msg = 1_048_576;
        initial.commercial_base_count = 2;
        initial.store_reply_message_enable = true;
        initial.async_topic_create_persist_enable = false;
        runtime.composition.state.set_broker_config(initial.clone()).unwrap();
        let old_host = runtime
            .composition
            .state
            .send_message_policy_state
            .snapshot()
            .store_host;
        let new_host = "127.0.0.2:20911".parse().unwrap();
        let (mut processor, _) = runtime.init_processor_checked().unwrap();
        let hook = PolicyHook::new();
        processor.install_send_hook_for_test(code, Box::new(Arc::clone(&hook)));

        let state = runtime.composition.state.send_message_policy_state.clone();
        let runtime_config = runtime.composition.state.config_state.clone();
        let updater_hook = Arc::clone(&hook);
        let updater = std::thread::spawn(move || {
            updater_hook.captured.wait();
            let mut updated = initial;
            updated.region_id = "after-region".into();
            updated.trace_on = true;
            updated.broker_identity.broker_cluster_name = "after-cluster".into();
            updated.commercial_base_count = 9;
            updated.auto_create_topic_enable = false;
            updated.reject_transaction_message = true;
            let result = runtime_config.replace_broker(updated);
            if let Ok(generation) = &result {
                state.update_broker_config(generation.broker());
                state.update_store_host(new_host);
            }
            updater_hook.resume.wait();
            result.unwrap();
        });
        let response = process_broker_request(&processor, &mut request(code, "PolicyTopic", transactional)).await;
        updater.join().unwrap();
        assert_eq!(
            response
                .ext_fields()
                .unwrap()
                .get(MessageConst::PROPERTY_MSG_REGION)
                .unwrap(),
            "before-region"
        );
        assert_eq!(
            response
                .ext_fields()
                .unwrap()
                .get(MessageConst::PROPERTY_TRACE_SWITCH)
                .unwrap(),
            "false"
        );
        let header = response
            .decode_command_custom_header::<SendMessageResponseHeader>()
            .unwrap();
        let id = MessageDecoder::decode_message_id(header.msg_id().as_str().split(',').next().unwrap()).unwrap();
        let stored = runtime
            .composition
            .state
            .message_store()
            .unwrap()
            .look_message_by_offset(id.offset)
            .unwrap();
        assert_eq!(stored.store_host, old_host);
        assert_eq!(
            stored.property(&MessageConst::PROPERTY_MSG_REGION.into()).as_deref(),
            Some("before-region")
        );
        if code != RequestCode::SendReplyMessage {
            assert_eq!(
                stored.property(&MessageConst::PROPERTY_CLUSTER.into()).as_deref(),
                Some("before-cluster")
            );
        }

        let next = process_broker_request(&processor, &mut request(code, "PolicyTopic", transactional)).await;
        assert_eq!(
            next.ext_fields()
                .unwrap()
                .get(MessageConst::PROPERTY_MSG_REGION)
                .unwrap(),
            "after-region"
        );
        if transactional {
            assert_eq!(ResponseCode::from(next.code()), ResponseCode::NoPermission);
        } else {
            let header = next
                .decode_command_custom_header::<SendMessageResponseHeader>()
                .unwrap();
            let id = MessageDecoder::decode_message_id(header.msg_id().as_str().split(',').next().unwrap()).unwrap();
            let stored = runtime
                .composition
                .state
                .message_store()
                .unwrap()
                .look_message_by_offset(id.offset)
                .unwrap();
            assert_eq!(stored.store_host, new_host);
            assert_eq!(
                stored.property(&MessageConst::PROPERTY_MSG_REGION.into()).as_deref(),
                Some("after-region")
            );
            if code != RequestCode::SendReplyMessage {
                assert_eq!(
                    stored.property(&MessageConst::PROPERTY_CLUSTER.into()).as_deref(),
                    Some("after-cluster")
                );
            }
        }
        {
            let observations = hook.observations.lock();
            assert_eq!(observations.before.as_slice(), &["before-region", "after-region"]);
            assert_eq!(observations.after[0], ("before-region".into(), 2));
            if !transactional {
                assert_eq!(observations.after[1], ("after-region".into(), 9));
            }
        }
        let rejected = process_broker_request(&processor, &mut request(code, "NewPolicyTopic", false)).await;
        assert_eq!(ResponseCode::from(rejected.code()), ResponseCode::TopicNotExist);
        drop(processor);
        runtime.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_policy_snapshot_does_not_preserve_revoked_permission() {
    let mut runtime = new_phase3_test_runtime("send-policy-permission").await;
    let mut initial = runtime.composition.state.broker_config().as_ref().clone();
    initial.async_topic_create_persist_enable = false;
    runtime.composition.state.set_broker_config(initial).unwrap();
    let (processor, _) = runtime.init_processor_checked().unwrap();
    let coordinator = runtime.composition.state.topic_config_coordinator_handle();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    coordinator
        .persist_and_register_accepted(Box::new(move || {
            Box::pin(async move {
                let _ = entered_tx.send(());
                let _ = release_rx.await;
                Ok(())
            })
        }))
        .await
        .unwrap();
    entered_rx.await.unwrap();
    let before = runtime.composition.state.message_store().unwrap().get_max_phy_offset();
    let request_processor = processor.clone();
    let sending = tokio::spawn(async move {
        process_broker_request(
            &request_processor,
            &mut request(RequestCode::SendMessage, "PolicyTopic", false),
        )
        .await
    });
    // The new topic's queued persistence proves that admission has accepted this
    // request. Revoke permission while it awaits asynchronous preparation.
    let waiting = tokio::time::timeout(Duration::from_secs(5), async {
        while coordinator.pending_count() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let mut config = runtime
        .composition
        .state
        .config_state
        .broker_snapshot()
        .as_ref()
        .clone();
    config.broker_permission = PermName::PERM_READ;
    let update = runtime.composition.state.config_state.replace_broker(config);
    let _ = release_tx.send(());
    let response = sending.await.unwrap();
    waiting.unwrap();
    update.unwrap();
    assert_eq!(ResponseCode::from(response.code()), ResponseCode::NoPermission);
    assert_eq!(
        runtime.composition.state.message_store().unwrap().get_max_phy_offset(),
        before
    );
    let next = process_broker_request(&processor, &mut request(RequestCode::SendMessage, "PolicyTopic", false)).await;
    assert_eq!(ResponseCode::from(next.code()), ResponseCode::NoPermission);
    drop(processor);
    runtime.shutdown().await;
}
