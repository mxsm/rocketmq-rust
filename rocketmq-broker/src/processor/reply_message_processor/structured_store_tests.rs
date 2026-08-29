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

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode as ProtocolResponseCode;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_protocol::protocol::header::reply_message_request_header::ReplyMessageRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_security_api::AuthenticatedRequestContext;
use rocketmq_security_api::Decision;
use rocketmq_security_api::Principal;
use rocketmq_security_api::RequestPolicy;
use rocketmq_store::store_append_receipt;
use rocketmq_store::AppendMessageResult;
use rocketmq_store::AppendMessageStatus;
use rocketmq_store::PutMessageResult;
use rocketmq_store::PutMessageStatus;
use rocketmq_store::StoreAppendReceipt;
use rocketmq_store_api::MessageAppender;
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::TransportSecurity;
use rocketmq_transport::api::v2::AuthorizedCommandDispatcherV2;
use rocketmq_transport::api::v2::EmbeddedDispatchOutcome;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::ResponseWriteObservationV2;
use rocketmq_transport::test_support::EmbeddedRequestHarnessV2;

use super::add_reply_response_metadata;
use super::append_reply_message_with_control_reply;
use super::apply_reply_store_result;
use super::push_reply_message;
use super::ReplyPushPort;
use super::ReplyPushPortError;
use crate::processor::send_message_processor::structured_store::StoreHookCompletion;

const OPAQUE: i32 = 98_308;

#[derive(Clone, Copy)]
enum StoreBehavior {
    Success,
    Error,
}

#[derive(Debug, Eq, PartialEq)]
struct BuiltReply {
    opaque: i32,
    code: i32,
    remark: Option<String>,
    region: Option<String>,
    trace: Option<String>,
    msg_id: String,
    queue_id: i32,
    queue_offset: i64,
}

struct State {
    events: Mutex<Vec<&'static str>>,
    built: Mutex<Option<BuiltReply>>,
}

#[derive(Clone, Copy)]
enum PushBehavior {
    Success,
    NonSuccess,
    MissingChannel,
    CallError,
}

#[derive(Debug, Eq, PartialEq)]
struct PushCall {
    sender_id: String,
    born_host: String,
    store_host: String,
    topic: String,
    body: Option<Bytes>,
    timeout_millis: u64,
    properties: Option<String>,
}

struct ProbePushPort {
    behavior: PushBehavior,
    state: Arc<State>,
    calls: Arc<Mutex<Vec<PushCall>>>,
}

impl ProbePushPort {
    fn new(behavior: PushBehavior, state: Arc<State>) -> Self {
        Self {
            behavior,
            state,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl ReplyPushPort for ProbePushPort {
    type Target = String;

    fn acquire(&mut self, sender_id: &str) -> Result<Self::Target, ReplyPushPortError> {
        if matches!(self.behavior, PushBehavior::MissingChannel) {
            Err(ReplyPushPortError::SessionNotFound)
        } else {
            Ok(sender_id.to_string())
        }
    }

    async fn push(
        &mut self,
        sender_id: Self::Target,
        header: ReplyMessageRequestHeader,
        body: Option<Bytes>,
        timeout_millis: u64,
    ) -> Result<RemotingCommand, ReplyPushPortError> {
        self.state.events.lock().push("push_completed");
        self.calls.lock().push(PushCall {
            sender_id,
            born_host: header.born_host.to_string(),
            store_host: header.store_host.to_string(),
            topic: header.topic.to_string(),
            body,
            timeout_millis,
            properties: header.properties.map(|value| value.to_string()),
        });
        match self.behavior {
            PushBehavior::Success => Ok(RemotingCommand::create_response_command_with_code(
                ProtocolResponseCode::Success,
            )),
            PushBehavior::NonSuccess => Ok(crate::processor::system_error_response(
                &application_remoting_command_factory(),
                0,
                "probe remote non-success",
            )),
            PushBehavior::MissingChannel => unreachable!("missing channels stop before message mutation"),
            PushBehavior::CallError => Err(ReplyPushPortError::TestCall),
        }
    }
}

fn push_message() -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT),
        CheetahString::from_static_str("reply-client"),
    );
    message.set_body(Bytes::from_static(b"reply-body"));
    message
}

fn push_header(
) -> rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader
{
    rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader {
        producer_group: "reply-producer".into(),
        topic: "reply-topic".into(),
        default_topic: "TBW102".into(),
        default_topic_queue_nums: 4,
        queue_id: 5,
        born_timestamp: 83,
        ..Default::default()
    }
}

impl State {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            built: Mutex::new(None),
        })
    }
}

struct ReplyAppender {
    behavior: StoreBehavior,
    state: Arc<State>,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct ReplyStoreError(&'static str);

impl MessageAppender<()> for ReplyAppender {
    type Receipt = StoreAppendReceipt;
    type Error = ReplyStoreError;

    fn append_message(
        &mut self,
        (): (),
    ) -> impl std::future::Future<Output = Result<Self::Receipt, Self::Error>> + Send {
        self.state.events.lock().push("store_entered");
        std::future::ready(match self.behavior {
            StoreBehavior::Success => {
                let append_result = AppendMessageResult {
                    status: AppendMessageStatus::PutOk,
                    msg_id: Some("RID-9838".to_string()),
                    logics_offset: 0,
                    ..AppendMessageResult::default()
                };
                Ok(store_append_receipt(
                    PutMessageResult::new_append_result(PutMessageStatus::PutOk, Some(append_result)),
                    23,
                    23,
                ))
            }
            StoreBehavior::Error => Err(ReplyStoreError("reply store not available")),
        })
    }
}

#[derive(Clone)]
struct ReplyProbeProcessor {
    behavior: StoreBehavior,
    state: Arc<State>,
}

impl RequestProcessorV2 for ReplyProbeProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let control = request.control().clone();
        let identity = request.original_identity();
        let opaque = identity.original_opaque();
        self.state.events.lock().push("before_hook");
        let mut push_port = ProbePushPort::new(PushBehavior::Success, Arc::clone(&self.state));
        let mut push_message = push_message();
        let push_result = push_reply_message(
            &mut push_port,
            "127.0.0.1:18080".parse().expect("inbound peer"),
            "127.0.0.1:10911".parse().expect("store host"),
            &push_header(),
            &mut push_message,
        )
        .await;
        assert!(push_result.success);
        let mut store = ReplyAppender {
            behavior: self.behavior,
            state: Arc::clone(&self.state),
        };
        let state = Arc::clone(&self.state);
        let reply = append_reply_message_with_control_reply(control, &mut store, (), move |result| {
            let (mut response, msg_id, queue_id, queue_offset) = match result {
                Ok(receipt) => {
                    let mut header = SendMessageResponseHeader::default();
                    assert!(apply_reply_store_result(receipt.result(), &mut header, 5, 1024));
                    let msg_id = header.msg_id().to_string();
                    let queue_id = header.queue_id();
                    let queue_offset = header.queue_offset();
                    (
                        application_remoting_command_factory().create_success_response_command_with_header(header),
                        msg_id,
                        queue_id,
                        queue_offset,
                    )
                }
                Err(remark) => (
                    crate::processor::system_error_response(&application_remoting_command_factory(), opaque, remark.0),
                    String::new(),
                    0,
                    0,
                ),
            };
            response.set_opaque_mut(opaque);
            add_reply_response_metadata(&mut response, "region-r", true);
            let fields = response.ext_fields().cloned().unwrap_or_default();
            *state.built.lock() = Some(BuiltReply {
                opaque: response.opaque(),
                code: response.code(),
                remark: response.remark().map(ToString::to_string),
                region: fields.get(MessageConst::PROPERTY_MSG_REGION).map(ToString::to_string),
                trace: fields.get(MessageConst::PROPERTY_TRACE_SWITCH).map(ToString::to_string),
                msg_id,
                queue_id,
                queue_offset,
            });
            (response, StoreHookCompletion::BeforeReply)
        })
        .await
        .map_err(|_| RocketMQError::invariant_violated("structured Reply conversion failed"))?;
        let (outcome, completion) = reply.into_parts();
        assert_eq!(completion, StoreHookCompletion::BeforeReply);
        self.state.events.lock().push("after_hook");
        Ok(outcome)
    }

    fn observe_response_write(&self, observation: ResponseWriteObservationV2) {
        assert_eq!(observation.original_code(), RequestCode::SendReplyMessage as i32);
        self.state.events.lock().push("write_observed");
    }
}

struct AllowEmbeddedPolicy;

impl RequestPolicy for AllowEmbeddedPolicy {
    fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
        Decision::Allow
    }
}

async fn dispatch(behavior: StoreBehavior, state: Arc<State>, name: &'static str) -> EmbeddedDispatchOutcome {
    let owner = RuntimeOwner::new(RuntimeConfig::server_default(name)).expect("structured Reply runtime");
    let context = owner.root_context().component(format!("{name}.request"));
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        ReplyProbeProcessor { behavior, state },
        Vec::new(),
        Arc::new(TransportSecurity::secure_enforced(
            Some(Arc::new(AllowEmbeddedPolicy)),
            None,
        )),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    ));
    let harness = EmbeddedRequestHarnessV2::new(
        dispatcher,
        context.task_group().clone(),
        Principal::new("structured-reply-test"),
    );
    let outcome = harness
        .dispatch(
            None,
            RemotingCommand::create_remoting_command(RequestCode::SendReplyMessage).set_opaque(OPAQUE),
        )
        .await
        .expect("structured Reply dispatch");
    drop(harness);
    drop(context);
    assert!(owner.shutdown_tasks().await.is_healthy());
    assert!(owner.shutdown_background().is_healthy());
    outcome
}

#[tokio::test]
async fn structured_reply_store_leaf_preserves_push_hook_metadata_and_error_visibility() {
    let success = State::new();
    let EmbeddedDispatchOutcome::Reply(success_plan) =
        dispatch(StoreBehavior::Success, Arc::clone(&success), "structured-reply-success").await
    else {
        panic!("successful Reply store must return one reply")
    };
    assert_eq!(success_plan.response_code(), ProtocolResponseCode::Success as i32);
    assert_eq!(
        success.events.lock().as_slice(),
        [
            "before_hook",
            "push_completed",
            "store_entered",
            "after_hook",
            "write_observed"
        ]
    );
    assert_eq!(
        success.built.lock().as_ref(),
        Some(&BuiltReply {
            opaque: OPAQUE,
            code: ProtocolResponseCode::Success as i32,
            remark: None,
            region: Some("region-r".to_string()),
            trace: Some("true".to_string()),
            msg_id: "RID-9838".to_string(),
            queue_id: 5,
            queue_offset: 0,
        })
    );
    drop(success_plan);

    let failed = State::new();
    let EmbeddedDispatchOutcome::Reply(error_plan) =
        dispatch(StoreBehavior::Error, Arc::clone(&failed), "structured-reply-error").await
    else {
        panic!("failed Reply store must return one visible reply")
    };
    assert_eq!(error_plan.response_code(), ProtocolResponseCode::SystemError as i32);
    {
        let built = failed.built.lock();
        let built = built.as_ref().expect("Reply store error response built");
        assert_eq!(error_plan.response_code(), built.code);
        assert_eq!(built.opaque, OPAQUE);
        assert_eq!(built.remark.as_deref(), Some("reply store not available"));
    }
    assert_eq!(
        failed.events.lock().as_slice(),
        [
            "before_hook",
            "push_completed",
            "store_entered",
            "after_hook",
            "write_observed"
        ]
    );
}

#[tokio::test]
async fn reply_push_capability_covers_success_missing_channel_non_success_and_call_error() {
    let cases = [
        (PushBehavior::Success, true, ""),
        (
            PushBehavior::MissingChannel,
            false,
            "push reply message fail, session of <reply-client> not found.",
        ),
        (
            PushBehavior::NonSuccess,
            false,
            "push reply message to reply-clientfail.",
        ),
        (
            PushBehavior::CallError,
            false,
            "push reply message to reply-clientfail.",
        ),
    ];

    for (behavior, expected_success, expected_remark) in cases {
        let state = State::new();
        let mut port = ProbePushPort::new(behavior, state);
        let calls = Arc::clone(&port.calls);
        let mut message = push_message();
        let result = push_reply_message(
            &mut port,
            "127.0.0.1:18080".parse::<SocketAddr>().expect("inbound peer"),
            "127.0.0.1:10911".parse::<SocketAddr>().expect("store host"),
            &push_header(),
            &mut message,
        )
        .await;

        assert_eq!(result.success, expected_success);
        assert_eq!(result.remark, expected_remark);
        let calls = calls.lock();
        if matches!(behavior, PushBehavior::MissingChannel) {
            assert!(calls.is_empty());
            assert!(
                message
                    .property(&CheetahString::from_static_str(MessageConst::PROPERTY_PUSH_REPLY_TIME))
                    .is_none(),
                "missing-channel compatibility must not mutate the message before optional storage"
            );
            continue;
        }
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].sender_id, "reply-client");
        assert_eq!(calls[0].born_host, "127.0.0.1:18080");
        assert_eq!(calls[0].store_host, "127.0.0.1:10911");
        assert_eq!(calls[0].topic, "reply-topic");
        assert_eq!(calls[0].body.as_deref(), Some(b"reply-body".as_slice()));
        assert_eq!(
            calls[0].timeout_millis,
            super::PUSH_REPLY_MESSAGE_TO_CLIENT_TIMEOUT_MILLIS
        );
        assert!(
            calls[0]
                .properties
                .as_deref()
                .is_some_and(|properties| properties.contains(MessageConst::PROPERTY_PUSH_REPLY_TIME)),
            "push timestamp must be captured before the capability call"
        );
    }
}
