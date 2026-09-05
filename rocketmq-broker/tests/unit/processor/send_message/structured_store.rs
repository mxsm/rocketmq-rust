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

use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode as ProtocolResponseCode;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
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
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::AuthorizedCommandDispatcher;
use rocketmq_transport::api::EmbeddedDispatchOutcome;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::api::RequestId;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::ResponseObservation;
use rocketmq_transport::api::TransportSecurity;
use rocketmq_transport::test_support::EmbeddedRequestHarness;
use tokio::sync::Notify;

use super::append_message_with_control_reply;
use super::StoreHookCompletion;
use crate::processor::send_message_processor::map_put_status_to_response;
use crate::processor::send_message_processor::set_success_response_header;

const OPAQUE: i32 = 98_307;

#[derive(Clone, Copy)]
enum StoreBehavior {
    GatedSuccess,
    PutRejected,
    ImmediateError,
    Pending,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BuiltResponse {
    opaque: i32,
    code: i32,
    remark: Option<String>,
    msg_id: String,
    queue_id: i32,
    queue_offset: i64,
    transaction_id: Option<String>,
    recall_handle: Option<String>,
}

struct ProbeState {
    events: Mutex<Vec<&'static str>>,
    entered: Notify,
    entered_count: AtomicUsize,
    release: Notify,
    store_future_drops: Arc<AtomicUsize>,
    built: Mutex<Option<BuiltResponse>>,
    built_replies: Mutex<HashSet<RequestId>>,
    pending_after_write: Mutex<HashSet<RequestId>>,
}

impl ProbeState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            entered: Notify::new(),
            entered_count: AtomicUsize::new(0),
            release: Notify::new(),
            store_future_drops: Arc::new(AtomicUsize::new(0)),
            built: Mutex::new(None),
            built_replies: Mutex::new(HashSet::new()),
            pending_after_write: Mutex::new(HashSet::new()),
        })
    }
}

struct StoreFutureOwner(Arc<AtomicUsize>);

impl Drop for StoreFutureOwner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ProbeAppender {
    behavior: StoreBehavior,
    state: Arc<ProbeState>,
}

impl MessageAppender<()> for ProbeAppender {
    type Receipt = StoreAppendReceipt;

    fn append_message(
        &mut self,
        (): (),
    ) -> impl std::future::Future<Output = Result<Self::Receipt, StoreError>> + Send {
        let state = Arc::clone(&self.state);
        let owner = StoreFutureOwner(Arc::clone(&self.state.store_future_drops));
        let behavior = self.behavior;
        async move {
            let _owner = owner;
            state.events.lock().push("store_entered");
            state.entered_count.fetch_add(1, Ordering::SeqCst);
            state.entered.notify_one();
            match behavior {
                StoreBehavior::GatedSuccess => {
                    state.release.notified().await;
                    state.events.lock().push("store_completed");
                    let append_result = AppendMessageResult {
                        status: AppendMessageStatus::PutOk,
                        msg_id: Some("MID-9837".to_string()),
                        logics_offset: 71,
                        ..AppendMessageResult::default()
                    };
                    Ok(store_append_receipt(
                        PutMessageResult::new_append_result(PutMessageStatus::PutOk, Some(append_result)),
                        11,
                        11,
                    ))
                }
                StoreBehavior::PutRejected => {
                    state.events.lock().push("store_rejected");
                    Ok(store_append_receipt(
                        PutMessageResult::new_default(PutMessageStatus::MessageIllegal),
                        0,
                        0,
                    ))
                }
                StoreBehavior::ImmediateError => {
                    state.events.lock().push("store_failed");
                    Err(StoreError::new(
                        &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE,
                        StoreOperation::Append,
                    ))
                }
                StoreBehavior::Pending => std::future::pending::<Result<StoreAppendReceipt, StoreError>>().await,
            }
        }
    }
}

#[derive(Clone)]
struct StoreProbeProcessor {
    behavior: StoreBehavior,
    state: Arc<ProbeState>,
}

impl RequestProcessor for StoreProbeProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let control = request.control().clone();
        let identity = request.original_identity();
        let original_opaque = identity.original_opaque();
        let request_id = identity.request_id();
        let original_one_way = identity.is_one_way();
        self.state.events.lock().push("before_hook");
        let mut store = ProbeAppender {
            behavior: self.behavior,
            state: Arc::clone(&self.state),
        };
        let state_for_response = Arc::clone(&self.state);
        let reply = append_message_with_control_reply(control, &mut store, (), move |result| {
            let (mut response, completion, msg_id, queue_id, queue_offset, transaction_id, recall_handle) = match result
            {
                Ok(receipt) => {
                    let mut response = application_remoting_command_factory()
                        .create_success_response_command_with_header(SendMessageResponseHeader::default());
                    if !map_put_status_to_response(receipt.result().put_message_status(), &mut response) {
                        (
                            response,
                            StoreHookCompletion::BeforeReply,
                            String::new(),
                            0,
                            0,
                            None,
                            None,
                        )
                    } else {
                        let header = response
                            .read_custom_header_mut::<SendMessageResponseHeader>()
                            .expect("structured Send response header");
                        set_success_response_header(
                            header,
                            &receipt,
                            3,
                            Some("TX-9837".into()),
                            Some("RECALL-9837".into()),
                        );
                        let msg_id = header.msg_id().to_string();
                        let queue_id = header.queue_id();
                        let queue_offset = header.queue_offset();
                        let transaction_id = header.transaction_id().map(ToString::to_string);
                        let recall_handle = header.recall_handle().map(ToString::to_string);
                        (
                            response,
                            StoreHookCompletion::AfterCanonicalWrite,
                            msg_id,
                            queue_id,
                            queue_offset,
                            transaction_id,
                            recall_handle,
                        )
                    }
                }
                Err(remark) => (
                    crate::processor::system_error_response(
                        &application_remoting_command_factory(),
                        original_opaque,
                        remark.to_string(),
                    ),
                    StoreHookCompletion::NoAfterHook,
                    String::new(),
                    0,
                    0,
                    None,
                    None,
                ),
            };
            response.set_opaque_mut(original_opaque);
            *state_for_response.built.lock() = Some(BuiltResponse {
                opaque: response.opaque(),
                code: response.code(),
                remark: response.remark().map(ToString::to_string),
                msg_id,
                queue_id,
                queue_offset,
                transaction_id,
                recall_handle,
            });
            (response, completion)
        })
        .await
        .map_err(|_| RocketMQError::invariant_violated("structured Send reply conversion failed"))?;
        let (outcome, completion) = reply.into_parts();
        if !original_one_way {
            assert!(
                self.state.built_replies.lock().insert(request_id),
                "one RequestId owns at most one built reply"
            );
        }
        match completion {
            StoreHookCompletion::AfterCanonicalWrite if !original_one_way => {
                assert!(
                    self.state.pending_after_write.lock().insert(request_id),
                    "one RequestId owns at most one pending after-hook"
                );
            }
            StoreHookCompletion::AfterCanonicalWrite => self.state.events.lock().push("after_hook"),
            StoreHookCompletion::BeforeReply => self.state.events.lock().push("after_hook"),
            StoreHookCompletion::NoAfterHook => {}
        }
        Ok(outcome)
    }

    fn observe_response(&self, observation: ResponseObservation) {
        let Some(observation) = observation.write_projection() else {
            return;
        };
        assert_eq!(observation.original_code(), RequestCode::SendMessage as i32);
        let request_id = observation.request_id();
        if self.state.built_replies.lock().remove(&request_id) {
            self.state.events.lock().push("write_observed");
        }
        if self.state.pending_after_write.lock().remove(&request_id) {
            self.state.events.lock().push("after_hook");
        }
    }
}

struct AllowEmbeddedPolicy;

impl RequestPolicy for AllowEmbeddedPolicy {
    fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
        Decision::Allow
    }
}

struct Fixture {
    owner: RuntimeOwner,
    context: rocketmq_runtime::ChildServiceContext,
    harness: EmbeddedRequestHarness<StoreProbeProcessor>,
}

impl Fixture {
    fn new(name: &'static str, behavior: StoreBehavior, state: Arc<ProbeState>) -> Self {
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default(name))
            .expect("test runtime configuration is valid")
            .build()
            .expect("structured store runtime");
        let context = owner.root_context().component(
            rocketmq_runtime::ScopeId::try_new(format!("{name}.request"))
                .expect("the request scope has a fixed nonblank suffix"),
        );
        let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
            StoreProbeProcessor { behavior, state },
            Vec::new(),
            Arc::new(TransportSecurity::secure_enforced(
                Some(Arc::new(AllowEmbeddedPolicy)),
                None,
            )),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        let harness = EmbeddedRequestHarness::new(
            dispatcher,
            context.task_group().clone(),
            Principal::new("structured-store-test"),
        );
        Self {
            owner,
            context,
            harness,
        }
    }

    async fn finish(self) {
        drop(self.harness);
        drop(self.context);
        assert!(self.owner.shutdown_tasks().await.is_healthy());
        assert!(self.owner.shutdown_background().is_healthy());
    }
}

fn request(one_way: bool) -> RemotingCommand {
    let command = RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(OPAQUE);
    if one_way {
        command.mark_oneway_rpc()
    } else {
        command
    }
}

#[tokio::test]
async fn structured_send_store_await_preserves_hook_order_and_response_side_contracts() {
    let state = ProbeState::new();
    let fixture = Fixture::new(
        "structured-send-success",
        StoreBehavior::GatedSuccess,
        Arc::clone(&state),
    );
    let outcome = {
        let entered = state.entered.notified();
        let dispatch = fixture.harness.dispatch(None, request(false));
        tokio::pin!(dispatch);
        tokio::select! {
            () = entered => {}
            result = &mut dispatch => panic!("store completed before its explicit release: {result:?}"),
        }
        assert_eq!(state.events.lock().as_slice(), ["before_hook", "store_entered"]);
        state.release.notify_one();
        dispatch.await.expect("structured Send reply")
    };
    let EmbeddedDispatchOutcome::Reply(plan) = outcome else {
        panic!("structured Send must return one reply")
    };
    assert_eq!(plan.response_code(), ProtocolResponseCode::Success as i32);
    assert_eq!(
        state.events.lock().as_slice(),
        [
            "before_hook",
            "store_entered",
            "store_completed",
            "write_observed",
            "after_hook"
        ]
    );
    assert_eq!(state.store_future_drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        state.built.lock().as_ref(),
        Some(&BuiltResponse {
            opaque: OPAQUE,
            code: ProtocolResponseCode::Success as i32,
            remark: None,
            msg_id: "MID-9837".to_string(),
            queue_id: 3,
            queue_offset: 71,
            transaction_id: Some("TX-9837".to_string()),
            recall_handle: Some("RECALL-9837".to_string()),
        })
    );
    drop(plan);
    fixture.finish().await;
}

#[tokio::test]
async fn structured_send_store_error_is_one_visible_reply() {
    let state = ProbeState::new();
    let fixture = Fixture::new(
        "structured-send-error",
        StoreBehavior::ImmediateError,
        Arc::clone(&state),
    );
    let EmbeddedDispatchOutcome::Reply(plan) = fixture
        .harness
        .dispatch(None, request(false))
        .await
        .expect("structured store error reply")
    else {
        panic!("store error must return one reply")
    };
    assert_eq!(plan.response_code(), ProtocolResponseCode::SystemError as i32);
    assert_eq!(state.store_future_drops.load(Ordering::SeqCst), 1);
    {
        let built = state.built.lock();
        let built = built.as_ref().expect("store error response built");
        assert_eq!(plan.response_code(), built.code);
        assert_eq!(built.opaque, OPAQUE);
        assert_eq!(
            built.remark.as_deref(),
            Some("storage.backend.unavailable: Storage backend is unavailable")
        );
    }
    assert_eq!(
        state.events.lock().as_slice(),
        ["before_hook", "store_entered", "store_failed", "write_observed"]
    );
    drop(plan);
    fixture.finish().await;
}

#[tokio::test]
async fn structured_send_rejected_status_runs_after_hook_before_canonical_write() {
    let state = ProbeState::new();
    let fixture = Fixture::new(
        "structured-send-rejected",
        StoreBehavior::PutRejected,
        Arc::clone(&state),
    );
    let EmbeddedDispatchOutcome::Reply(plan) = fixture
        .harness
        .dispatch(None, request(false))
        .await
        .expect("structured rejected-status reply")
    else {
        panic!("rejected PutMessageStatus must return one reply")
    };
    assert_eq!(plan.response_code(), ProtocolResponseCode::MessageIllegal as i32);
    assert_eq!(
        state.events.lock().as_slice(),
        [
            "before_hook",
            "store_entered",
            "store_rejected",
            "after_hook",
            "write_observed"
        ]
    );
    assert!(state.pending_after_write.lock().is_empty());
    drop(plan);
    fixture.finish().await;
}

#[tokio::test]
async fn structured_send_after_write_completions_are_correlated_by_request_id() {
    let state = ProbeState::new();
    let fixture = Fixture::new(
        "structured-send-request-ids",
        StoreBehavior::GatedSuccess,
        Arc::clone(&state),
    );
    let (first, second) = {
        let first = fixture.harness.dispatch(None, request(false));
        let second = fixture.harness.dispatch(None, request(false));
        let both = async { tokio::join!(first, second) };
        tokio::pin!(both);
        while state.entered_count.load(Ordering::SeqCst) < 2 {
            let entered = state.entered.notified();
            tokio::select! {
                () = entered => {}
                result = &mut both => panic!("stores completed before explicit release: {result:?}"),
            }
        }
        state.release.notify_waiters();
        both.await
    };
    assert!(matches!(
        first.expect("first structured Send"),
        EmbeddedDispatchOutcome::Reply(_)
    ));
    assert!(matches!(
        second.expect("second structured Send"),
        EmbeddedDispatchOutcome::Reply(_)
    ));

    {
        let events = state.events.lock();
        assert_eq!(events.iter().filter(|event| **event == "write_observed").count(), 2);
        assert_eq!(events.iter().filter(|event| **event == "after_hook").count(), 2);
    }
    assert!(state.built_replies.lock().is_empty());
    assert!(state.pending_after_write.lock().is_empty());
    fixture.finish().await;
}

#[tokio::test]
async fn structured_send_one_way_completes_the_after_hook_without_a_write_observer() {
    let state = ProbeState::new();
    let fixture = Fixture::new(
        "structured-send-one-way",
        StoreBehavior::GatedSuccess,
        Arc::clone(&state),
    );
    let outcome = {
        let entered = state.entered.notified();
        let dispatch = fixture.harness.dispatch(None, request(true));
        tokio::pin!(dispatch);
        tokio::select! {
            () = entered => {}
            result = &mut dispatch => panic!("one-way store completed before its explicit release: {result:?}"),
        }
        state.release.notify_one();
        dispatch.await.expect("one-way structured Send dispatch")
    };

    assert!(matches!(outcome, EmbeddedDispatchOutcome::OneWay { .. }));
    assert_eq!(
        state.events.lock().as_slice(),
        ["before_hook", "store_entered", "store_completed", "after_hook"]
    );
    assert!(state.built_replies.lock().is_empty());
    assert!(state.pending_after_write.lock().is_empty());
    fixture.finish().await;
}

#[tokio::test]
async fn structured_send_parent_cancel_stops_waiting_and_suppresses_reply() {
    let state = ProbeState::new();
    let fixture = Fixture::new("structured-send-cancel", StoreBehavior::Pending, Arc::clone(&state));
    let outcome = {
        let entered = state.entered.notified();
        let dispatch = fixture.harness.dispatch(None, request(false));
        tokio::pin!(dispatch);
        tokio::select! {
            () = entered => {}
            result = &mut dispatch => panic!("pending store completed before parent cancellation: {result:?}"),
        }
        fixture.context.task_group().cancel();
        dispatch
            .await
            .expect("parent cancellation is a source-free dispatch outcome")
    };
    assert!(matches!(outcome, EmbeddedDispatchOutcome::Cancelled));
    // Future ownership is released promptly and no reply is constructed. The
    // test deliberately makes no assertion that a real backend rolled back an
    // operation it may already have accepted.
    assert_eq!(state.store_future_drops.load(Ordering::SeqCst), 1);
    assert!(state.built.lock().is_none());
    assert!(state.built_replies.lock().is_empty());
    assert!(state.pending_after_write.lock().is_empty());
    fixture.finish().await;
}

#[tokio::test]
async fn structured_send_expired_deadline_suppresses_store_and_reply() {
    let state = ProbeState::new();
    let fixture = Fixture::new("structured-send-deadline", StoreBehavior::Pending, Arc::clone(&state));
    let outcome = fixture
        .harness
        .dispatch(Some(RequestDeadline::after(Duration::ZERO)), request(false))
        .await
        .expect("deadline is a source-free dispatch outcome");
    assert!(matches!(outcome, EmbeddedDispatchOutcome::DeadlineExceeded));
    assert_eq!(state.store_future_drops.load(Ordering::SeqCst), 0);
    assert!(state.events.lock().is_empty());
    assert!(state.built.lock().is_none());
    assert!(state.built_replies.lock().is_empty());
    assert!(state.pending_after_write.lock().is_empty());
    fixture.finish().await;
}
