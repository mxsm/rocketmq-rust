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

use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_security_api::AuthenticatedRequestContext;
use rocketmq_security_api::Decision;
use rocketmq_security_api::Principal;
use rocketmq_security_api::RequestPolicy;

use super::*;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::admission::ResourceLimit;
use crate::dispatch::AuthenticationState;
use crate::dispatch::EmbeddedDispatchErrorKind;
use crate::dispatch::HandlerOutcome;
use crate::dispatch::ProtocolNoResponseReason;
use crate::dispatch::RemotingRequest;
use crate::dispatch::RequestMeta;
use crate::dispatch::RequestOrigin;
use crate::dispatch::ResponseBody;
use crate::dispatch::ResponseDisposition;
use crate::runtime::processor_v2::RejectRequestDecision;
use crate::runtime::processor_v2::ResponseWriteObservationV2;
use crate::runtime::processor_v2::ResponseWriteOutcomeV2;
use crate::security::TransportSecurity;
#[path = "tests/lifecycle.rs"]
mod lifecycle;
#[path = "tests/ownership.rs"]
mod ownership;
#[path = "tests/policy.rs"]
mod policy;
use policy::DenyPolicy;

#[derive(Clone, Copy)]
enum Behavior {
    Reply,
    Reject,
    Error,
    NoReply,
    Deferred,
    UnclaimedDeferred,
    ForgedDeferred,
    OneWayNoReply,
    CrossRequestDeferred,
    CrossRequestNoReply,
    Wait,
}

#[derive(Default)]
struct ProcessorState {
    clones: AtomicUsize,
    orderings: AtomicUsize,
    rejects: AtomicUsize,
    processes: AtomicUsize,
    request_body_pointer: Mutex<Option<usize>>,
    facts: Mutex<Vec<(bool, bool, String)>>,
    observations: Mutex<Vec<ResponseWriteObservationV2>>,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

struct TestProcessor {
    behavior: Behavior,
    state: Arc<ProcessorState>,
    response: Bytes,
}

impl TestProcessor {
    fn new(behavior: Behavior) -> (Self, Arc<ProcessorState>, usize) {
        let state = Arc::new(ProcessorState::default());
        let response = Bytes::from_static(b"channel-free embedded response");
        let response_pointer = response.as_ptr() as usize;
        (
            Self {
                behavior,
                state: Arc::clone(&state),
                response,
            },
            state,
            response_pointer,
        )
    }
}

impl Clone for TestProcessor {
    fn clone(&self) -> Self {
        self.state.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            behavior: self.behavior,
            state: Arc::clone(&self.state),
            response: self.response.clone(),
        }
    }
}

impl RequestProcessorV2 for TestProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.state.processes.fetch_add(1, Ordering::SeqCst);
        *self.state.request_body_pointer.lock().expect("request pointer lock") =
            request.command().body().map(|body| body.as_ptr() as usize);
        let embedded = matches!(
            request.origin(),
            RequestOrigin::Embedded {
                caller: EmbeddedCaller::BrokerProxy
            }
        );
        let embedded_session = matches!(request.session(), crate::session_view::SessionView::Embedded { .. });
        let principal = match request.authentication() {
            AuthenticationState::Authenticated(principal, ..) => principal.id().to_owned(),
            AuthenticationState::Anonymous | AuthenticationState::SecurityDisabled => "missing".to_owned(),
        };
        self.state
            .facts
            .lock()
            .expect("facts lock")
            .push((embedded, embedded_session, principal));
        if matches!(self.behavior, Behavior::Wait) {
            self.state.entered.notify_one();
            self.state.resume.notified().await;
        }
        match self.behavior {
            Behavior::Reply | Behavior::Reject | Behavior::Wait => {
                request.command_mut().set_code_ref(999_001);
                request.command_mut().set_opaque_mut(-900);
                Ok(HandlerOutcome::Reply(
                    ResponsePlan::bytes(
                        RemotingCommand::create_response_command_with_code(71).set_opaque(-777),
                        self.response.clone(),
                    )
                    .expect("response plan"),
                ))
            }
            Behavior::Error => Err(RocketMQError::illegal_argument("embedded processor failure")),
            Behavior::NoReply => Ok(HandlerOutcome::NoReply(
                request.protocol_no_response(ProtocolNoResponseReason::CallbackHandled)?,
            )),
            Behavior::Deferred => {
                request
                    .mark_deferred_response_taken()
                    .expect("embedded builder reserves the sealed deferred capability");
                Ok(HandlerOutcome::Deferred(
                    crate::dispatch::DeferredRegistration::for_test(request.original_identity().request_id()),
                ))
            }
            Behavior::UnclaimedDeferred | Behavior::ForgedDeferred => Ok(HandlerOutcome::Deferred(
                crate::dispatch::DeferredRegistration::for_test(request.original_identity().request_id()),
            )),
            Behavior::OneWayNoReply => Ok(HandlerOutcome::NoReply(crate::dispatch::ProtocolNoResponse::for_test(
                request.original_identity().request_id(),
                request.original_identity().original_code(),
                ProtocolNoResponseReason::CallbackHandled,
            ))),
            Behavior::CrossRequestDeferred => Ok(HandlerOutcome::Deferred(
                crate::dispatch::DeferredRegistration::for_test(
                    crate::dispatch::RequestId::real(9_999_991, 1).expect("foreign request id"),
                ),
            )),
            Behavior::CrossRequestNoReply => {
                Ok(HandlerOutcome::NoReply(crate::dispatch::ProtocolNoResponse::for_test(
                    crate::dispatch::RequestId::real(9_999_992, 1).expect("foreign request id"),
                    request.original_identity().original_code(),
                    ProtocolNoResponseReason::CallbackHandled,
                )))
            }
        }
    }

    fn reject_request(&self, _code: i32) -> RejectRequestDecision {
        self.state.rejects.fetch_add(1, Ordering::SeqCst);
        if matches!(self.behavior, Behavior::Reject) {
            RejectRequestDecision::Reject(
                ResponsePlan::bytes(
                    RemotingCommand::create_response_command_with_code(73).set_opaque(-778),
                    self.response.clone(),
                )
                .expect("rejection plan"),
            )
        } else {
            RejectRequestDecision::Proceed
        }
    }

    fn request_ordering(
        &self,
        ingress: crate::dispatch::IngressRequestView<'_>,
    ) -> crate::request_ordering::RequestOrdering {
        self.state.orderings.fetch_add(1, Ordering::SeqCst);
        assert_eq!(ingress.original_identity().original_code(), 39);
        crate::request_ordering::RequestOrdering::Concurrent
    }

    fn observe_response_write(&self, observation: ResponseWriteObservationV2) {
        self.state
            .observations
            .lock()
            .expect("observation lock")
            .push(observation);
    }
}

struct EmbeddedFixture {
    runtime: RuntimeOwner,
    _service: ChildServiceContext,
    task_group: rocketmq_runtime::TaskGroup,
}

struct AllowPolicy;

impl RequestPolicy for AllowPolicy {
    fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
        Decision::Allow
    }
}

impl EmbeddedFixture {
    fn new(name: &'static str) -> Self {
        let runtime = RuntimeOwner::new(RuntimeConfig::server_default(name)).expect("embedded V2 test runtime owner");
        let service = runtime.root_context().component(name);
        let task_group = service.task_group().clone();
        Self {
            runtime,
            _service: service,
            task_group,
        }
    }

    fn dispatcher(
        &self,
        processor: TestProcessor,
        admission: Arc<AdmissionController>,
    ) -> AuthorizedCommandDispatcherV2<TestProcessor> {
        AuthorizedCommandDispatcherV2::new(
            processor,
            Vec::new(),
            Arc::new(TransportSecurity::secure_enforced(Some(Arc::new(AllowPolicy)), None)),
            admission,
        )
    }

    async fn shutdown(self) {
        self.runtime
            .shutdown_tasks()
            .await
            .assert_no_task_leak()
            .expect("embedded fixture tasks must drain");
    }
}

fn request(one_way: bool) -> (RemotingCommand, usize) {
    let body = Bytes::from_static(b"embedded request storage");
    let pointer = body.as_ptr() as usize;
    let mut command = RemotingCommand::create_remoting_command(39)
        .set_opaque(811)
        .set_body(body);
    if one_way {
        command.mark_oneway_rpc_ref();
    }
    (command, pointer)
}

#[tokio::test]
async fn public_reply_is_channel_free_zero_copy_bound_and_observed_once_after_one_admitted_clone() {
    let fixture = EmbeddedFixture::new("embedded-v2-reply");
    let (processor, state, response_pointer) = TestProcessor::new(Behavior::Reply);
    let dispatcher = fixture.dispatcher(
        processor,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let (command, request_pointer) = request(false);

    let outcome = dispatcher
        .dispatch_embedded_v2(&fixture.task_group, Principal::new("broker-proxy"), None, command)
        .await
        .expect("embedded reply");
    let EmbeddedDispatchOutcome::Reply(plan) = outcome else {
        panic!("expected embedded reply")
    };
    let ResponseBody::Bytes(body) = plan.test_body() else {
        panic!("expected contiguous response body")
    };

    assert_eq!(body.as_ptr() as usize, response_pointer);
    assert_eq!(plan.test_head().opaque(), 811);
    assert!(plan.test_head().is_response_type());
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(state.orderings.load(Ordering::SeqCst), 1);
    assert_eq!(state.rejects.load(Ordering::SeqCst), 1);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(
        *state.request_body_pointer.lock().expect("request pointer lock"),
        Some(request_pointer)
    );
    assert_eq!(
        state.facts.lock().expect("facts lock").as_slice(),
        [(true, true, "broker-proxy".to_owned())]
    );
    {
        let observations = state.observations.lock().expect("observation lock");
        assert_eq!(observations.len(), 1);
        let ResponseWriteOutcomeV2::Written(receipt) = observations[0].outcome() else {
            panic!("local response plan must be accepted")
        };
        assert_eq!(receipt.disposition(), ResponseDisposition::InProcessAccepted);
        assert_eq!(
            observations[0].path(),
            crate::runtime::processor_v2::ResponseWritePath::Inline
        );
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn one_way_discards_processor_rejection_and_error_plans_before_binding_without_observation() {
    for behavior in [Behavior::Reply, Behavior::Reject, Behavior::Error] {
        let fixture = EmbeddedFixture::new("embedded-v2-oneway");
        let (processor, state, _) = TestProcessor::new(behavior);
        let dispatcher = fixture.dispatcher(
            processor,
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        );
        let (command, _) = request(true);

        let outcome = dispatcher
            .dispatch_embedded_v2(&fixture.task_group, Principal::new("broker-proxy"), None, command)
            .await
            .expect("one-way terminal outcome");
        assert!(matches!(outcome, EmbeddedDispatchOutcome::OneWay { .. }));
        assert!(state.observations.lock().expect("observation lock").is_empty());
        let failures = dispatcher.core.reported_failure_categories();
        if matches!(behavior, Behavior::Error) {
            assert_eq!(failures, ["processor_error"]);
        } else {
            assert!(failures.is_empty());
        }
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn non_oneway_processor_error_is_observed_without_terminal_failure_reporting() {
    let fixture = EmbeddedFixture::new("embedded-v2-processor-error-response");
    let (processor, state, _) = TestProcessor::new(Behavior::Error);
    let dispatcher = fixture.dispatcher(
        processor,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );

    let outcome = dispatcher
        .dispatch_embedded_v2(
            &fixture.task_group,
            Principal::new("broker-proxy"),
            None,
            request(false).0,
        )
        .await
        .expect("processor error response must use the local plan handoff");
    assert!(matches!(outcome, EmbeddedDispatchOutcome::Reply(_)));
    assert!(dispatcher.core.reported_failure_categories().is_empty());
    {
        let observations = state.observations.lock().expect("observation lock");
        assert_eq!(observations.len(), 1);
        assert!(matches!(observations[0].outcome(), ResponseWriteOutcomeV2::Written(_)));
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn one_way_discards_pre_admission_deadline_security_and_admission_plans_without_observation() {
    let fixture = EmbeddedFixture::new("embedded-v2-pre-oneway");

    let (processor, deadline_state, _) = TestProcessor::new(Behavior::Reply);
    let deadline_dispatcher = fixture.dispatcher(
        processor,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let outcome = deadline_dispatcher
        .dispatch_embedded_v2(
            &fixture.task_group,
            Principal::new("broker-proxy"),
            Some(RequestDeadline::after(Duration::ZERO)),
            request(true).0,
        )
        .await
        .expect("one-way pre-admission deadline");
    assert!(matches!(outcome, EmbeddedDispatchOutcome::OneWay { .. }));
    assert_eq!(deadline_state.clones.load(Ordering::SeqCst), 0);
    assert!(deadline_state.observations.lock().expect("observation lock").is_empty());

    let (processor, security_state, _) = TestProcessor::new(Behavior::Reply);
    let deny = Arc::new(DenyPolicy {
        evaluations: AtomicUsize::new(0),
        peerless: AtomicUsize::new(0),
    });
    let security_dispatcher = AuthorizedCommandDispatcherV2::new(
        processor,
        Vec::new(),
        Arc::new(TransportSecurity::secure_enforced(Some(deny), None)),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let outcome = security_dispatcher
        .dispatch_embedded_v2(
            &fixture.task_group,
            Principal::new("broker-proxy"),
            None,
            request(true).0,
        )
        .await
        .expect("one-way security denial");
    assert!(matches!(outcome, EmbeddedDispatchOutcome::OneWay { .. }));
    assert_eq!(security_state.clones.load(Ordering::SeqCst), 0);
    assert!(security_state.observations.lock().expect("observation lock").is_empty());

    let limits = AdmissionLimits {
        per_ip: ResourceLimit { count: 1, bytes: 1024 },
        per_session: ResourceLimit { count: 1, bytes: 1024 },
        control_reserve: ResourceLimit { count: 0, bytes: 0 },
        ..AdmissionLimits::default()
    };
    let admission = Arc::new(AdmissionController::new(limits));
    let held_scope = admission
        .prepare_embedded_scope(EmbeddedCaller::BrokerProxy, 9_002)
        .expect("typed embedded scope");
    let held = held_scope
        .try_acquire(AdmissionResource::Queued, 128, AdmissionClass::Data)
        .expect("hold embedded caller queue");
    let (processor, admission_state, _) = TestProcessor::new(Behavior::Reply);
    let admission_dispatcher = fixture.dispatcher(processor, admission);
    let outcome = admission_dispatcher
        .dispatch_embedded_v2(
            &fixture.task_group,
            Principal::new("broker-proxy"),
            None,
            request(true).0,
        )
        .await
        .expect("one-way admission rejection");
    assert!(matches!(outcome, EmbeddedDispatchOutcome::OneWay { .. }));
    assert_eq!(admission_state.clones.load(Ordering::SeqCst), 0);
    assert!(admission_state
        .observations
        .lock()
        .expect("observation lock")
        .is_empty());
    drop(held);
    fixture.shutdown().await;
}

#[tokio::test]
async fn expired_reply_maps_once_to_deadline_with_one_failed_observation_and_typed_source() {
    let fixture = EmbeddedFixture::new("embedded-v2-deadline-error");
    let (processor, state, _) = TestProcessor::new(Behavior::Reply);
    let dispatcher = fixture.dispatcher(
        processor,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let error = dispatcher
        .dispatch_embedded_v2(
            &fixture.task_group,
            Principal::new("broker-proxy"),
            Some(RequestDeadline::after(Duration::ZERO)),
            request(false).0,
        )
        .await
        .expect_err("expired reply handoff must fail");
    assert_eq!(error.kind(), EmbeddedDispatchErrorKind::DeadlineExceeded);
    assert!(std::error::Error::source(&error).is_some());
    assert!(!format!("{error:?}").contains("broker-proxy"));
    {
        let observations = state.observations.lock().expect("observation lock");
        assert_eq!(observations.len(), 1);
        assert!(matches!(
            observations[0].outcome(),
            ResponseWriteOutcomeV2::Failed {
                kind: crate::dispatch::ResponseErrorKind::DeadlineExceeded,
                ..
            }
        ));
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn sealed_deferred_and_protocol_no_reply_survive_inline_validation_without_response_observation() {
    for (behavior, code) in [(Behavior::Deferred, 39), (Behavior::NoReply, 39)] {
        let fixture = EmbeddedFixture::new("embedded-v2-affine-outcomes");
        let (processor, state, _) = TestProcessor::new(behavior);
        let dispatcher = fixture.dispatcher(
            processor,
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        );
        let command = RemotingCommand::create_remoting_command(code).set_opaque(812);
        let outcome = dispatcher
            .dispatch_embedded_v2(&fixture.task_group, Principal::new("broker-proxy"), None, command)
            .await
            .expect("affine embedded outcome");
        match behavior {
            Behavior::Deferred => assert!(matches!(outcome, EmbeddedDispatchOutcome::Deferred { .. })),
            Behavior::NoReply => assert!(matches!(
                outcome,
                EmbeddedDispatchOutcome::NoReply {
                    reason: ProtocolNoResponseReason::CallbackHandled,
                    ..
                }
            )),
            Behavior::Reply
            | Behavior::Reject
            | Behavior::Error
            | Behavior::Wait
            | Behavior::UnclaimedDeferred
            | Behavior::ForgedDeferred
            | Behavior::OneWayNoReply
            | Behavior::CrossRequestDeferred
            | Behavior::CrossRequestNoReply => unreachable!(),
        }
        assert!(state.observations.lock().expect("observation lock").is_empty());
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn malformed_request_and_affine_handler_contracts_map_to_unique_public_kinds() {
    let fixture = EmbeddedFixture::new("embedded-v2-contract-errors");

    let (processor, state, _) = TestProcessor::new(Behavior::UnclaimedDeferred);
    let dispatcher = fixture.dispatcher(
        processor,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let error = dispatcher
        .dispatch_embedded_v2(
            &fixture.task_group,
            Principal::new("broker-proxy"),
            None,
            request(false).0,
        )
        .await
        .expect_err("unclaimed deferred proof must fail");
    assert_eq!(error.kind(), EmbeddedDispatchErrorKind::HandlerContract);
    assert!(state.observations.lock().expect("observation lock").is_empty());

    let (processor, state, _) = TestProcessor::new(Behavior::ForgedDeferred);
    let dispatcher = fixture.dispatcher(
        processor,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let error = dispatcher
        .dispatch_embedded_v2(
            &fixture.task_group,
            Principal::new("broker-proxy"),
            None,
            request(true).0,
        )
        .await
        .expect_err("one-way deferred proof must fail");
    assert_eq!(error.kind(), EmbeddedDispatchErrorKind::OneWayContract);
    assert!(state.observations.lock().expect("observation lock").is_empty());

    let (processor, state, _) = TestProcessor::new(Behavior::OneWayNoReply);
    let dispatcher = fixture.dispatcher(
        processor,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let error = dispatcher
        .dispatch_embedded_v2(
            &fixture.task_group,
            Principal::new("broker-proxy"),
            None,
            request(true).0,
        )
        .await
        .expect_err("one-way no-reply proof must fail");
    assert_eq!(error.kind(), EmbeddedDispatchErrorKind::OneWayContract);
    assert!(state.observations.lock().expect("observation lock").is_empty());

    for behavior in [Behavior::CrossRequestDeferred, Behavior::CrossRequestNoReply] {
        let (processor, state, _) = TestProcessor::new(behavior);
        let dispatcher = fixture.dispatcher(
            processor,
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        );
        let error = dispatcher
            .dispatch_embedded_v2(
                &fixture.task_group,
                Principal::new("broker-proxy"),
                None,
                request(true).0,
            )
            .await
            .expect_err("cross-request one-way proof must fail closed");
        assert_eq!(error.kind(), EmbeddedDispatchErrorKind::HandlerContract);
        assert_eq!(state.processes.load(Ordering::SeqCst), 1);
        assert!(state.observations.lock().expect("observation lock").is_empty());
    }

    let (processor, state, _) = TestProcessor::new(Behavior::Reply);
    let dispatcher = fixture.dispatcher(
        processor,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let error = dispatcher
        .dispatch_embedded_v2(
            &fixture.task_group,
            Principal::new("broker-proxy"),
            None,
            RemotingCommand::create_response_command_with_code(39).set_opaque(811),
        )
        .await
        .expect_err("response-shaped ingress must fail request construction");
    assert_eq!(error.kind(), EmbeddedDispatchErrorKind::RequestConstruction);
    assert_eq!(state.processes.load(Ordering::SeqCst), 0);
    fixture.shutdown().await;
}

#[tokio::test]
async fn embedded_admission_rejection_uses_typed_caller_scope_without_processor_clone() {
    let fixture = EmbeddedFixture::new("embedded-v2-admission");
    let limits = AdmissionLimits {
        per_ip: ResourceLimit { count: 1, bytes: 1024 },
        per_session: ResourceLimit { count: 1, bytes: 1024 },
        control_reserve: ResourceLimit { count: 0, bytes: 0 },
        ..AdmissionLimits::default()
    };
    let admission = Arc::new(AdmissionController::new(limits));
    let held_scope = admission
        .prepare_embedded_scope(EmbeddedCaller::BrokerProxy, 9_001)
        .expect("typed embedded scope");
    let held = held_scope
        .try_acquire(AdmissionResource::Queued, 128, AdmissionClass::Data)
        .expect("hold caller-scoped queue capacity");
    let (processor, state, _) = TestProcessor::new(Behavior::Reply);
    let dispatcher = fixture.dispatcher(processor, Arc::clone(&admission));

    let outcome = dispatcher
        .dispatch_embedded_v2(
            &fixture.task_group,
            Principal::new("broker-proxy"),
            None,
            request(false).0,
        )
        .await
        .expect("reject policy returns a reply plan");
    let EmbeddedDispatchOutcome::Reply(plan) = outcome else {
        panic!("admission rejection must return an embedded reply")
    };
    assert_eq!(
        plan.response_code(),
        rocketmq_protocol::code::response_code::ResponseCode::SystemBusy.to_i32()
    );
    assert_eq!(state.clones.load(Ordering::SeqCst), 0);
    assert_eq!(state.processes.load(Ordering::SeqCst), 0);
    assert_eq!(state.observations.lock().expect("observation lock").len(), 1);
    drop(held);
    fixture.shutdown().await;
}

#[tokio::test]
async fn embedded_caller_admission_isolated_from_the_loopback_network_scope() {
    let fixture = EmbeddedFixture::new("embedded-v2-admission-isolation");
    let limits = AdmissionLimits {
        per_ip: ResourceLimit { count: 1, bytes: 1024 },
        per_session: ResourceLimit { count: 1, bytes: 1024 },
        control_reserve: ResourceLimit { count: 0, bytes: 0 },
        ..AdmissionLimits::default()
    };
    let admission = Arc::new(AdmissionController::new(limits));
    let network_scope = admission
        .prepare_scope(AdmissionScope::new(IpAddr::V4(Ipv4Addr::LOCALHOST)).with_session(8_001))
        .expect("loopback network scope");
    let held_network = network_scope
        .try_acquire(AdmissionResource::Queued, 128, AdmissionClass::Data)
        .expect("hold network loopback capacity");
    let (processor, state, _) = TestProcessor::new(Behavior::Reply);
    let dispatcher = fixture.dispatcher(processor, Arc::clone(&admission));

    assert!(matches!(
        dispatcher
            .dispatch_embedded_v2(
                &fixture.task_group,
                Principal::new("broker-proxy"),
                None,
                request(false).0
            )
            .await
            .expect("typed embedded capacity remains isolated"),
        EmbeddedDispatchOutcome::Reply(_)
    ));
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    drop(held_network);
    fixture.shutdown().await;
}
