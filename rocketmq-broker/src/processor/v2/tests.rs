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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_security_api::AuthenticatedRequestContext;
use rocketmq_security_api::Decision;
use rocketmq_security_api::Principal;
use rocketmq_security_api::RequestPolicy;
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::RPCHook;
use rocketmq_transport::api::v1::TransportSecurity;
use rocketmq_transport::api::v2::AuthorizedCommandDispatcherV2;
use rocketmq_transport::api::v2::EmbeddedDispatchOutcome;
use rocketmq_transport::api::v2::RejectRequestDecision;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::ResponseWriteObservationV2;
use rocketmq_transport::test_support::EmbeddedRequestHarnessV2;

use super::*;
use crate::config::broker_config::BrokerConfig;
use crate::latency::broker_fast_failure::FastFailureQueueKind;

const ROUTED_CODE: i32 = 91_501;
const MUTATED_CODE: i32 = 91_502;
type ObservedRequest = (i32, i32, i32, i32);

#[derive(Clone)]
struct ProbeProcessor {
    calls: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<ObservedRequest>>>,
    observations: Arc<Mutex<Vec<i32>>>,
    reject: bool,
}

impl ProbeProcessor {
    fn new(reject: bool) -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            seen: Arc::new(Mutex::new(Vec::new())),
            observations: Arc::new(Mutex::new(Vec::new())),
            reject,
        }
    }
}

impl RequestProcessorV2 for ProbeProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.seen.lock().push((
            request.original_identity().original_code(),
            request.command().code(),
            request.original_identity().original_opaque(),
            request.command().opaque(),
        ));
        request.command_mut().add_ext_field("broker-v2-probe", "mutated");
        let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_opaque(request.original_identity().original_opaque());
        Ok(HandlerOutcome::Reply(
            ResponsePlan::command(response).expect("probe response plan"),
        ))
    }

    fn reject_request(&self, _code: i32) -> RejectRequestDecision {
        if !self.reject {
            return RejectRequestDecision::Proceed;
        }
        RejectRequestDecision::Reject(
            ResponsePlan::command(RemotingCommand::create_response_command_with_code(
                ResponseCode::SystemBusy,
            ))
            .expect("probe rejection plan"),
        )
    }

    fn observe_response_write(&self, observation: ResponseWriteObservationV2) {
        self.observations.lock().push(observation.original_code());
    }
}

#[derive(Clone)]
struct PreMutatingRouter {
    inner: BrokerRequestProcessorV2<ProbeProcessor>,
}

impl RequestProcessorV2 for PreMutatingRouter {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        request.command_mut().set_code_mut(MUTATED_CODE);
        request.command_mut().set_opaque_mut(77_777);
        RequestProcessorV2::process(&mut self.inner, request).await
    }

    fn reject_request(&self, code: i32) -> RejectRequestDecision {
        RequestProcessorV2::reject_request(&self.inner, code)
    }

    fn request_ordering(&self, request: IngressRequestView<'_>) -> RequestOrdering {
        RequestProcessorV2::request_ordering(&self.inner, request)
    }

    fn observe_response_write(&self, observation: ResponseWriteObservationV2) {
        RequestProcessorV2::observe_response_write(&self.inner, observation);
    }
}

#[derive(Clone)]
struct OrderingProbeRouter {
    inner: PreMutatingRouter,
    observed: Arc<Mutex<Vec<RequestOrdering>>>,
}

impl RequestProcessorV2 for OrderingProbeRouter {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        RequestProcessorV2::process(&mut self.inner, request).await
    }

    fn reject_request(&self, code: i32) -> RejectRequestDecision {
        RequestProcessorV2::reject_request(&self.inner, code)
    }

    fn request_ordering(&self, ingress: IngressRequestView<'_>) -> RequestOrdering {
        let ordering = RequestProcessorV2::request_ordering(&self.inner, ingress);
        self.observed.lock().push(ordering);
        ordering
    }

    fn observe_response_write(&self, observation: ResponseWriteObservationV2) {
        RequestProcessorV2::observe_response_write(&self.inner, observation);
    }
}

struct AllowEmbeddedPolicy;

impl RequestPolicy for AllowEmbeddedPolicy {
    fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
        Decision::Allow
    }
}

struct RouterFixture<P> {
    owner: RuntimeOwner,
    harness: EmbeddedRequestHarnessV2<P>,
}

impl<P> RouterFixture<P>
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    fn new(router: P, hooks: Vec<Arc<dyn RPCHook>>) -> Self {
        let owner = RuntimeOwner::new(RuntimeConfig::server_default("broker-v2-router-test"))
            .expect("Broker V2 router test runtime");
        let request_context = owner.root_context().component("broker-v2-router-request");
        let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
            router,
            hooks,
            Arc::new(TransportSecurity::secure_enforced(
                Some(Arc::new(AllowEmbeddedPolicy)),
                None,
            )),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        let harness = EmbeddedRequestHarnessV2::new(
            dispatcher,
            request_context.task_group().clone(),
            Principal::new("broker-v2-router-test"),
        );
        Self { owner, harness }
    }

    async fn finish(self) {
        drop(self.harness);
        assert!(self.owner.shutdown_tasks().await.is_healthy());
        assert!(self.owner.shutdown_background().is_healthy());
    }
}

#[tokio::test]
async fn router_selects_by_immutable_original_code_and_forwards_mutable_command() {
    const ORIGINAL_OPAQUE: i32 = 43;
    let probe = ProbeProcessor::new(false);
    let seen = Arc::clone(&probe.seen);
    let observations = Arc::clone(&probe.observations);
    let mut router = BrokerRequestProcessorV2::new();
    router.register_processor(ROUTED_CODE, probe);
    let fixture = RouterFixture::new(PreMutatingRouter { inner: router }, Vec::new());

    let outcome = fixture
        .harness
        .dispatch(
            None,
            RemotingCommand::create_remoting_command(ROUTED_CODE).set_opaque(ORIGINAL_OPAQUE),
        )
        .await
        .expect("formal V2 aggregate dispatch");
    let EmbeddedDispatchOutcome::Reply(plan) = outcome else {
        panic!("formal V2 aggregate must produce one reply");
    };

    assert_eq!(plan.response_code(), ResponseCode::Success as i32);
    assert_eq!(
        seen.lock().as_slice(),
        &[(ROUTED_CODE, MUTATED_CODE, ORIGINAL_OPAQUE, 77_777)]
    );
    assert_eq!(observations.lock().as_slice(), &[ROUTED_CODE]);
    fixture.finish().await;
}

#[tokio::test]
async fn router_v2_ordering_matches_v1_before_command_mutation() {
    let probe = ProbeProcessor::new(false);
    let seen = Arc::clone(&probe.seen);
    let mut router = BrokerRequestProcessorV2::new();
    router.register_processor(RequestCode::SendMessage as i32, probe.clone());
    router.register_processor(RequestCode::QueryMessage as i32, probe.clone());
    router.register_maintenance_processor(RequestCode::MaintenanceGetCapabilities as i32, probe);
    let observed = Arc::new(Mutex::new(Vec::new()));
    let fixture = RouterFixture::new(
        OrderingProbeRouter {
            inner: PreMutatingRouter { inner: router },
            observed: Arc::clone(&observed),
        },
        Vec::new(),
    );

    let mut ordered = RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(61);
    ordered.add_ext_field("producerGroup", "producer-a");
    ordered.add_ext_field("topic", "topic-a");
    ordered.add_ext_field("queueId", "3");
    let concurrent = RemotingCommand::create_remoting_command(RequestCode::QueryMessage).set_opaque(62);
    let maintenance = RemotingCommand::create_remoting_command(RequestCode::MaintenanceGetCapabilities).set_opaque(63);
    let commands = [ordered, concurrent, maintenance];
    let expected = commands
        .iter()
        .map(request_ordering::broker_request_ordering)
        .collect::<Vec<_>>();

    for command in commands {
        let outcome = fixture
            .harness
            .dispatch(None, command)
            .await
            .expect("formal V2 ordering dispatch");
        assert!(matches!(outcome, EmbeddedDispatchOutcome::Reply(_)));
    }

    assert_eq!(observed.lock().as_slice(), expected.as_slice());
    assert!(matches!(expected[0], RequestOrdering::Ordered(_)));
    assert_eq!(expected[1], RequestOrdering::Concurrent);
    assert_eq!(expected[2], RequestOrdering::Concurrent);
    assert_eq!(
        seen.lock()
            .iter()
            .map(|(original_code, mutable_code, _, _)| (*original_code, *mutable_code))
            .collect::<Vec<_>>(),
        vec![
            (RequestCode::SendMessage as i32, MUTATED_CODE),
            (RequestCode::QueryMessage as i32, MUTATED_CODE),
            (RequestCode::MaintenanceGetCapabilities as i32, MUTATED_CODE),
        ]
    );
    fixture.finish().await;
}

#[test]
fn router_delegates_typed_rejection_and_rejects_unknown_codes() {
    let mut router = BrokerRequestProcessorV2::new();
    router.register_processor(ROUTED_CODE, ProbeProcessor::new(true));

    let RejectRequestDecision::Reject(route_plan) = router.reject_request(ROUTED_CODE) else {
        panic!("registered leaf rejection must remain affine");
    };
    assert_eq!(route_plan.response_code(), ResponseCode::SystemBusy as i32);

    let RejectRequestDecision::Reject(unknown_plan) = router.reject_request(ROUTED_CODE + 1) else {
        panic!("unknown Broker V2 code must fail before execution");
    };
    let expected = request_code_not_supported_with_factory(&application_remoting_command_factory(), ROUTED_CODE + 1);
    assert_eq!(unknown_plan.response_code(), expected.code());
}

#[test]
fn malformed_chosen_rejection_fails_closed_with_an_owned_fallback() {
    let malformed = RemotingCommand::create_remoting_command(ROUTED_CODE).mark_oneway_rpc();

    let RejectRequestDecision::Reject(plan) = response_rejection(malformed, "malformed test rejection") else {
        panic!("malformed rejection must not proceed into processor execution");
    };
    assert_eq!(plan.response_code(), ResponseCode::SystemBusy as i32);
    assert_eq!(plan.body_kind(), rocketmq_transport::api::v2::ResponseBodyKind::Empty);
}

fn fast_failure(max_count: usize) -> BrokerFastFailure {
    BrokerFastFailure::new(Arc::new(BrokerConfig {
        broker_fast_failure_enable: true,
        broker_fast_failure_pending_max_count: max_count,
        broker_fast_failure_pending_max_bytes: 64 * 1024,
        wait_time_mills_in_send_queue: 60_000,
        ..BrokerConfig::default()
    }))
}

#[tokio::test]
async fn router_fast_failure_keeps_response_affine_and_releases_run_resources() {
    let service = fast_failure(1);
    let probe = ProbeProcessor::new(false);
    let calls = Arc::clone(&probe.calls);
    let mut router = BrokerRequestProcessorV2::new();
    router.register_processor(RequestCode::SendMessage as i32, probe);
    router.set_broker_fast_failure(service.clone());
    let fixture = RouterFixture::new(router, Vec::new());

    let first = fixture
        .harness
        .dispatch(
            None,
            RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(51),
        )
        .await
        .expect("admitted fast-failure V2 dispatch");
    let EmbeddedDispatchOutcome::Reply(first) = first else {
        panic!("admitted V2 route must reply");
    };
    assert_eq!(first.response_code(), ResponseCode::Success as i32);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(service.pending_count_snapshot().iter().all(|(_, count)| *count == 0));

    let held = fast_failure_dispatch::try_admit(
        &service,
        FastFailureQueueKind::Send,
        fast_failure_dispatch::FastFailureRequestMetadata::from_command(
            &RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(52),
        ),
    )
    .expect("fill the single pending fast-failure budget");
    let rejected = fixture
        .harness
        .dispatch(
            None,
            RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(53),
        )
        .await
        .expect("budget rejection remains a typed reply");
    let EmbeddedDispatchOutcome::Reply(rejected) = rejected else {
        panic!("budget rejection must own one response plan");
    };
    assert_eq!(rejected.response_code(), ResponseCode::SystemBusy as i32);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(held);
    let budget = service.pending_budget_snapshot();
    assert_eq!(budget.current_count, 0);
    assert_eq!(budget.current_bytes, 0);

    fixture.finish().await;
}
