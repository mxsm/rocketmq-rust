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
use crate::dispatch::DeferredAdmission;
use crate::dispatch::DeferredId;
use crate::dispatch::DeferredParts;
use crate::dispatch::DeferredRegistry;
use crate::dispatch::DeferredRequest;
use crate::dispatch::DeferredResumeRetainedSize;
use crate::dispatch::DeferredRetainedSizeParts;
use crate::dispatch::DeferredWaitLimits;
use crate::dispatch::DeferredWakeReason;
use crate::telemetry::TransportTelemetry;

#[derive(Clone)]
struct TerminalDeferredProcessor {
    registry: DeferredRegistry<()>,
    admission: DeferredAdmission,
    registered: Arc<Mutex<Option<DeferredId>>>,
    registered_notify: Arc<tokio::sync::Notify>,
    commit_checkpoint: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
}

impl TerminalDeferredProcessor {
    fn new(registry: DeferredRegistry<()>, admission: DeferredAdmission) -> Self {
        Self {
            registry,
            admission,
            registered: Arc::new(Mutex::new(None)),
            registered_notify: Arc::new(tokio::sync::Notify::new()),
            commit_checkpoint: None,
        }
    }

    fn with_commit_checkpoint(mut self, checkpoint: impl Fn() + Send + Sync + 'static) -> Self {
        self.commit_checkpoint = Some(Arc::new(checkpoint));
        self
    }

    async fn registered_id(&self) -> DeferredId {
        loop {
            let notified = self.registered_notify.notified();
            if let Some(id) = *self.registered.lock().expect("registered id lock") {
                return id;
            }
            notified.await;
        }
    }
}

impl RequestProcessor for TerminalDeferredProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let responder = request
            .take_deferred_responder()
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let retained = DeferredRegistry::<()>::try_retained_size(DeferredRetainedSizeParts::new(0))
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let permit = self
            .admission
            .try_reserve(retained)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let mut registration = self
            .registry
            .register(DeferredRequest::new((), DeferredParts::new(responder, permit)))
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        if let Some(checkpoint) = self.commit_checkpoint.clone() {
            registration.set_commit_checkpoint(move || checkpoint());
        }
        *self.registered.lock().expect("registered id lock") = Some(registration.deferred_id());
        self.registered_notify.notify_waiters();
        Ok(HandlerOutcome::Deferred(registration))
    }
}

fn deferred_fixture(
    name: &'static str,
) -> (
    EmbeddedFixture,
    Arc<AdmissionController>,
    DeferredAdmission,
    DeferredRegistry<()>,
) {
    let fixture = EmbeddedFixture::new(name);
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let admission = DeferredAdmission::try_configure(controller.as_ref(), DeferredWaitLimits::new(8, 1024 * 1024))
        .expect("embedded deferred admission");
    (fixture, controller, admission, DeferredRegistry::new())
}

fn dispatcher(
    processor: TerminalDeferredProcessor,
    controller: Arc<AdmissionController>,
    telemetry: TransportTelemetry,
) -> AuthorizedCommandDispatcher<TerminalDeferredProcessor> {
    AuthorizedCommandDispatcher::new_with_telemetry(
        processor,
        Vec::new(),
        Arc::new(TransportSecurity::secure_enforced(Some(Arc::new(AllowPolicy)), None)),
        controller,
        telemetry,
    )
}

fn assert_released(
    fixture: &EmbeddedFixture,
    controller: &AdmissionController,
    admission: &DeferredAdmission,
    registry: &DeferredRegistry<()>,
) {
    assert_eq!(registry.test_index_counts(), (0, 0, 0));
    assert_eq!(registry.test_claim_marker_count(), 0);
    let deferred = admission.snapshot();
    assert_eq!(deferred.waiting_count(), 0);
    assert_eq!(deferred.retained_bytes(), 0);
    let resources = controller.snapshot();
    assert_eq!(resources.queued.current_count, 0);
    assert_eq!(resources.inflight.current_count, 0);
    assert_eq!(resources.processors.current_count, 0);
    assert_eq!(fixture.task_group.task_count(), 0);
}

async fn wait_until_active(registry: &DeferredRegistry<()>, id: DeferredId) {
    for _ in 0..128 {
        if registry.test_is_active(id) {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("deferred registration did not become active");
}

async fn wait_until_execution_released(controller: &AdmissionController) {
    for _ in 0..128 {
        let resources = controller.snapshot();
        if resources.queued.current_count == 0
            && resources.inflight.current_count == 0
            && resources.processors.current_count == 0
        {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("embedded processor execution permits did not release");
}

async fn wait_until_released(registry: &DeferredRegistry<()>, fixture: &EmbeddedFixture) {
    for _ in 0..128 {
        if registry.test_index_counts() == (0, 0, 0) && fixture.task_group.task_count() == 0 {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("embedded deferred resources did not drain");
}

#[tokio::test]
async fn terminal_wait_commits_resumes_and_returns_the_final_plan_with_composition_telemetry() {
    let (fixture, controller, admission, registry) = deferred_fixture("embedded-terminal-arrival");
    let processor = TerminalDeferredProcessor::new(registry.clone(), admission.clone());
    let observer = processor.clone();
    let (telemetry, deferred_state_constructions) = TransportTelemetry::with_deferred_state_construction_capture();
    let dispatcher = Arc::new(dispatcher(processor, Arc::clone(&controller), telemetry));
    let task_group = fixture.task_group.clone();
    let dispatch = tokio::spawn({
        let dispatcher = Arc::clone(&dispatcher);
        async move {
            dispatcher
                .dispatch_embedded_wait_response(&task_group, Principal::new("broker-proxy"), None, request(false).0)
                .await
        }
    });

    let id = observer.registered_id().await;
    wait_until_active(&registry, id).await;
    wait_until_execution_released(controller.as_ref()).await;
    let resources = controller.snapshot();
    assert_eq!(resources.queued.current_count, 0);
    assert_eq!(resources.inflight.current_count, 0);
    assert_eq!(resources.processors.current_count, 0);
    let claim = registry
        .claim(id, DeferredWakeReason::MessageArrived)
        .await
        .expect("claim committed embedded wait");
    claim
        .resume(DeferredResumeRetainedSize::new(0), |(), reason| async move {
            assert_eq!(reason, DeferredWakeReason::MessageArrived);
            RemotingResponse::bytes(
                RemotingCommand::create_response_command_with_code(0),
                Bytes::from_static(b"embedded-terminal-response"),
            )
            .map_err(|error| RocketMQError::response_process_failed("terminal_wait_test", error.to_string()))
        })
        .await
        .expect("resume final embedded response");
    let outcome = dispatch
        .await
        .expect("terminal dispatch join")
        .expect("terminal embedded response");
    let EmbeddedDispatchOutcome::Reply(plan) = outcome else {
        panic!("terminal wait must return the final remoting response")
    };
    assert_eq!(plan.test_head().opaque(), 811);
    let ResponseBody::Bytes(body) = plan.test_body() else {
        panic!("terminal response must retain its contiguous body")
    };
    assert_eq!(body.as_ref(), b"embedded-terminal-response");
    assert_eq!(deferred_state_constructions.load(Ordering::SeqCst), 1);
    assert_released(&fixture, controller.as_ref(), &admission, &registry);
    fixture.shutdown().await;
}

#[tokio::test]
async fn terminal_wait_deadline_closes_the_committed_registry_entry_and_releases_all_resources() {
    let (fixture, controller, admission, registry) = deferred_fixture("embedded-terminal-deadline");
    let processor = TerminalDeferredProcessor::new(registry.clone(), admission.clone());
    let observer = processor.clone();
    let dispatcher = Arc::new(dispatcher(
        processor,
        Arc::clone(&controller),
        TransportTelemetry::noop(),
    ));
    let task_group = fixture.task_group.clone();
    let dispatch = tokio::spawn({
        let dispatcher = Arc::clone(&dispatcher);
        async move {
            dispatcher
                .dispatch_embedded_wait_response(
                    &task_group,
                    Principal::new("broker-proxy"),
                    Some(RequestDeadline::after(Duration::from_millis(25))),
                    request(false).0,
                )
                .await
        }
    });

    let id = observer.registered_id().await;
    wait_until_active(&registry, id).await;
    let error = dispatch
        .await
        .expect("terminal deadline join")
        .expect_err("terminal deadline must stop the embedded wait");
    assert_eq!(error.kind(), EmbeddedDispatchErrorKind::DeadlineExceeded);
    assert_released(&fixture, controller.as_ref(), &admission, &registry);
    fixture.shutdown().await;
}

#[tokio::test]
async fn dropping_terminal_wait_closes_the_embedded_session_and_registry_without_leaks() {
    let (fixture, controller, admission, registry) = deferred_fixture("embedded-terminal-drop");
    let processor = TerminalDeferredProcessor::new(registry.clone(), admission.clone());
    let observer = processor.clone();
    let dispatcher = Arc::new(dispatcher(
        processor,
        Arc::clone(&controller),
        TransportTelemetry::noop(),
    ));
    let task_group = fixture.task_group.clone();
    let dispatch = tokio::spawn({
        let dispatcher = Arc::clone(&dispatcher);
        async move {
            dispatcher
                .dispatch_embedded_wait_response(&task_group, Principal::new("broker-proxy"), None, request(false).0)
                .await
        }
    });

    let id = observer.registered_id().await;
    wait_until_active(&registry, id).await;
    tokio::task::yield_now().await;
    dispatch.abort();
    assert!(dispatch
        .await
        .expect_err("terminal wait must be aborted")
        .is_cancelled());
    wait_until_released(&registry, &fixture).await;
    assert_released(&fixture, controller.as_ref(), &admission, &registry);
    fixture.shutdown().await;
}

#[tokio::test]
async fn parent_cancel_during_commit_fails_closed_and_rolls_back_every_owner() {
    let (fixture, controller, admission, registry) = deferred_fixture("embedded-terminal-commit-cancel");
    let cancel_group = fixture.task_group.clone();
    let processor = TerminalDeferredProcessor::new(registry.clone(), admission.clone())
        .with_commit_checkpoint(move || cancel_group.cancel());
    let observer = processor.clone();
    let dispatcher = Arc::new(dispatcher(
        processor,
        Arc::clone(&controller),
        TransportTelemetry::noop(),
    ));

    let error = dispatcher
        .dispatch_embedded_wait_response(
            &fixture.task_group,
            Principal::new("broker-proxy"),
            None,
            request(false).0,
        )
        .await
        .expect_err("cancellation at commit must fail closed");
    assert!(matches!(
        error.kind(),
        EmbeddedDispatchErrorKind::Cancelled | EmbeddedDispatchErrorKind::DeferredCommit
    ));
    let _ = observer.registered_id().await;
    assert_released(&fixture, controller.as_ref(), &admission, &registry);
    fixture.shutdown().await;
}
