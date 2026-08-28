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

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use rocketmq_error::RocketMQResult;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use tokio::sync::Notify;

use super::execute_work;
use super::ResumeStopView;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::dispatch::DeferredAdmission;
use crate::dispatch::DeferredParts;
use crate::dispatch::DeferredRegistry;
use crate::dispatch::DeferredRequest;
use crate::dispatch::DeferredRetainedSizeParts;
use crate::dispatch::DeferredWaitLimits;
use crate::dispatch::DeferredWakeReason;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestControlView;
use crate::dispatch::RequestMeta;
use crate::dispatch::ResponsePlan;
use crate::dispatch::ResponseSink;
use crate::session_view::EmbeddedSessionRecord;
use crate::telemetry::TransportTelemetry;

struct ReadyRetainedFuture {
    output: Option<RocketMQResult<ResponsePlan>>,
    drops: Arc<AtomicUsize>,
}

impl Future for ReadyRetainedFuture {
    type Output = RocketMQResult<ResponsePlan>;

    fn poll(mut self: Pin<&mut Self>, _context: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        std::task::Poll::Ready(self.output.take().expect("retained future is polled once"))
    }
}

impl Drop for ReadyRetainedFuture {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}

#[tokio::test]
async fn completed_handler_future_is_retained_until_canonical_response_handoff_terminal() {
    let runtime = RuntimeOwner::new(RuntimeConfig::server_default("deferred-resume-handler-owner"))
        .expect("handler-owner runtime");
    let parent = runtime.root_context().component("handler-owner").task_group().clone();
    let session = EmbeddedSessionRecord::new(98_330);
    let control = RequestControlView::from_meta(
        &RequestMeta::new(Instant::now(), None),
        session.view().state().clone(),
        &parent,
    );
    let checked = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (sink, _receiver, _attempts) =
        ResponseSink::local_plan_with_handoff_gate(control.clone(), Arc::clone(&checked), Arc::clone(&release));
    let original = OriginalRequestIdentity::capture(
        98_330,
        &AtomicU64::new(1),
        &RemotingCommand::create_remoting_command(39).set_opaque(833),
    )
    .expect("handler-owner identity");
    let responder = sink
        .deferred_seed_for_test(TransportTelemetry::noop(), session.view().id(), control)
        .into_responder(original);
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(1, 4096))
        .expect("handler-owner admission");
    let retained = DeferredRegistry::<()>::try_retained_size(DeferredRetainedSizeParts::new(0))
        .expect("handler-owner retained size");
    let permit = admission.try_reserve(retained).expect("handler-owner wait permit");
    let registry = DeferredRegistry::new();
    let registration = registry
        .register(DeferredRequest::new((), DeferredParts::new(responder, permit)))
        .expect("handler-owner registration");
    let id = registration.deferred_id();
    registration.commit().expect("publish handler-owner registration");
    let claim = registry
        .claim(id, DeferredWakeReason::MessageArrived)
        .await
        .expect("claim handler-owner registration");
    let parts = claim.into_execution_parts();
    let stop_view = ResumeStopView::from_execution_parts(&parts);
    let drops = Arc::new(AtomicUsize::new(0));
    let future_drops = Arc::clone(&drops);
    let execution = tokio::spawn(execute_work(
        parts,
        move |(), _reason| ReadyRetainedFuture {
            output: Some(Ok(ResponsePlan::command(
                RemotingCommand::create_response_command_with_code(0),
            )
            .expect("handler-owner response plan"))),
            drops: future_drops,
        },
        stop_view,
    ));

    checked.notified().await;
    assert_eq!(
        drops.load(Ordering::Acquire),
        0,
        "a ready handler future still owns affine terminal resources while response delivery is blocked"
    );
    release.notify_one();
    execution
        .await
        .expect("handler-owner execution task")
        .expect("canonical local response handoff");
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert_eq!(admission.snapshot().waiting_count(), 0);
}
