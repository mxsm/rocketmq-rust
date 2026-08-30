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

use rocketmq_transport::api::DeferredAdmission;
use rocketmq_transport::api::DeferredExpiryMargins;
use rocketmq_transport::api::DeferredWaitLimits;

use super::*;
use crate::long_polling::pop_lite_deferred::deadline::PopLiteWaitDeadline;

#[derive(Clone)]
struct DeadlineProcessor {
    service: Arc<PopLiteDeferredService>,
    deadlines: mpsc::UnboundedSender<PopLiteWaitDeadline>,
}

impl RequestProcessor for DeadlineProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let prepared = self
            .service
            .prepare(request, PopLiteRetainedEstimate::default())
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        self.deadlines
            .send(prepared.deadline())
            .map_err(|_| RocketMQError::illegal_argument("PopLite deadline observer closed"))?;
        let registration = self
            .service
            .register(prepared, request)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        Ok(HandlerOutcome::Deferred(registration))
    }
}

fn capped_service(controller: &AdmissionController) -> Arc<PopLiteDeferredService> {
    let admission = DeferredAdmission::try_configure(controller, DeferredWaitLimits::new(4, 4 * 1024 * 1024))
        .expect("capped PopLite admission");
    Arc::new(PopLiteDeferredService::new(
        admission,
        PopLiteIndexLimits::new(nonzero(4), nonzero(4), nonzero(2)),
        LiteEventDispatcher::default(),
        DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
        Duration::from_millis(100),
        nonzero(4),
    ))
}

#[tokio::test]
async fn pop_lite_deferred_max_age_expires_as_business_timeout_and_drains() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = capped_service(controller.as_ref());
    let (deadline_tx, mut deadlines) = mpsc::unbounded_channel();
    let processor = DeadlineProcessor {
        service: Arc::clone(&service),
        deadlines: deadline_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command_for("deadline-client", 301, 60_000))
        .await
        .expect("send capped PopLite waiter");
    let deadline = deadlines.recv().await.expect("observe capped PopLite deadline");
    assert!(deadline.effective_end_millis() <= current_millis().saturating_add(100));
    tokio::time::sleep_until(deadline.protocol_at()).await;

    let mut claims = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let claims = service.sweep_expired().into_claims();
            if !claims.is_empty() {
                break claims;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("protocol deadline becomes sweepable");
    assert_eq!(claims.len(), 1);
    let claim = claims.pop().expect("one capped timeout claim");
    assert_eq!(claim.reason(), DeferredWakeReason::Timeout);

    service
        .resume_claimed(
            claim,
            DeferredResumeRetainedSize::new(43),
            move |_resume, reason| async move {
                assert_eq!(reason, DeferredWakeReason::Timeout);
                RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                    ResponseCode::PollingTimeout,
                ))
                .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
            },
        )
        .await
        .expect("business timeout writes canonically");
    let response = client
        .receive_command()
        .await
        .expect("deadline connection")
        .expect("business timeout response");
    assert_eq!(response.opaque(), 301);
    assert_eq!(response.code(), ResponseCode::PollingTimeout as i32);
    let terminal = service.resource_snapshot();
    assert_eq!(terminal.admission.waiting_count(), 0);
    assert_eq!(terminal.admission.retained_bytes(), 0);
    assert_eq!(terminal.index.live, 0);
    assert_eq!(terminal.resume_execution_count, 0);
    assert_eq!(terminal.resume_execution_bytes, 0);
    running.finish().await;
}
