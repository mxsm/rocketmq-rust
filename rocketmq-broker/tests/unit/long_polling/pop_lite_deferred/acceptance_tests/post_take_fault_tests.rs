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
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredRegisterRejectionKind;

#[derive(Clone)]
struct ExpiryAttachmentFaultProcessor {
    service: Arc<PopLiteDeferredService>,
    observed: mpsc::UnboundedSender<PopLiteDeferredRegisterRejectionKind>,
}

impl RequestProcessor for ExpiryAttachmentFaultProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let prepared = prepared_or_test_error(self.service.prepare(request, PopLiteRetainedEstimate::default()))?;
        let rejection = match self.service.register(prepared, request) {
            Ok(PopLiteDeferredRegisterOutcome::Rejected(rejection)) => rejection,
            Ok(PopLiteDeferredRegisterOutcome::Registered(_)) | Err(_) => {
                panic!("the injected post-take expiry attachment must fail closed")
            }
        };
        let kind = rejection.kind();
        drop(rejection);
        self.observed
            .send(kind)
            .map_err(|_| RocketMQError::illegal_argument("post-take expiry observer closed"))?;
        Err(RocketMQError::illegal_argument("injected post-take expiry rejection"))
    }
}

#[tokio::test]
async fn pop_lite_deferred_expiry_attach_after_take_cancels_without_handler_or_frame() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    service.fail_next_expiry_attachment_after_take();
    let client_id = CheetahString::from_static_str("post-take-expiry");
    let event = CheetahString::from_static_str("%LMQ%$parent-topic$post-take-expiry");
    assert_eq!(
        dispatcher.do_full_dispatch(
            &client_id,
            &CheetahString::from_static_str("group-a"),
            &HashSet::from([event.clone()]),
        ),
        1
    );
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (mut client, running) = start_server(
        ExpiryAttachmentFaultProcessor {
            service: Arc::clone(&service),
            observed: observed_tx,
        },
        controller,
    )
    .await;

    client
        .send_command(request_command_for(client_id.as_str(), 406, 60_000))
        .await
        .expect("send post-take expiry attachment request");
    assert_eq!(
        observed_rx.recv().await.expect("post-take expiry result"),
        PopLiteDeferredRegisterRejectionKind::Expiry
    );
    let terminal = service.resource_snapshot();
    assert_eq!(terminal.admission.waiting_count(), 0);
    assert_eq!(terminal.admission.retained_bytes(), 0);
    assert_eq!(terminal.index.live, 0);
    assert_eq!(terminal.index.reserved, 0);
    assert_eq!(terminal.index.candidates, 0);
    assert_eq!(terminal.index.clients, 0);
    assert_eq!(terminal.prepared_registrations, 0);
    assert_eq!(terminal.pending_claims, 0);
    assert_eq!(terminal.resume_execution_count, 0);
    assert_eq!(terminal.resume_execution_bytes, 0);
    assert_eq!(terminal.event_reservations.events, 0);
    assert_eq!(terminal.active_client_gates, 0);
    assert_eq!(dispatcher.pending_events(&client_id), vec![event]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 1);

    assert_eq!(dispatcher.take_pending_events(&client_id).len(), 1);
    assert_eq!(dispatcher.budget_snapshot().current_count, 0);
    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "post-take expiry attachment failure emits no fallback frame"
    );
}
