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

use std::collections::BTreeSet;
use std::sync::atomic::AtomicUsize;

use super::*;

struct FixedFilter {
    matched: bool,
    commit_calls: Option<Arc<AtomicUsize>>,
}

impl MessageFilter for FixedFilter {
    fn is_matched_by_consume_queue(&self, _tags_code: Option<i64>, _cq_ext_unit: Option<&CqExtUnit>) -> bool {
        self.matched
    }

    fn is_matched_by_commit_log(
        &self,
        _msg_buffer: Option<&[u8]>,
        _properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) -> bool {
        if let Some(calls) = self.commit_calls.as_ref() {
            calls.fetch_add(1, Ordering::AcqRel);
        }
        false
    }
}

#[derive(Clone)]
struct PerRequestFilterProcessor {
    service: Arc<NotificationDeferredService>,
    registrations: mpsc::UnboundedSender<()>,
    commit_calls: Option<Arc<AtomicUsize>>,
}

impl RequestProcessor for PerRequestFilterProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let header = request
            .command()
            .decode_command_custom_header::<NotificationRequestHeader>()?;
        let matched = header.client_id.as_deref() != Some("8102");
        let filter: ArcMessageFilter = Arc::new(FixedFilter {
            matched,
            commit_calls: self.commit_calls.clone(),
        });
        let prepared = self
            .service
            .prepare(request, None, Some(filter), NotificationRetainedEstimate::default())
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let registration = self
            .service
            .register(prepared, request)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let _ = self.registrations.send(());
        Ok(HandlerOutcome::Deferred(registration))
    }

    fn request_ordering(&self, _ingress: rocketmq_transport::api::IngressRequestView<'_>) -> RequestOrdering {
        RequestOrdering::Concurrent
    }
}

fn success_plan(has_msg: bool) -> rocketmq_error::RocketMQResult<RemotingResponse> {
    let head = application_remoting_command_factory().create_success_response_command_with_header(
        NotificationResponseHeader {
            has_msg,
            polling_full: false,
        },
    );
    RemotingResponse::command(head).map_err(|error| RocketMQError::illegal_argument(error.to_string()))
}

#[tokio::test]
async fn notification_deferred_miss_prefix_beyond_callback_batch_continuation_claims_and_resumes_match() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service_with_scan(controller.as_ref(), 1);
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = PerRequestFilterProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        commit_calls: None,
    };
    let (mut client, running) = start_server(processor, controller).await;
    for opaque in [8_101, 8_102] {
        client
            .send_command(request_command_for("GroupA", opaque, 60_000))
            .await
            .expect("send filtered Notification waiter");
        registrations.recv().await.expect("observe filtered registration");
    }

    let topic = CheetahString::from_static_str("TopicA");
    let arrival = NotificationArrivalView::new(&topic, 0);
    let first = service.prepare_arrival_batch(arrival, None);
    assert_eq!(first.inspected(), 1);
    assert_eq!(first.candidate_count(), 0, "newest filter miss restores its entry");
    let first = service.claim_prepared_arrival(first).await;
    let (claims, cursor) = first.into_parts();
    assert!(claims.is_empty());
    assert!(!cursor.is_complete(), "older matching prefix remains for continuation");
    assert_eq!(service.snapshot().index().live(), 2);
    assert_eq!(service.snapshot().admission().waiting_count(), 2);

    let continuation = service
        .admit_continuation(arrival, cursor)
        .expect("admit miss-prefix continuation");
    let service_for_handler = Arc::clone(&service);
    let (resumed_tx, mut resumed_rx) = mpsc::unbounded_channel();
    let handle_claims = Arc::new(move |claims| {
        let service = Arc::clone(&service_for_handler);
        let resumed = resumed_tx.clone();
        async move {
            for claim in claims {
                let result = service
                    .resume_claimed(
                        claim,
                        DeferredResumeRetainedSize::default(),
                        |_resume, reason| async move {
                            assert_eq!(reason, DeferredWakeReason::MessageArrived);
                            success_plan(true)
                        },
                    )
                    .await;
                let _ = resumed.send(result);
            }
        }
    });
    service
        .spawn_continuation(running.action_context.task_group(), continuation, handle_claims)
        .expect("spawn miss-prefix continuation");
    assert!(matches!(
        resumed_rx
            .recv()
            .await
            .expect("continuation resumes matching waiter")
            .expect("matching continuation writes canonically"),
        rocketmq_transport::api::DeferredResumeOutcome::Completed(_)
    ));
    let response = client
        .receive_command()
        .await
        .expect("filtered connection")
        .expect("matching continuation frame");
    assert_eq!(
        response.opaque(),
        8_101,
        "older matching waiter, not newest miss, resumes"
    );
    assert_eq!(service.snapshot().index().live(), 1);
    assert_eq!(service.snapshot().admission().waiting_count(), 1);

    client.shutdown().await.expect("close filtered Notification session");
    running.finish().await;
}

#[tokio::test]
async fn notification_deferred_properties_absent_matches_and_one_arrival_claims_wildcard_then_exact() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let commit_calls = Arc::new(AtomicUsize::new(0));
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = PerRequestFilterProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        commit_calls: Some(Arc::clone(&commit_calls)),
    };
    let (mut client, running) = start_server(processor, controller).await;
    let mut wildcard = request_command_for("GroupA", 8_201, 60_000);
    wildcard.add_ext_field("queueId", "-1");
    for command in [wildcard, request_command_for("GroupA", 8_202, 60_000)] {
        client
            .send_command(command)
            .await
            .expect("send wildcard/exact Notification waiter");
        registrations.recv().await.expect("observe wildcard/exact registration");
    }

    let topic = CheetahString::from_static_str("TopicA");
    let prepared = service.prepare_arrival_batch(NotificationArrivalView::new(&topic, 0), None);
    assert_eq!(
        prepared.candidate_count(),
        2,
        "wildcard and exact keys both match one arrival"
    );
    assert_eq!(
        commit_calls.load(Ordering::Acquire),
        0,
        "absent properties skip commit-log filter"
    );
    let (claims, cursor) = service.claim_prepared_arrival(prepared).await.into_parts();
    assert!(cursor.is_complete());
    assert_eq!(claims.len(), 2);
    for claim in claims {
        assert!(matches!(
            service
                .resume_claimed(
                    claim,
                    DeferredResumeRetainedSize::default(),
                    |_resume, _reason| async move { success_plan(true) },
                )
                .await
                .expect("wildcard/exact canonical resume"),
            rocketmq_transport::api::DeferredResumeOutcome::Completed(_)
        ));
    }
    let mut opaque = BTreeSet::new();
    for _ in 0..2 {
        opaque.insert(
            client
                .receive_command()
                .await
                .expect("wildcard/exact connection")
                .expect("wildcard/exact response")
                .opaque(),
        );
    }
    assert_eq!(opaque, BTreeSet::from([8_201, 8_202]));
    assert_eq!(service.snapshot().index().live(), 0);
    assert_eq!(service.snapshot().admission().waiting_count(), 0);
    running.finish().await;
}
