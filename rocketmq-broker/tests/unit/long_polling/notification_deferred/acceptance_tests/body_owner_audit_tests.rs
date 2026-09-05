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

use std::fs::File;
use std::io::Write;
use std::sync::atomic::AtomicUsize;

use bytes::Bytes;
use rocketmq_transport::api::ClaimedDeferred;
use rocketmq_transport::api::DeferredResumeOutcome;
use rocketmq_transport::api::FileRegion;
use rocketmq_transport::api::FileRegionLease;
use rocketmq_transport::api::FileRegionSequence;

use super::super::service::ResumeNotification;
use super::*;

const OWNER_BODY: &[u8] = b"notification-owner-body";
const ONE_OVER_FRAME_BODY_BYTES: usize = 16 * 1024 * 1024;

struct CountingBodyOwner {
    body: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl CountingBodyOwner {
    fn new(body: Vec<u8>, drops: Arc<AtomicUsize>) -> Self {
        Self { body, drops }
    }
}

impl AsRef<[u8]> for CountingBodyOwner {
    fn as_ref(&self) -> &[u8] {
        &self.body
    }
}

impl Drop for CountingBodyOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct CountingFileOwner {
    file: File,
    drops: Arc<AtomicUsize>,
}

impl FileRegionLease for CountingFileOwner {
    fn file(&self) -> &File {
        &self.file
    }
}

impl Drop for CountingFileOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn notification_head() -> RemotingCommand {
    application_remoting_command_factory().create_success_response_command_with_header(NotificationResponseHeader {
        has_msg: false,
        polling_full: false,
    })
}

async fn register_and_claim(
    client: &mut Connection,
    registrations: &mut mpsc::UnboundedReceiver<Registration>,
    service: &NotificationDeferredService,
) -> (Registration, ClaimedDeferred<ResumeNotification>) {
    client
        .send_command(request_command())
        .await
        .expect("send owner-backed Notification request");
    let registration = registrations
        .recv()
        .await
        .expect("observe owner-backed Notification registration");
    let topic = CheetahString::from_static_str("TopicA");
    let prepared = service.prepare_arrival_batch(NotificationArrivalView::new(&topic, 0), None);
    let batch = service.claim_prepared_arrival(prepared).await;
    let (mut claims, cursor) = batch.into_parts();
    assert!(cursor.is_complete());
    assert_eq!(claims.len(), 1);
    (registration, claims.pop().expect("one owner-backed Notification claim"))
}

fn assert_service_released(service: &NotificationDeferredService) {
    let snapshot = service.snapshot();
    assert_eq!(snapshot.admission().waiting_count(), 0);
    assert_eq!(snapshot.admission().retained_bytes(), 0);
    assert_eq!(snapshot.index().live(), 0);
    assert_eq!(snapshot.index().reserved(), 0);
    assert_eq!(snapshot.index().candidates(), 0);
    assert_eq!(snapshot.pending_claims(), 0);
    assert_eq!(snapshot.resume_executions(), 0);
    assert_eq!(snapshot.resume_execution_bytes(), 0);
}

#[tokio::test]
async fn notification_deferred_owner_backed_body_success_releases_once_without_retry() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        filter: None,
    };
    let (mut client, running) = start_server(processor, controller).await;
    let (registration, claim) = register_and_claim(&mut client, &mut registrations, &service).await;
    let owner_drops = Arc::new(AtomicUsize::new(0));
    let response_owner_drops = Arc::clone(&owner_drops);
    let attempts = Arc::new(AtomicUsize::new(0));
    let response_attempts = Arc::clone(&attempts);

    assert!(matches!(
        service
            .resume_claimed(
                claim,
                DeferredResumeRetainedSize::default(),
                move |resume, reason| async move {
                    assert_eq!(reason, DeferredWakeReason::MessageArrived);
                    assert_eq!(resume.request().effective_peer(), registration.peer);
                    response_attempts.fetch_add(1, Ordering::SeqCst);
                    let body = Bytes::from_owner(CountingBodyOwner::new(OWNER_BODY.to_vec(), response_owner_drops));
                    RemotingResponse::bytes(notification_head(), body)
                        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                },
            )
            .await
            .expect("write owner-backed Notification body"),
        DeferredResumeOutcome::Completed(_)
    ));

    let response = client
        .receive_command()
        .await
        .expect("owner-backed Notification connection remains open")
        .expect("owner-backed Notification response frame");
    assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(response.body().map(|body| body.as_ref()), Some(OWNER_BODY));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
    assert_service_released(&service);

    running.finish().await;
    assert!(client.receive_command().await.is_none());
}

#[tokio::test]
async fn notification_deferred_prewrite_failure_releases_owner_once_without_retry() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        filter: None,
    };
    let (mut client, running) = start_server(processor, controller).await;
    let (_, claim) = register_and_claim(&mut client, &mut registrations, &service).await;
    let owner_drops = Arc::new(AtomicUsize::new(0));
    let response_owner_drops = Arc::clone(&owner_drops);
    let attempts = Arc::new(AtomicUsize::new(0));
    let response_attempts = Arc::clone(&attempts);

    let error = service
        .resume_claimed(
            claim,
            DeferredResumeRetainedSize::default(),
            move |_resume, reason| async move {
                assert_eq!(reason, DeferredWakeReason::MessageArrived);
                response_attempts.fetch_add(1, Ordering::SeqCst);
                let body = Bytes::from_owner(CountingBodyOwner::new(
                    vec![b'x'; ONE_OVER_FRAME_BODY_BYTES],
                    response_owner_drops,
                ));
                RemotingResponse::bytes(notification_head(), body)
                    .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
            },
        )
        .await
        .expect_err("frame-limit failure rejects the owner-backed Notification body");
    let _ = error;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
    assert_service_released(&service);

    client.shutdown().await.expect("close prewrite-failure client");
    running.finish().await;
}

#[tokio::test]
async fn notification_deferred_parent_cancel_releases_prepared_owner_once_without_retry() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        filter: None,
    };
    let (mut client, running) = start_server(processor, controller).await;
    let (_, claim) = register_and_claim(&mut client, &mut registrations, &service).await;
    let owner_drops = Arc::new(AtomicUsize::new(0));
    let response_owner_drops = Arc::clone(&owner_drops);
    let attempts = Arc::new(AtomicUsize::new(0));
    let response_attempts = Arc::clone(&attempts);
    let (plan_ready_tx, plan_ready_rx) = oneshot::channel();
    let (release_plan_tx, release_plan_rx) = oneshot::channel();
    let (receipt_tx, receipt_rx) = oneshot::channel();
    let service_for_resume = Arc::clone(&service);
    running
        .action_context
        .spawn_service("notification-owner-parent-cancel", async move {
            let result = service_for_resume
                .resume_claimed(
                    claim,
                    DeferredResumeRetainedSize::default(),
                    move |_resume, reason| async move {
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        response_attempts.fetch_add(1, Ordering::SeqCst);
                        let body = Bytes::from_owner(CountingBodyOwner::new(OWNER_BODY.to_vec(), response_owner_drops));
                        let plan = RemotingResponse::bytes(notification_head(), body)
                            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
                        let _ = plan_ready_tx.send(());
                        release_plan_rx
                            .await
                            .map_err(|_| RocketMQError::illegal_argument("cancelled Notification plan release"))?;
                        Ok(plan)
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn owner-backed Notification cancellation");

    plan_ready_rx
        .await
        .expect("owner-backed Notification plan reaches the accepted handler");
    assert_eq!(owner_drops.load(Ordering::SeqCst), 0);
    running.cancel_server_parent();
    let outcome = receipt_rx
        .await
        .expect("cancelled Notification receipt channel")
        .expect("parent cancellation is a normal deferred resume outcome");
    assert!(matches!(outcome, DeferredResumeOutcome::Cancelled));
    assert!(release_plan_tx.send(()).is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
    assert_service_released(&service);

    running.finish().await;
}

#[tokio::test]
async fn notification_deferred_post_writer_claim_partial_releases_file_owner_once_without_retry() {
    const REGION_BYTES: &[u8] = b"leased-notification-region";

    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        filter: None,
    };
    let (mut client, running) = start_server(processor, controller).await;
    let (_, claim) = register_and_claim(&mut client, &mut registrations, &service).await;
    let owner_drops = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::new(AtomicUsize::new(0));
    let response_attempts = Arc::clone(&attempts);
    let mut file = tempfile::tempfile().expect("temporary Notification region");
    file.write_all(REGION_BYTES).expect("write Notification region");
    file.flush().expect("flush Notification region");
    let owner = Arc::new(CountingFileOwner {
        file,
        drops: Arc::clone(&owner_drops),
    });
    let lease: Arc<dyn FileRegionLease> = owner.clone();
    let region = FileRegion::try_new(lease, 0, REGION_BYTES.len() as u64).expect("validated Notification region");
    let regions = FileRegionSequence::try_new(vec![region]).expect("Notification region sequence");
    owner
        .file
        .set_len(0)
        .expect("inject deterministic body EOF after region validation");
    drop(owner);

    let error = service
        .resume_claimed(
            claim,
            DeferredResumeRetainedSize::default(),
            move |_resume, reason| async move {
                assert_eq!(reason, DeferredWakeReason::MessageArrived);
                response_attempts.fetch_add(1, Ordering::SeqCst);
                RemotingResponse::file_regions(notification_head(), regions)
                    .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
            },
        )
        .await
        .expect_err("truncated leased body fails after the canonical frame head");
    let _ = error;
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "partial writes are never retried");
    assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
    assert_service_released(&service);

    drop(client);
    running.finish().await;
}
