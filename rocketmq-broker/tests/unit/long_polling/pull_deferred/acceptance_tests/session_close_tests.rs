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

use rocketmq_transport::api::DeferredClaimOutcome;
use rocketmq_transport::api::DeferredResumeOutcome;

use super::*;

#[cfg(windows)]
#[tokio::test]
async fn tcp_session_close_drops_prepared_owner_once_without_retrying() {
    const ORIGINAL_OPAQUE: i32 = 9_833;
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = PullDeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        barrier: Arc::clone(&barrier),
        hold_before_outcome: false,
        rollback_registration: false,
    };
    let (address, running) = start_running_server(processor, Arc::clone(&controller)).await;
    let raw_client = TcpStream::connect(address).await.expect("connect closing Pull client");
    configure_abortive_close(&raw_client).expect("configure deterministic session reset");
    let mut framed = Framed::new(raw_client, RemotingCommandCodec::new());
    framed
        .send(request_command(ORIGINAL_OPAQUE))
        .await
        .expect("send session-close Pull request");
    framed
        .send(
            RemotingCommand::create_remoting_command(SENTINEL_CODE)
                .set_opaque(ORIGINAL_OPAQUE + 1)
                .mark_oneway_rpc(),
        )
        .await
        .expect("send session-close commit sentinel");
    barrier.commit_observed.notified().await;
    let registered = registrations.recv().await.expect("observe session-close registration");

    let topic = CheetahString::from_static_str("TopicA");
    let mut cursor = PullScanCursor::new();
    let mut candidates = service.reserve_arrival_batch(&PullArrivalView::new(&topic, 0, 8), &mut cursor);
    let DeferredClaimOutcome::Claimed(claim) = service
        .claim_candidate(
            candidates.pop().expect("one session-close Pull candidate"),
            DeferredWakeReason::MessageArrived,
        )
        .await
        .expect("claim session-close Pull")
    else {
        panic!("session-close Pull candidate must retain its claimed request");
    };
    assert!(candidates.is_empty());

    let owner_drops = Arc::new(AtomicUsize::new(0));
    let response_owner_drops = Arc::clone(&owner_drops);
    let rereads = Arc::new(AtomicUsize::new(0));
    let reread_count = Arc::clone(&rereads);
    let (plan_ready_tx, plan_ready_rx) = oneshot::channel();
    let (release_plan_tx, release_plan_rx) = oneshot::channel();
    let (receipt_tx, receipt_rx) = oneshot::channel();
    let service_for_resume = Arc::clone(&service);
    running
        .actions
        .spawn_service("pull-deferred-session-close-write", async move {
            let result = service_for_resume
                .resume_claimed(
                    claim,
                    rocketmq_transport::api::DeferredResumeRetainedSize::default(),
                    move |resume, reason| async move {
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        assert_eq!(resume.request().effective_peer(), registered.peer);
                        reread_count.fetch_add(1, Ordering::SeqCst);
                        let body = Bytes::from_owner(CountingBodyOwner::new(
                            b"owner-held-across-session-close".to_vec(),
                            response_owner_drops,
                        ));
                        let _ = plan_ready_tx.send(());
                        release_plan_rx
                            .await
                            .map_err(|_| RocketMQError::illegal_argument("session-close plan release closed"))?;
                        RemotingResponse::bytes(
                            RemotingCommand::create_response_command_with_code(ResponseCode::Success),
                            body,
                        )
                        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn owned session-close Pull resume");

    plan_ready_rx.await.expect("session-close Pull remoting response ready");
    assert_eq!(owner_drops.load(Ordering::SeqCst), 0);
    drop(framed);
    tokio::time::timeout(Duration::from_secs(2), async {
        while owner_drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("session close reaches the accepted resume owner");
    assert_eq!(
        owner_drops.load(Ordering::SeqCst),
        1,
        "session close cancels the accepted resume and releases its prepared owner"
    );
    assert!(
        release_plan_tx.send(()).is_err(),
        "session close must cancel the in-flight response builder"
    );
    let outcome = receipt_rx
        .await
        .expect("session-close Pull receipt channel")
        .expect("closed session is a normal deferred resume outcome");
    assert!(matches!(outcome, DeferredResumeOutcome::SessionClosed));
    assert_eq!(rereads.load(Ordering::SeqCst), 1, "closed sessions are never retried");
    assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
    assert_released(&service);
    let _ = service.shutdown();
    running.finish().await;
}
