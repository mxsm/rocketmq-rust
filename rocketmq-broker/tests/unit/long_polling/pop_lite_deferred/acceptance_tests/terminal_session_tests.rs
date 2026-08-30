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

#[tokio::test]
async fn pop_lite_deferred_caller_drop_keeps_gate_until_canonical_terminal_and_replays_next_event() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    for _ in 0..2 {
        client
            .send_command(request_command())
            .await
            .expect("send same-client PopLite waiter");
        registrations.recv().await.expect("observe same-client registration");
    }

    let client_id = CheetahString::from_static_str("client-a");
    let first = CheetahString::from_static_str("%LMQ%$parent-topic$child-a");
    let second = CheetahString::from_static_str("%LMQ%$parent-topic$child-b");
    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([first.clone()]),
    );
    let claim = service
        .claim_event(&client_id)
        .await
        .expect("claim first PopLite event")
        .expect("first same-client waiter");
    let (handler_started_tx, handler_started_rx) = oneshot::channel();
    let (release_handler_tx, release_handler_rx) = oneshot::channel();
    let service_for_resume = Arc::clone(&service);
    let caller = tokio::spawn(async move {
        service_for_resume
            .resume_event_claim(
                claim,
                DeferredResumeRetainedSize::default(),
                move |_resume, reason, reservation| async move {
                    assert_eq!(reason, DeferredWakeReason::MessageArrived);
                    let _ = handler_started_tx.send(());
                    release_handler_rx
                        .await
                        .map_err(|_| RocketMQError::illegal_argument("terminal handler release closed"))?;
                    let batch = reservation.commit();
                    batch.complete(&HashSet::new());
                    RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                        ResponseCode::Success,
                    ))
                    .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                },
            )
            .await
    });
    handler_started_rx.await.expect("first PopLite handler accepted");

    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([second.clone()]),
    );
    assert!(service.observe_pending_event(&client_id));
    caller.abort();
    assert!(caller.await.expect_err("caller future is cancelled").is_cancelled());
    assert_eq!(service.resource_snapshot().active_client_gates, 1);
    assert_eq!(service.resource_snapshot().accepted_resumes, 1);
    assert!(
        service
            .claim_event(&client_id)
            .await
            .expect("active-gate observation is not an error")
            .is_none(),
        "the second same-client waiter cannot claim while the first canonical job is active"
    );

    release_handler_tx
        .send(())
        .expect("release network-owned PopLite handler");
    let response = client
        .receive_command()
        .await
        .expect("same-client connection remains open")
        .expect("first canonical response frame");
    assert_eq!(response.code(), ResponseCode::Success as i32);
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.resource_snapshot().active_client_gates != 0 || service.resource_snapshot().accepted_resumes != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("canonical write terminal releases the first gate");
    assert_eq!(
        service.take_pending_replays(nonzero(1)),
        vec![client_id.clone()],
        "the event observed during the first wake is replayed after its canonical terminal"
    );
    let second_claim = service
        .claim_event(&client_id)
        .await
        .expect("claim replayed PopLite event")
        .expect("second same-client waiter wakes after the first terminal");
    drop(second_claim);
    assert_eq!(dispatcher.pending_events(&client_id), vec![second]);
    assert_eq!(dispatcher.take_pending_events(&client_id).len(), 1);

    client.shutdown().await.expect("close same-client PopLite session");
    running.finish().await;
}
#[tokio::test]
async fn pop_lite_deferred_requeue_stays_affine_until_canonical_writer_terminal() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    for opaque in [ORIGINAL_OPAQUE, ORIGINAL_OPAQUE + 1] {
        client
            .send_command(request_command_for("client-a", opaque, 60_000))
            .await
            .expect("send terminal-owned PopLite waiter");
        registrations
            .recv()
            .await
            .expect("observe terminal-owned PopLite registration");
    }

    let client_id = CheetahString::from_static_str("client-a");
    let event = CheetahString::from_static_str("%LMQ%$parent-topic$terminal-child");
    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([event.clone()]),
    );
    let claim = service
        .claim_event(&client_id)
        .await
        .expect("claim terminal-owned PopLite event")
        .expect("first terminal-owned waiter is claimable");
    let handler_ready = Arc::new(Barrier::new(2));
    let release_writer = Arc::new(Barrier::new(2));
    service.set_terminal_ready_hook(Arc::clone(&handler_ready), Arc::clone(&release_writer));
    let service_for_resume = Arc::clone(&service);
    let requeued_event = event.clone();
    let (receipt_tx, receipt_rx) = oneshot::channel();
    running
        .action_context
        .spawn_service("pop-lite-terminal-owned-requeue", async move {
            let result = service_for_resume
                .resume_event_claim(
                    claim,
                    DeferredResumeRetainedSize::default(),
                    move |_resume, reason, reservation| async move {
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        reservation.commit().complete(&HashSet::from([requeued_event]));
                        RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                            ResponseCode::Success,
                        ))
                        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn terminal-owned PopLite resume");

    handler_ready.wait();
    let pre_writer = service.resource_snapshot();
    assert_eq!(pre_writer.event_reservations.events, 1);
    assert_eq!(pre_writer.event_reservations.permits, 1);
    assert!(pre_writer.event_reservations.retained_bytes > 0);
    assert_eq!(pre_writer.active_client_gates, 1);
    assert!(pre_writer.resume_execution_bytes > 0);
    assert!(
        dispatcher.reserve_pending_events(&client_id).is_none(),
        "the legacy seam cannot steal a requeue before writer terminal"
    );
    assert!(
        service
            .claim_event(&client_id)
            .await
            .expect("active terminal gate observation is not an error")
            .is_none(),
        "a second same-client wake cannot consume the staged requeue"
    );
    release_writer.wait();

    receipt_rx
        .await
        .expect("terminal-owned receipt channel")
        .expect("terminal-owned canonical response");
    let response = client
        .receive_command()
        .await
        .expect("terminal-owned connection remains open")
        .expect("terminal-owned canonical frame");
    assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(dispatcher.pending_events(&client_id), vec![event.clone()]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 1);
    let terminal = service.resource_snapshot();
    assert_eq!(terminal.event_reservations.events, 0);
    assert_eq!(terminal.event_reservations.retained_bytes, 0);
    assert_eq!(terminal.active_client_gates, 0);
    assert_eq!(terminal.accepted_resumes, 0);
    assert_eq!(terminal.resume_execution_bytes, 0);
    assert_eq!(service.take_pending_replays(nonzero(1)), vec![client_id.clone()]);

    let second = service
        .claim_event(&client_id)
        .await
        .expect("claim terminal requeue exactly once")
        .expect("second waiter resumes after terminal");
    drop(second);
    assert_eq!(dispatcher.pending_events(&client_id), vec![event.clone()]);
    assert_eq!(dispatcher.take_pending_events(&client_id), vec![event]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 0);

    client.shutdown().await.expect("close terminal-owned PopLite client");
    running.finish().await;
}

#[tokio::test]
async fn pop_lite_deferred_staged_requeue_rolls_back_once_when_session_closes_before_write() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command_for("staged-cancel", ORIGINAL_OPAQUE, 60_000))
        .await
        .expect("send staged-cancel PopLite waiter");
    registrations
        .recv()
        .await
        .expect("observe staged-cancel PopLite registration");

    let client_id = CheetahString::from_static_str("staged-cancel");
    let event = CheetahString::from_static_str("%LMQ%$parent-topic$staged-cancel-child");
    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([event.clone()]),
    );
    let claim = service
        .claim_event(&client_id)
        .await
        .expect("claim staged-cancel PopLite event")
        .expect("staged-cancel waiter is claimable");
    let service_for_resume = Arc::clone(&service);
    let requeued_event = event.clone();
    let (staged_tx, staged_rx) = oneshot::channel();
    let (release_handler_tx, release_handler_rx) = oneshot::channel();
    let (receipt_tx, receipt_rx) = oneshot::channel();
    running
        .action_context
        .spawn_service("pop-lite-staged-cancel", async move {
            let result = service_for_resume
                .resume_event_claim(
                    claim,
                    DeferredResumeRetainedSize::default(),
                    move |_resume, reason, reservation| async move {
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        reservation.commit().complete(&HashSet::from([requeued_event]));
                        let _ = staged_tx.send(());
                        release_handler_rx
                            .await
                            .map_err(|_| RocketMQError::illegal_argument("staged-cancel handler release closed"))?;
                        RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                            ResponseCode::Success,
                        ))
                        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn staged-cancel PopLite resume");

    staged_rx.await.expect("staged completion reached before cancellation");
    let staged = service.resource_snapshot();
    assert_eq!(staged.event_reservations.events, 1);
    assert!(staged.event_reservations.retained_bytes > 0);
    assert_eq!(staged.active_client_gates, 1);
    assert_eq!(staged.accepted_resumes, 1);
    assert!(staged.resume_execution_bytes > 0);
    assert!(dispatcher.pending_events(&client_id).is_empty());

    client
        .shutdown()
        .await
        .expect("close staged-cancel PopLite session before writer handoff");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = service.resource_snapshot();
            if snapshot.event_reservations.events == 0
                && snapshot.active_client_gates == 0
                && snapshot.accepted_resumes == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("session close cancels staged PopLite ownership");
    assert!(
        release_handler_tx.send(()).is_err(),
        "session close drops the staged handler before it can produce a plan"
    );
    let error = receipt_rx
        .await
        .expect("staged-cancel receipt channel")
        .expect_err("closed session rejects staged PopLite response");
    assert_eq!(error.kind(), DeferredResumeErrorKind::SessionClosed);
    let terminal = service.resource_snapshot();
    assert_eq!(terminal.event_reservations.events, 0);
    assert_eq!(terminal.event_reservations.retained_bytes, 0);
    assert_eq!(terminal.active_client_gates, 0);
    assert_eq!(terminal.accepted_resumes, 0);
    assert_eq!(terminal.resume_execution_bytes, 0);
    assert_eq!(dispatcher.pending_events(&client_id), vec![event.clone()]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 1);
    assert_eq!(service.take_pending_replays(nonzero(1)), vec![client_id.clone()]);
    assert_eq!(dispatcher.take_pending_events(&client_id), vec![event]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 0);
    running.finish().await;
}

#[tokio::test]
async fn pop_lite_deferred_parent_shutdown_settles_staged_requeue_without_a_frame() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command_for("parent-cancel", ORIGINAL_OPAQUE, 60_000))
        .await
        .expect("send parent-cancel PopLite waiter");
    registrations
        .recv()
        .await
        .expect("observe parent-cancel PopLite registration");

    let client_id = CheetahString::from_static_str("parent-cancel");
    let first = CheetahString::from_static_str("%LMQ%$parent-topic$parent-a");
    let second = CheetahString::from_static_str("%LMQ%$parent-topic$parent-b");
    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([first.clone(), second.clone()]),
    );
    let claim = service
        .claim_event(&client_id)
        .await
        .expect("claim parent-cancel PopLite event")
        .expect("parent-cancel waiter is claimable");
    let handler_ready = Arc::new(Barrier::new(2));
    let release_writer = Arc::new(Barrier::new(2));
    service.set_terminal_ready_hook(Arc::clone(&handler_ready), Arc::clone(&release_writer));
    let service_for_resume = Arc::clone(&service);
    let first_for_requeue = first.clone();
    let (receipt_tx, receipt_rx) = oneshot::channel();
    running
        .action_context
        .spawn_service("pop-lite-parent-cancel-staged", async move {
            let result = service_for_resume
                .resume_event_claim(
                    claim,
                    DeferredResumeRetainedSize::new(73),
                    move |_resume, _reason, reservation| async move {
                        reservation.commit().complete(&HashSet::from([first_for_requeue]));
                        RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                            ResponseCode::Success,
                        ))
                        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn parent-cancel staged PopLite resume");

    handler_ready.wait();
    let staged = service.resource_snapshot();
    assert_eq!(staged.event_reservations.events, 2);
    assert_eq!(staged.active_client_gates, 1);
    assert_eq!(staged.resume_execution_count, 1);
    assert!(staged.resume_execution_bytes > 73);
    let _ = service.shutdown();
    release_writer.wait();

    let error = receipt_rx
        .await
        .expect("parent-cancel receipt channel")
        .expect_err("parent shutdown rejects the staged response");
    assert_eq!(error.kind(), DeferredResumeErrorKind::Cancelled);
    assert_eq!(
        error.prior_terminal_reason(),
        Some(DeferredTerminalReason::ParentCancelled)
    );
    assert_eq!(error.write_progress(), None);
    let terminal = service.resource_snapshot();
    assert_eq!(terminal.event_reservations.events, 0);
    assert_eq!(terminal.active_client_gates, 0);
    assert_eq!(terminal.resume_execution_count, 0);
    assert_eq!(terminal.resume_execution_bytes, 0);
    assert_eq!(dispatcher.pending_events(&client_id), vec![first.clone()]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 1);
    assert_eq!(service.take_pending_replays(nonzero(1)), Vec::<CheetahString>::new());
    assert_eq!(dispatcher.take_pending_events(&client_id), vec![first]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 0);

    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "parent cancellation emits no PopLite response frame"
    );
}

#[tokio::test]
async fn pop_lite_deferred_session_close_rolls_back_claimed_events_and_gate() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command())
        .await
        .expect("send session-close PopLite request");
    registrations.recv().await.expect("observe session-close registration");

    let client_id = CheetahString::from_static_str("client-a");
    let first = CheetahString::from_static_str("%LMQ%$parent-topic$child-a");
    let second = CheetahString::from_static_str("%LMQ%$parent-topic$child-b");
    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([first.clone(), second.clone()]),
    );
    let claim = service
        .claim_event(&client_id)
        .await
        .expect("claim session-close PopLite event")
        .expect("registered client has a claim");

    let (handler_started_tx, handler_started_rx) = oneshot::channel();
    let (release_handler_tx, release_handler_rx) = oneshot::channel();
    let (receipt_tx, receipt_rx) = oneshot::channel();
    let body_drops = Arc::new(AtomicUsize::new(0));
    let body_drops_for_handler = Arc::clone(&body_drops);
    let service_for_resume = Arc::clone(&service);
    running
        .action_context
        .spawn_service("pop-lite-deferred-session-close-resume", async move {
            let result = service_for_resume
                .resume_event_claim(
                    claim,
                    DeferredResumeRetainedSize::default(),
                    move |_resume, reason, reservation| async move {
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        let body = Bytes::from_owner(CountingBodyOwner {
                            body: b"owner-backed-pop-lite-cancel".to_vec(),
                            drops: body_drops_for_handler,
                        });
                        let _ = handler_started_tx.send(());
                        release_handler_rx
                            .await
                            .map_err(|_| RocketMQError::illegal_argument("session-close handler release closed"))?;
                        let batch = reservation.commit();
                        batch.complete(&HashSet::new());
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
        .expect("spawn session-close PopLite resume");
    handler_started_rx.await.expect("PopLite resume handler started");
    let accepted = service.resource_snapshot();
    assert_eq!(accepted.admission.waiting_count(), 0);
    assert_eq!(accepted.index.live, 0);
    assert_eq!(accepted.event_reservations.events, 2);
    assert_eq!(accepted.event_reservations.permits, 2);
    assert_eq!(accepted.active_client_gates, 1);
    assert_eq!(accepted.accepted_resumes, 1);

    client.shutdown().await.expect("close PopLite client session");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = service.resource_snapshot();
            if snapshot.event_reservations.events == 0
                && snapshot.active_client_gates == 0
                && snapshot.accepted_resumes == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("session close cancels accepted PopLite resume ownership");
    assert!(
        release_handler_tx.send(()).is_err(),
        "session close cancels the handler before event commit"
    );
    let error = receipt_rx
        .await
        .expect("session-close PopLite receipt channel")
        .expect_err("closed session rejects the accepted PopLite response");
    assert_eq!(error.kind(), DeferredResumeErrorKind::SessionClosed);
    assert_eq!(body_drops.load(Ordering::SeqCst), 1);
    assert_eq!(dispatcher.pending_events(&client_id), vec![first, second]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 2);
    let terminal = service.resource_snapshot();
    assert_eq!(terminal.admission.waiting_count(), 0);
    assert_eq!(terminal.index.live, 0);
    assert_eq!(terminal.active_client_gates, 0);

    assert_eq!(dispatcher.take_pending_events(&client_id).len(), 2);
    running.finish().await;
}
