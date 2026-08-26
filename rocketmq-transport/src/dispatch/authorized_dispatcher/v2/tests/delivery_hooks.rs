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

use super::harness::*;
#[tokio::test]
async fn admitted_reply_uses_body_free_hooks_resolve_bind_writer_and_one_observation_in_order() {
    let mut harness = DispatchHarness::new("dispatch-v2-reply").await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let hook_events = Arc::new(Mutex::new(Vec::new()));
    let recording_hook = hook(false, false, Arc::clone(&hook_events));
    let before_seen = Arc::clone(&recording_hook.before_body_seen);
    let after_request_seen = Arc::clone(&recording_hook.after_request_body_seen);
    let after_response_seen = Arc::clone(&recording_hook.after_response_body_seen);
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        processor,
        vec![Arc::new(recording_hook)],
    ));
    let command = request(false);
    let body_pointer = command.body().expect("request body").as_ptr() as usize;
    let (session, original) = harness.request_session(&command);

    let outcome = dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("dispatch submission");
    assert!(matches!(outcome, DispatchOutcome::Accepted(_)));
    let response = harness.receive().await;
    wait_for_observation_count(&state, 1).await;

    assert_eq!(response.opaque(), 811, "binding must restore immutable ingress opaque");
    assert!(response.is_response_type());
    assert_eq!(response.code(), 71);
    assert_eq!(response.body().map(Bytes::as_ref), Some(&b"V2 response body"[..]));
    assert_eq!(
        response
            .ext_fields()
            .and_then(|fields| fields.get("hook-after"))
            .map(cheetah_string::CheetahString::as_str),
        Some("applied")
    );
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(
        *state.request_body_pointer.lock().expect("body pointer lock"),
        Some(body_pointer)
    );
    assert_eq!(before_seen.load(Ordering::SeqCst), 0);
    assert_eq!(after_request_seen.load(Ordering::SeqCst), 0);
    assert_eq!(after_response_seen.load(Ordering::SeqCst), 0);
    assert_eq!(
        hook_events.lock().expect("hook event lock").as_slice(),
        ["before", "after"]
    );
    assert_eq!(
        state.events.lock().expect("event lock").as_slice(),
        ["ordering", "reject", "process", "observe"]
    );
    let observation = {
        let observations = state.observations.lock().expect("observation lock");
        assert_eq!(observations.len(), 1);
        observations[0]
    };
    assert_eq!(observation.request_id(), original.request_id());
    assert_eq!(observation.original_code(), 91);
    assert_eq!(observation.response_code(), 71);
    assert_eq!(observation.body_kind(), ResponseBodyKind::Bytes);
    assert_eq!(observation.path(), ResponseWritePath::Inline);
    assert!(matches!(
        observation.outcome(),
        ResponseWriteOutcomeV2::Written(receipt)
            if receipt.request_id() == original.request_id()
                && receipt.disposition() == ResponseDisposition::TransportWritten
    ));
    harness.shutdown().await;
}

#[tokio::test]
async fn structured_rejection_clones_once_bypasses_hooks_and_processes_and_observes_one_write() {
    let mut harness = DispatchHarness::new("dispatch-v2-structured-reject").await;
    let (processor, state) = TestProcessor::new(Behavior::Reject);
    let hook_events = Arc::new(Mutex::new(Vec::new()));
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        processor,
        vec![Arc::new(hook(false, false, Arc::clone(&hook_events)))],
    ));
    let command = request(false);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("dispatch rejection");
    let response = harness.receive().await;
    wait_for_observation_count(&state, 1).await;

    assert_eq!(response.code(), 73);
    assert_eq!(response.opaque(), 811);
    assert_eq!(response.body().map(Bytes::as_ref), Some(&b"structured rejection"[..]));
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(state.rejects.load(Ordering::SeqCst), 1);
    assert_eq!(state.processes.load(Ordering::SeqCst), 0);
    assert_eq!(state.observations.lock().expect("observation lock").len(), 1);
    assert!(hook_events.lock().expect("hook event lock").is_empty());
    harness.shutdown().await;
}

#[tokio::test]
async fn mapped_processor_and_after_hook_errors_do_not_retry_after_or_write_twice() {
    for (name, fail_after) in [
        ("dispatch-v2-processor-error", false),
        ("dispatch-v2-after-error", true),
    ] {
        let mut harness = DispatchHarness::new(name).await;
        let behavior = if fail_after { Behavior::Reply } else { Behavior::Error };
        let (processor, state) = TestProcessor::new(behavior);
        let hook_events = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
            processor,
            vec![Arc::new(hook(fail_after, false, Arc::clone(&hook_events)))],
        ));
        let command = request(false);
        let (session, _) = harness.request_session(&command);

        dispatcher
            .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
            .await
            .expect("dispatch mapped error");
        let response = harness.receive().await;
        wait_for_observation_count(&state, 1).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);
        assert_eq!(response.opaque(), 811);
        assert_eq!(
            hook_events.lock().expect("hook event lock").as_slice(),
            ["before", "after"]
        );
        assert_eq!(state.clones.load(Ordering::SeqCst), 1);
        assert_eq!(state.processes.load(Ordering::SeqCst), 1);
        assert_eq!(state.observations.lock().expect("observation lock").len(), 1);
        harness.assert_no_response().await;
        harness.shutdown().await;
    }
}

#[tokio::test]
async fn after_hook_oneway_mutation_fails_closed_and_maps_once_without_recursive_after() {
    let mut harness = DispatchHarness::new("dispatch-v2-after-oneway-contract").await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let hook_events = Arc::new(Mutex::new(Vec::new()));
    let mut recording_hook = hook(false, false, Arc::clone(&hook_events));
    recording_hook.mark_after_oneway = true;
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        processor,
        vec![Arc::new(recording_hook)],
    ));
    let command = request(false);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("dispatch response-head contract error");
    let response = harness.receive().await;
    wait_for_observation_count(&state, 1).await;

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
    assert_eq!(response.opaque(), 811);
    assert!(!response.is_oneway_rpc());
    assert_eq!(
        hook_events.lock().expect("hook event lock").as_slice(),
        ["before", "after"]
    );
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(state.observations.lock().expect("observation lock").len(), 1);
    harness.assert_no_response().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn binding_restores_response_type_after_hook_clears_it_without_losing_plan_body() {
    let mut harness = DispatchHarness::new("dispatch-v2-after-response-type-binding").await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let hook_events = Arc::new(Mutex::new(Vec::new()));
    let mut recording_hook = hook(false, false, Arc::clone(&hook_events));
    recording_hook.clear_after_response_type = true;
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        processor,
        vec![Arc::new(recording_hook)],
    ));
    let command = request(false);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("dispatch response-type binding correction");
    let response = harness.receive().await;
    wait_for_observation_count(&state, 1).await;

    assert!(response.is_response_type());
    assert_eq!(response.opaque(), 811);
    assert_eq!(response.code(), 71);
    assert_eq!(response.body().map(Bytes::as_ref), Some(&b"V2 response body"[..]));
    assert_eq!(
        hook_events.lock().expect("hook event lock").as_slice(),
        ["before", "after"]
    );
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(state.observations.lock().expect("observation lock").len(), 1);
    harness.assert_no_response().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn possibly_partial_flush_failure_is_observed_once_and_never_retried() {
    let harness = DispatchHarness::new_with_flush_failure("dispatch-v2-possibly-partial").await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(processor, Vec::new()));
    let command = request(false);
    let (session, _) = harness.request_session(&command);
    let session_view = session.session_view();

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("dispatch flush failure request");
    wait_for_observation_count(&state, 1).await;
    tokio::time::timeout(Duration::from_secs(2), session_view.state().closed())
        .await
        .expect("writer failure must close the canonical session");

    {
        let observations = state.observations.lock().expect("observation lock");
        assert_eq!(observations.len(), 1);
        assert_eq!(
            observations[0].outcome(),
            ResponseWriteOutcomeV2::Failed {
                kind: ResponseErrorKind::Transport,
                progress: Some(WriteProgress::PossiblyPartial),
            }
        );
    }
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(state.observations.lock().expect("observation lock").len(), 1);
    assert!(session_view.state().is_closed());
    assert!(!session_view.state().is_healthy());
    harness.shutdown().await;
}

#[tokio::test]
async fn hook_body_attachment_fails_closed_before_processing_and_never_exposes_either_body() {
    let mut harness = DispatchHarness::new("dispatch-v2-hook-body-contract").await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let hook_events = Arc::new(Mutex::new(Vec::new()));
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        processor,
        vec![Arc::new(hook(false, true, Arc::clone(&hook_events)))],
    ));
    let command = request(false);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("dispatch hook contract error");
    let response = harness.receive().await;
    wait_for_observation_count(&state, 1).await;

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
    assert_eq!(response.opaque(), 811);
    assert_eq!(state.processes.load(Ordering::SeqCst), 0);
    assert_eq!(state.observations.lock().expect("observation lock").len(), 1);
    assert_eq!(hook_events.lock().expect("hook event lock").as_slice(), ["before"]);
    harness.shutdown().await;
}

#[tokio::test]
async fn after_hook_body_attachment_drops_original_plan_and_maps_one_bodyless_frame() {
    let mut harness = DispatchHarness::new("dispatch-v2-after-body-contract").await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let hook_events = Arc::new(Mutex::new(Vec::new()));
    let mut recording_hook = hook(false, false, Arc::clone(&hook_events));
    recording_hook.attach_after_body = true;
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        processor,
        vec![Arc::new(recording_hook)],
    ));
    let command = request(false);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("dispatch response-body contract error");
    let response = harness.receive().await;
    wait_for_observation_count(&state, 1).await;

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
    assert_eq!(response.opaque(), 811);
    assert!(
        response.body().is_none(),
        "mapped invariant response must stay bodyless"
    );
    assert_eq!(
        hook_events.lock().expect("hook event lock").as_slice(),
        ["before", "after"]
    );
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(state.observations.lock().expect("observation lock").len(), 1);
    harness.assert_no_response().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn one_hook_snapshot_is_retained_across_before_process_and_after() {
    let mut harness = DispatchHarness::new("dispatch-v2-hook-snapshot").await;
    let (processor, state) = TestProcessor::new(Behavior::WaitReply);
    let first_events = Arc::new(Mutex::new(Vec::new()));
    let second_events = Arc::new(Mutex::new(Vec::new()));
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        processor,
        vec![Arc::new(hook(false, false, Arc::clone(&first_events)))],
    ));
    let command = request(false);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("dispatch retained hook snapshot");
    state.entered.notified().await;
    dispatcher.register_rpc_hook(Arc::new(hook(false, false, Arc::clone(&second_events))));
    state.resume.notify_one();
    let _response = harness.receive().await;
    wait_for_observation_count(&state, 1).await;

    assert_eq!(
        first_events.lock().expect("first hook events").as_slice(),
        ["before", "after"]
    );
    assert!(second_events.lock().expect("second hook events").is_empty());
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(state.observations.lock().expect("observation lock").len(), 1);
    harness.shutdown().await;
}
