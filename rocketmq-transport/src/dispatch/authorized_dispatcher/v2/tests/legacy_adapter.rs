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
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use super::harness::*;
use crate::base::pending_request_table::PendingRequestTable;
use crate::dispatch::legacy_processor_adapter::bridge_construction_counts;
use crate::dispatch::DispatchOutcome;
use crate::dispatch::LegacyProcessorAdapter;
use crate::net::channel::Channel;
use crate::request_ordering::RequestOrdering;
use crate::request_ordering::RequestOrderingKey;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::processor::ResponseWriteObservation;
use crate::runtime::processor::ResponseWriteOutcome;
use crate::runtime::RPCHook;
use crate::session_view::SessionId;
use crate::telemetry::TransportTelemetry;

const PRIMARY_CODE: i32 = 310;
const SENTINEL_CODE: i32 = 311;
const ORIGINAL_OPAQUE: i32 = 811;
const MUTATED_CODE: i32 = 1_310;
const MUTATED_OPAQUE: i32 = 1_811;
const REJECTION_CODE: i32 = 73;
const DIRECT_CODE: i32 = 76;
const PROCESSOR_NAME: &str = "dsp05-legacy-test";
const ORDERING_KEY: RequestOrderingKey = RequestOrderingKey::new(9_794);

type LegacyProcessInput = (i32, i32, bool, Option<usize>);

#[derive(Clone, Copy)]
enum LegacyBehavior {
    Reply,
    WaitReply,
    NoneThenSentinel,
    DirectChannelThenNone,
    DirectChannelRefThenNone,
    DirectContextThenNone,
    ProcessorError,
    MalformedThenSentinel,
    RejectFalseSome,
    RejectTrueSome,
    RejectTrueMalformed,
    RejectTrueNone,
}

#[derive(Default)]
struct LegacyState {
    clones: AtomicUsize,
    rejects: AtomicUsize,
    processes: AtomicUsize,
    before_hooks: AtomicUsize,
    after_hooks: AtomicUsize,
    events: Mutex<Vec<&'static str>>,
    ordering_inputs: Mutex<Vec<(i32, i32)>>,
    process_inputs: Mutex<Vec<LegacyProcessInput>>,
    observations: Mutex<Vec<ResponseWriteObservation>>,
    returned_body_pointer: Mutex<Option<usize>>,
    direct_channel: Mutex<Option<Channel>>,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

struct LegacyProcessor {
    behavior: LegacyBehavior,
    state: Arc<LegacyState>,
}

impl Clone for LegacyProcessor {
    fn clone(&self) -> Self {
        self.state.clones.fetch_add(1, Ordering::SeqCst);
        self.state.events.lock().expect("legacy event lock").push("clone");
        Self {
            behavior: self.behavior,
            state: Arc::clone(&self.state),
        }
    }
}

impl LegacyProcessor {
    fn reply(&self) -> RemotingCommand {
        let body = Bytes::from(vec![19_u8; 257]);
        *self
            .state
            .returned_body_pointer
            .lock()
            .expect("legacy returned body pointer lock") = Some(body.as_ptr() as usize);
        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_opaque(-991)
            .set_body(body)
    }
}

impl RequestProcessor for LegacyProcessor {
    async fn process_request(
        &mut self,
        channel: Channel,
        context: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.state.processes.fetch_add(1, Ordering::SeqCst);
        self.state.events.lock().expect("legacy event lock").push("process");
        self.state
            .process_inputs
            .lock()
            .expect("legacy process input lock")
            .push((
                request.code(),
                request.opaque(),
                request.is_oneway_rpc(),
                request.body().map(|body| body.as_ptr() as usize),
            ));

        if request.code() == SENTINEL_CODE {
            return Ok(Some(self.reply()));
        }

        match self.behavior {
            LegacyBehavior::Reply
            | LegacyBehavior::RejectFalseSome
            | LegacyBehavior::RejectTrueSome
            | LegacyBehavior::RejectTrueMalformed
            | LegacyBehavior::RejectTrueNone => Ok(Some(self.reply())),
            LegacyBehavior::WaitReply => {
                self.state.entered.notify_one();
                self.state.resume.notified().await;
                Ok(Some(self.reply()))
            }
            LegacyBehavior::NoneThenSentinel => Ok(None),
            LegacyBehavior::DirectChannelThenNone => {
                *self.state.direct_channel.lock().expect("legacy direct channel lock") = Some(channel.clone());
                channel
                    .send_command(direct_response(request.opaque()))
                    .await
                    .map_err(|error| {
                        RocketMQError::response_process_failed("dsp05-direct-channel", error.to_string())
                    })?;
                Ok(None)
            }
            LegacyBehavior::DirectChannelRefThenNone => {
                *self.state.direct_channel.lock().expect("legacy direct channel lock") = Some(channel.clone());
                let mut response = direct_response(request.opaque());
                channel.send_command_ref(&mut response).await.map_err(|error| {
                    RocketMQError::response_process_failed("dsp05-direct-channel-ref", error.to_string())
                })?;
                Ok(None)
            }
            LegacyBehavior::DirectContextThenNone => {
                context
                    .try_write_response(direct_response(request.opaque()))
                    .await
                    .map_err(|error| {
                        RocketMQError::response_process_failed("dsp05-direct-context", error.to_string())
                    })?;
                Ok(None)
            }
            LegacyBehavior::ProcessorError => Err(RocketMQError::illegal_argument("legacy processor failure")),
            LegacyBehavior::MalformedThenSentinel => {
                Ok(Some(RemotingCommand::create_remoting_command(991).set_opaque(-991)))
            }
        }
    }

    fn reject_request(&self, code: i32) -> (bool, Option<RemotingCommand>) {
        self.state.rejects.fetch_add(1, Ordering::SeqCst);
        self.state.events.lock().expect("legacy event lock").push("reject");
        match self.behavior {
            LegacyBehavior::RejectFalseSome if code == PRIMARY_CODE => (
                false,
                Some(RemotingCommand::create_response_command_with_code(REJECTION_CODE)),
            ),
            LegacyBehavior::RejectTrueSome if code == PRIMARY_CODE => (
                true,
                Some(
                    RemotingCommand::create_response_command_with_code(REJECTION_CODE)
                        .set_opaque(-701)
                        .set_body(Bytes::from_static(b"custom rejection")),
                ),
            ),
            LegacyBehavior::RejectTrueMalformed if code == PRIMARY_CODE => (
                true,
                Some(RemotingCommand::create_remoting_command(REJECTION_CODE).set_opaque(-702)),
            ),
            LegacyBehavior::RejectTrueNone if code == PRIMARY_CODE => (true, None),
            _ => (false, None),
        }
    }

    fn request_ordering(&self, request: &RemotingCommand) -> RequestOrdering {
        self.state.events.lock().expect("legacy event lock").push("ordering");
        self.state
            .ordering_inputs
            .lock()
            .expect("legacy ordering input lock")
            .push((request.code(), request.opaque()));
        RequestOrdering::Ordered(ORDERING_KEY)
    }

    fn observe_response_write(&self, observation: ResponseWriteObservation) {
        self.state.events.lock().expect("legacy event lock").push("observe");
        self.state
            .observations
            .lock()
            .expect("legacy observation lock")
            .push(observation);
    }
}

struct LegacyHook {
    state: Arc<LegacyState>,
    label: &'static str,
    mutate_request: bool,
    fail_before: bool,
    fail_after: bool,
    mark_after_oneway: bool,
}

impl RPCHook for LegacyHook {
    fn do_before_request(
        &self,
        _remote_addr: std::net::SocketAddr,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.state.before_hooks.fetch_add(1, Ordering::SeqCst);
        self.state.events.lock().expect("legacy event lock").push(self.label);
        if self.fail_before {
            return Err(RocketMQError::illegal_argument("legacy before hook failure"));
        }
        if self.mutate_request {
            request.set_code_mut(MUTATED_CODE);
            request.set_opaque_mut(MUTATED_OPAQUE);
        }
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: std::net::SocketAddr,
        request: &RemotingCommand,
        response: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.state.after_hooks.fetch_add(1, Ordering::SeqCst);
        self.state.events.lock().expect("legacy event lock").push(self.label);
        response.set_opaque_mut(-883);
        if self.mark_after_oneway && request.code() == PRIMARY_CODE {
            response.mark_oneway_rpc_ref();
        }
        if self.fail_after {
            Err(RocketMQError::illegal_argument("legacy after hook failure"))
        } else {
            Ok(())
        }
    }
}

type LegacyDispatcher = AuthorizedCommandDispatcherV2<LegacyProcessorAdapter<LegacyProcessor>>;
type MetricCapture = Arc<parking_lot::Mutex<Vec<(&'static str, i32)>>>;

fn legacy_dispatcher(
    behavior: LegacyBehavior,
    hooks: Vec<Arc<dyn RPCHook>>,
) -> (Arc<LegacyDispatcher>, Arc<LegacyState>, MetricCapture) {
    let state = Arc::new(LegacyState::default());
    let processor = LegacyProcessor {
        behavior,
        state: Arc::clone(&state),
    };
    let (telemetry, metrics) = TransportTelemetry::with_legacy_processor_request_capture();
    let adapter = LegacyProcessorAdapter::new(processor, PROCESSOR_NAME, telemetry, PendingRequestTable::new());
    (
        Arc::new(AuthorizedCommandDispatcherV2::new_legacy(adapter, hooks)),
        state,
        metrics,
    )
}

fn hook(
    state: &Arc<LegacyState>,
    label: &'static str,
    mutate_request: bool,
    fail_before: bool,
    fail_after: bool,
) -> Arc<dyn RPCHook> {
    Arc::new(LegacyHook {
        state: Arc::clone(state),
        label,
        mutate_request,
        fail_before,
        fail_after,
        mark_after_oneway: false,
    })
}

fn malforming_after_hook(state: &Arc<LegacyState>) -> Arc<dyn RPCHook> {
    Arc::new(LegacyHook {
        state: Arc::clone(state),
        label: "malformed-after-hook",
        mutate_request: false,
        fail_before: false,
        fail_after: false,
        mark_after_oneway: true,
    })
}

fn command(code: i32, one_way: bool) -> RemotingCommand {
    let mut command = RemotingCommand::create_remoting_command(code)
        .set_opaque(ORIGINAL_OPAQUE)
        .set_body(Bytes::from(vec![7_u8; 313]));
    command.add_ext_field("dsp05", "ingress");
    if one_way {
        command.mark_oneway_rpc()
    } else {
        command
    }
}

fn direct_response(opaque: i32) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code(DIRECT_CODE)
        .set_opaque(opaque)
        .set_body(Bytes::from_static(b"legacy direct response"))
}

async fn dispatch(
    harness: &DispatchHarness,
    dispatcher: &Arc<LegacyDispatcher>,
    command: RemotingCommand,
    deadline: Option<RequestDeadline>,
) -> (DispatchOutcome, OriginalRequestIdentity) {
    let (session, original) = harness.request_session(&command);
    let outcome = dispatcher
        .dispatch(
            &harness.authorized,
            session,
            harness.context(deadline),
            command,
            512,
            None,
        )
        .await
        .expect("legacy adapter dispatch boundary");
    (outcome, original)
}

async fn finish_harness(harness: &mut DispatchHarness) {
    harness.drain_close_and_assert_eof().await;
}

#[tokio::test]
async fn admitted_reply_preserves_order_clone_hooks_body_and_immutable_binding() {
    let mut harness = DispatchHarness::new("dsp05-legacy-standard").await;
    let state = Arc::new(LegacyState::default());
    let hooks = vec![hook(&state, "hook", true, false, false)];
    let processor = LegacyProcessor {
        behavior: LegacyBehavior::Reply,
        state: Arc::clone(&state),
    };
    let (telemetry, metrics) = TransportTelemetry::with_legacy_processor_request_capture();
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new_legacy(
        LegacyProcessorAdapter::new(processor, PROCESSOR_NAME, telemetry, PendingRequestTable::new()),
        hooks,
    ));
    let request = command(PRIMARY_CODE, false);
    let request_body_pointer = request.body().expect("legacy request body").as_ptr() as usize;
    let (outcome, original) = dispatch(&harness, &dispatcher, request, None).await;
    assert!(matches!(outcome, DispatchOutcome::Accepted(_)));
    let response = harness.receive().await;
    harness.drain_requests().await;

    assert_eq!(response.code(), ResponseCode::Success.to_i32());
    assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
    assert!(response.is_response_type());
    assert_eq!(response.body().map(|body| body.len()), Some(257));
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(state.rejects.load(Ordering::SeqCst), 1);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(state.before_hooks.load(Ordering::SeqCst), 1);
    assert_eq!(state.after_hooks.load(Ordering::SeqCst), 1);
    assert_eq!(
        *state.ordering_inputs.lock().expect("legacy ordering input lock"),
        vec![(PRIMARY_CODE, ORIGINAL_OPAQUE)]
    );
    assert_eq!(
        *state.process_inputs.lock().expect("legacy process input lock"),
        vec![(MUTATED_CODE, MUTATED_OPAQUE, false, Some(request_body_pointer))]
    );
    assert_eq!(
        state.events.lock().expect("legacy event lock").as_slice(),
        ["ordering", "clone", "reject", "hook", "process", "hook", "observe"]
    );
    {
        let observations = state.observations.lock().expect("legacy observation lock");
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].request_code, PRIMARY_CODE);
        assert_eq!(observations[0].response_code, ResponseCode::Success.to_i32());
        assert_eq!(observations[0].outcome, ResponseWriteOutcome::Sent);
    }
    assert_eq!(metrics.lock().as_slice(), [(PROCESSOR_NAME, PRIMARY_CODE)]);
    assert_eq!(
        original.request_id().owner_id(),
        SessionId::from_session_owner(harness.session.session_id()).owner_id()
    );
    assert_eq!(
        bridge_construction_counts(SessionId::from_session_owner(harness.session.session_id())),
        (1, 1)
    );

    finish_harness(&mut harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn legacy_reply_direct_write_and_ambiguous_none_record_one_response_terminal_each() {
    for (name, behavior, expected_frame, expected_metric) in [
        (
            "dsp05-observe-returned-reply",
            LegacyBehavior::Reply,
            true,
            ("inline", "transport_written"),
        ),
        (
            "dsp05-observe-direct-write",
            LegacyBehavior::DirectChannelThenNone,
            true,
            ("inline", "transport_written"),
        ),
        (
            "dsp05-observe-ambiguous-none",
            LegacyBehavior::NoneThenSentinel,
            false,
            ("no_response", "protocol_no_response"),
        ),
    ] {
        let mut harness = DispatchHarness::new(name).await;
        let state = Arc::new(LegacyState::default());
        let processor = LegacyProcessor {
            behavior,
            state: Arc::clone(&state),
        };
        let (telemetry, response_metrics) = TransportTelemetry::with_v2_boundary_metric_capture();
        let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new_legacy(
            LegacyProcessorAdapter::new(processor, PROCESSOR_NAME, telemetry, PendingRequestTable::new()),
            Vec::new(),
        ));

        let (outcome, _) = dispatch(&harness, &dispatcher, command(PRIMARY_CODE, false), None).await;
        assert!(matches!(outcome, DispatchOutcome::Accepted(_)));
        harness.drain_requests().await;
        if expected_frame {
            let response = harness.receive().await;
            assert!(response.is_response_type());
        } else {
            harness.assert_no_response().await;
        }

        assert_eq!(response_metrics.response_events(), vec![expected_metric]);
        assert_eq!(response_metrics.snapshot().3, 1);
        assert_eq!(state.clones.load(Ordering::SeqCst), 1);

        finish_harness(&mut harness).await;
        harness.shutdown().await;
    }
}

#[tokio::test]
async fn rejection_tuple_is_exact_and_each_admitted_path_records_one_metric_and_observation() {
    for (name, behavior, expected_code, expected_processes) in [
        (
            "dsp05-reject-false-some",
            LegacyBehavior::RejectFalseSome,
            ResponseCode::Success.to_i32(),
            1,
        ),
        (
            "dsp05-reject-true-some",
            LegacyBehavior::RejectTrueSome,
            REJECTION_CODE,
            0,
        ),
        (
            "dsp05-reject-true-none",
            LegacyBehavior::RejectTrueNone,
            ResponseCode::SystemBusy.to_i32(),
            0,
        ),
    ] {
        let mut harness = DispatchHarness::new(name).await;
        let (dispatcher, state, metrics) = legacy_dispatcher(behavior, Vec::new());
        let (outcome, _) = dispatch(&harness, &dispatcher, command(PRIMARY_CODE, false), None).await;
        assert!(matches!(outcome, DispatchOutcome::Accepted(_)));
        let response = harness.receive().await;
        harness.drain_requests().await;

        assert_eq!(response.code(), expected_code);
        assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
        assert_eq!(state.clones.load(Ordering::SeqCst), 1);
        assert_eq!(state.rejects.load(Ordering::SeqCst), 1);
        assert_eq!(state.processes.load(Ordering::SeqCst), expected_processes);
        {
            let observations = state.observations.lock().expect("legacy observation lock");
            assert_eq!(observations.len(), 1);
            assert_eq!(observations[0].request_code, PRIMARY_CODE);
            assert_eq!(observations[0].response_code, expected_code);
        }
        assert_eq!(metrics.lock().as_slice(), [(PROCESSOR_NAME, PRIMARY_CODE)]);

        finish_harness(&mut harness).await;
        harness.shutdown().await;
    }
}

#[tokio::test]
async fn before_processor_and_after_failures_preserve_the_v1_observation_table() {
    for (
        name,
        behavior,
        fail_before,
        fail_after,
        expected_code,
        expected_processes,
        expected_after,
        expected_observations,
    ) in [
        (
            "dsp05-before-error",
            LegacyBehavior::Reply,
            true,
            false,
            ResponseCode::InvalidParameter.to_i32(),
            0,
            0,
            0,
        ),
        (
            "dsp05-processor-error",
            LegacyBehavior::ProcessorError,
            false,
            false,
            ResponseCode::SystemError.to_i32(),
            1,
            1,
            1,
        ),
        (
            "dsp05-after-error",
            LegacyBehavior::Reply,
            false,
            true,
            ResponseCode::InvalidParameter.to_i32(),
            1,
            1,
            0,
        ),
    ] {
        let mut harness = DispatchHarness::new(name).await;
        let state = Arc::new(LegacyState::default());
        let processor = LegacyProcessor {
            behavior,
            state: Arc::clone(&state),
        };
        let (telemetry, metrics) = TransportTelemetry::with_legacy_processor_request_capture();
        let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new_legacy(
            LegacyProcessorAdapter::new(processor, PROCESSOR_NAME, telemetry, PendingRequestTable::new()),
            vec![hook(&state, "hook", false, fail_before, fail_after)],
        ));
        let (outcome, _) = dispatch(&harness, &dispatcher, command(PRIMARY_CODE, false), None).await;
        assert!(matches!(outcome, DispatchOutcome::Accepted(_)));
        let response = harness.receive().await;
        harness.drain_requests().await;

        assert_eq!(response.code(), expected_code);
        assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
        assert_eq!(state.clones.load(Ordering::SeqCst), 1);
        assert_eq!(state.processes.load(Ordering::SeqCst), expected_processes);
        assert_eq!(state.before_hooks.load(Ordering::SeqCst), 1);
        assert_eq!(state.after_hooks.load(Ordering::SeqCst), expected_after);
        assert_eq!(
            state.observations.lock().expect("legacy observation lock").len(),
            expected_observations
        );
        assert_eq!(metrics.lock().as_slice(), [(PROCESSOR_NAME, PRIMARY_CODE)]);

        finish_harness(&mut harness).await;
        harness.shutdown().await;
    }
}

#[tokio::test]
async fn none_oneway_and_both_network_direct_write_paths_use_a_sentinel_and_observe_only_the_sentinel() {
    for (name, behavior, one_way, direct) in [
        ("dsp05-none-sentinel", LegacyBehavior::NoneThenSentinel, false, false),
        ("dsp05-oneway-sentinel", LegacyBehavior::Reply, true, false),
        (
            "dsp05-channel-direct-sentinel",
            LegacyBehavior::DirectChannelThenNone,
            false,
            true,
        ),
        (
            "dsp05-context-direct-sentinel",
            LegacyBehavior::DirectContextThenNone,
            false,
            true,
        ),
    ] {
        let mut harness = DispatchHarness::new(name).await;
        let (dispatcher, state, metrics) = legacy_dispatcher(behavior, Vec::new());
        dispatch(&harness, &dispatcher, command(PRIMARY_CODE, one_way), None).await;
        dispatch(&harness, &dispatcher, command(SENTINEL_CODE, false), None).await;

        let first = harness.receive().await;
        if direct {
            assert_eq!(first.code(), DIRECT_CODE);
            assert_eq!(first.opaque(), ORIGINAL_OPAQUE);
            let sentinel = harness.receive().await;
            assert_eq!(sentinel.code(), ResponseCode::Success.to_i32());
            assert_eq!(sentinel.opaque(), ORIGINAL_OPAQUE);
        } else {
            assert_eq!(first.code(), ResponseCode::Success.to_i32());
            assert_eq!(first.opaque(), ORIGINAL_OPAQUE);
        }
        harness.drain_requests().await;
        harness.assert_no_response().await;

        assert_eq!(state.clones.load(Ordering::SeqCst), 2);
        assert_eq!(state.processes.load(Ordering::SeqCst), 2);
        {
            let observations = state.observations.lock().expect("legacy observation lock");
            assert_eq!(observations.len(), 1);
            assert_eq!(observations[0].request_code, SENTINEL_CODE);
        }
        assert_eq!(
            metrics.lock().as_slice(),
            [(PROCESSOR_NAME, PRIMARY_CODE), (PROCESSOR_NAME, SENTINEL_CODE)]
        );

        finish_harness(&mut harness).await;
        harness.shutdown().await;
    }
}

#[tokio::test]
async fn one_hook_snapshot_excludes_hooks_registered_while_the_processor_is_running() {
    let mut harness = DispatchHarness::new("dsp05-hook-snapshot").await;
    let state = Arc::new(LegacyState::default());
    let processor = LegacyProcessor {
        behavior: LegacyBehavior::WaitReply,
        state: Arc::clone(&state),
    };
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new_legacy(
        LegacyProcessorAdapter::new(
            processor,
            PROCESSOR_NAME,
            TransportTelemetry::noop(),
            PendingRequestTable::new(),
        ),
        vec![hook(&state, "original-hook", false, false, false)],
    ));
    dispatch(&harness, &dispatcher, command(PRIMARY_CODE, false), None).await;
    state.entered.notified().await;
    dispatcher.register_rpc_hook(hook(&state, "late-hook", false, false, false));
    state.resume.notify_one();
    let response = harness.receive().await;
    harness.drain_requests().await;

    assert_eq!(response.code(), ResponseCode::Success.to_i32());
    {
        let events = state.events.lock().expect("legacy event lock");
        assert_eq!(events.iter().filter(|event| **event == "original-hook").count(), 2);
        assert!(!events.contains(&"late-hook"));
    }

    finish_harness(&mut harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn malformed_legacy_head_fails_before_encoding_and_ordered_sentinel_still_drains() {
    let mut harness = DispatchHarness::new("dsp05-malformed-response").await;
    let (dispatcher, state, metrics) = legacy_dispatcher(LegacyBehavior::MalformedThenSentinel, Vec::new());
    dispatch(&harness, &dispatcher, command(PRIMARY_CODE, false), None).await;
    dispatch(&harness, &dispatcher, command(SENTINEL_CODE, false), None).await;
    let sentinel = harness.receive().await;
    harness.drain_requests().await;
    harness.assert_no_response().await;

    assert_eq!(sentinel.code(), ResponseCode::Success.to_i32());
    assert_eq!(sentinel.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(state.observations.lock().expect("legacy observation lock").len(), 1);
    assert_eq!(dispatcher.reported_failure_categories(), vec!["processor_adapter"]);
    assert_eq!(
        metrics.lock().as_slice(),
        [(PROCESSOR_NAME, PRIMARY_CODE), (PROCESSOR_NAME, SENTINEL_CODE)]
    );

    finish_harness(&mut harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn original_oneway_discards_invalid_rejection_processor_and_after_hook_commands_before_plan_validation() {
    for (name, behavior, malformed_after_hook) in [
        (
            "dsp05-oneway-malformed-rejection",
            LegacyBehavior::RejectTrueMalformed,
            false,
        ),
        (
            "dsp05-oneway-malformed-processor",
            LegacyBehavior::MalformedThenSentinel,
            false,
        ),
        ("dsp05-oneway-malformed-after-hook", LegacyBehavior::Reply, true),
    ] {
        let mut harness = DispatchHarness::new(name).await;
        let state = Arc::new(LegacyState::default());
        let processor = LegacyProcessor {
            behavior,
            state: Arc::clone(&state),
        };
        let hooks = malformed_after_hook
            .then(|| malforming_after_hook(&state))
            .into_iter()
            .collect();
        let (telemetry, metrics) = TransportTelemetry::with_legacy_processor_request_capture();
        let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new_legacy(
            LegacyProcessorAdapter::new(processor, PROCESSOR_NAME, telemetry, PendingRequestTable::new()),
            hooks,
        ));

        dispatch(&harness, &dispatcher, command(PRIMARY_CODE, true), None).await;
        dispatch(&harness, &dispatcher, command(SENTINEL_CODE, false), None).await;
        let sentinel = harness.receive().await;
        harness.drain_requests().await;
        harness.assert_no_response().await;

        assert_eq!(sentinel.code(), ResponseCode::Success.to_i32());
        assert_eq!(sentinel.opaque(), ORIGINAL_OPAQUE);
        {
            let observations = state.observations.lock().expect("legacy observation lock");
            assert_eq!(observations.len(), 1);
            assert_eq!(observations[0].request_code, SENTINEL_CODE);
        }
        assert!(dispatcher.reported_failure_categories().is_empty());
        assert_eq!(
            metrics.lock().as_slice(),
            [(PROCESSOR_NAME, PRIMARY_CODE), (PROCESSOR_NAME, SENTINEL_CODE)]
        );

        finish_harness(&mut harness).await;
        harness.shutdown().await;
    }
}

async fn post_start_public_direct_write_deadline(behavior: LegacyBehavior) {
    let (mut harness, write_barrier) =
        DispatchHarness::new_with_post_start_barrier("dsp05-direct-post-start-deadline").await;
    let (dispatcher, state, metrics) = legacy_dispatcher(behavior, Vec::new());

    dispatch(
        &harness,
        &dispatcher,
        command(PRIMARY_CODE, false),
        Some(RequestDeadline::after(Duration::from_millis(40))),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), write_barrier.wait_reached())
        .await
        .expect("direct write must reach the post-start socket barrier");
    tokio::time::sleep(Duration::from_millis(80)).await;

    let direct_channel = state
        .direct_channel
        .lock()
        .expect("legacy direct channel lock")
        .take()
        .expect("processor captured its direct-write channel");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if direct_channel.legacy_response_terminal_state()
                == Some(crate::dispatch::ResponseTerminalState::Failed {
                    progress: crate::dispatch::WriteProgress::PossiblyPartial,
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("post-start cancellation must publish PossiblyPartial to the shared slot");

    write_barrier.release();
    let direct = harness.receive().await;
    assert_eq!((direct.code(), direct.opaque()), (DIRECT_CODE, ORIGINAL_OPAQUE));

    dispatch(&harness, &dispatcher, command(SENTINEL_CODE, false), None).await;
    let sentinel = harness.receive().await;
    harness.drain_requests().await;
    harness.assert_no_response().await;
    assert_eq!(sentinel.code(), ResponseCode::Success.to_i32());
    assert_eq!(sentinel.opaque(), ORIGINAL_OPAQUE);
    {
        let observations = state.observations.lock().expect("legacy observation lock");
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].request_code, SENTINEL_CODE);
    }
    assert!(dispatcher.reported_failure_categories().is_empty());
    assert_eq!(
        metrics.lock().as_slice(),
        [(PROCESSOR_NAME, PRIMARY_CODE), (PROCESSOR_NAME, SENTINEL_CODE)]
    );

    drop(direct_channel);
    finish_harness(&mut harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn public_send_command_post_start_deadline_keeps_one_possibly_partial_owner_and_never_centrally_retries() {
    post_start_public_direct_write_deadline(LegacyBehavior::DirectChannelThenNone).await;
}

#[tokio::test]
async fn public_send_command_ref_post_start_deadline_keeps_one_possibly_partial_owner_and_never_centrally_retries() {
    post_start_public_direct_write_deadline(LegacyBehavior::DirectChannelRefThenNone).await;
}

#[tokio::test]
async fn pre_admission_deadline_and_explicit_v2_dispatch_record_no_legacy_metric_or_bridge_construction() {
    let mut legacy = DispatchHarness::new("dsp05-pre-admission-zero").await;
    let legacy_session_id = SessionId::from_session_owner(legacy.session.session_id());
    let (dispatcher, state, metrics) = legacy_dispatcher(LegacyBehavior::Reply, Vec::new());
    let (outcome, _) = dispatch(
        &legacy,
        &dispatcher,
        command(PRIMARY_CODE, false),
        Some(RequestDeadline::after(Duration::ZERO)),
    )
    .await;
    assert_eq!(outcome, DispatchOutcome::Rejected);
    let deadline = legacy.receive().await;
    assert_eq!(deadline.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(state.clones.load(Ordering::SeqCst), 0);
    assert_eq!(state.processes.load(Ordering::SeqCst), 0);
    assert!(metrics.lock().is_empty());
    assert_eq!(bridge_construction_counts(legacy_session_id), (0, 0));
    finish_harness(&mut legacy).await;
    legacy.shutdown().await;

    let mut v2 = DispatchHarness::new("dsp05-v2-no-legacy-construction").await;
    let v2_session_id = SessionId::from_session_owner(v2.session.session_id());
    let (processor, _v2_state) = TestProcessor::new(Behavior::Reply);
    let v2_dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(processor, Vec::new()));
    let request = request(false);
    let (session, _) = v2.request_session(&request);
    v2_dispatcher
        .dispatch(&v2.authorized, session, v2.context(None), request, 256, None)
        .await
        .expect("explicit V2 dispatch");
    let _response = v2.receive().await;
    v2.drain_requests().await;
    assert_eq!(bridge_construction_counts(v2_session_id), (0, 0));
    assert!(metrics.lock().is_empty());
    finish_harness(&mut v2).await;
    v2.shutdown().await;
}
