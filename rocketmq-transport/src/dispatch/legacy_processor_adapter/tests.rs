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
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::TaskGroup;
use tokio::sync::oneshot;

use super::*;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::connection::Connection;
use crate::deadline::RequestDeadline;
use crate::dispatch::RequestControlView;
use crate::dispatch::RequestMeta;
use crate::dispatch::ResponseBody;
use crate::security::TransportSecurity;
use crate::server::run_connected_session;
use crate::server::ConnectionHandler;

struct CaptureSession {
    sender: Mutex<Option<oneshot::Sender<SessionHandle>>>,
}

impl ConnectionHandler for CaptureSession {
    fn connected(&self, session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            if let Some(sender) = self.sender.lock().expect("capture session lock").take() {
                let _ = sender.send(session);
            }
        })
    }

    fn command(
        &self,
        _session: SessionHandle,
        _command: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
}

struct NetworkHarness {
    runtime: RuntimeOwner,
    first: SessionHandle,
    second: SessionHandle,
    first_peer: Connection,
    second_peer: Connection,
    first_runner: tokio::task::JoinHandle<()>,
    second_runner: tokio::task::JoinHandle<()>,
}

impl NetworkHarness {
    async fn new() -> Self {
        let runtime = RuntimeOwner::new(RuntimeConfig::default()).expect("legacy bridge runtime owner");
        let first_service = runtime.root_context().component("legacy-bridge-first");
        let second_service = runtime.root_context().component("legacy-bridge-second");
        let (first, first_peer, first_runner) = connected_session(first_service.task_group(), "first").await;
        let (second, second_peer, second_runner) = connected_session(second_service.task_group(), "second").await;
        Self {
            runtime,
            first,
            second,
            first_peer,
            second_peer,
            first_runner,
            second_runner,
        }
    }

    async fn shutdown(self) {
        drop((self.first, self.second, self.first_peer, self.second_peer));
        self.first_runner.await.expect("first session runner");
        self.second_runner.await.expect("second session runner");
        let report = self.runtime.shutdown_tasks().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    fn plan_response(&self, session: &SessionHandle) -> ResponseSink {
        let view = session.session_view();
        let control = RequestControlView::from_meta(
            &RequestMeta::new(Instant::now(), None),
            view.state().clone(),
            session.task_group(),
        );
        ResponseSink::network_plan(session.clone(), AdmissionClass::Data, control)
    }
}

async fn connected_session(
    task_group: &TaskGroup,
    name: &'static str,
) -> (SessionHandle, Connection, tokio::task::JoinHandle<()>) {
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let security = Arc::new(TransportSecurity::development_insecure_loopback(None, None));
    let (transport, peer) = tokio::io::duplex(64 * 1024);
    let (session_tx, session_rx) = oneshot::channel();
    let handler = Arc::new(CaptureSession {
        sender: Mutex::new(Some(session_tx)),
    });
    let session_task_group = task_group.clone();
    let (_, runner) = task_group
        .spawn_service_with_handle(
            format!("legacy-bridge-{name}-session"),
            run_connected_session(
                Connection::new_with_plaintext_stream(transport),
                "127.0.0.1:19241".parse().expect("local address"),
                "127.0.0.1:19242".parse().expect("remote address"),
                session_task_group,
                admission,
                security,
                None,
                Duration::from_secs(30),
                handler,
            ),
        )
        .expect("spawn tracked legacy bridge session");
    (
        session_rx.await.expect("capture connected session"),
        Connection::new_with_plaintext_stream(peer),
        runner,
    )
}

#[tokio::test]
async fn network_bridge_uses_one_real_session_and_the_adapter_stable_response_table() {
    let harness = NetworkHarness::new().await;
    let response = harness.plan_response(&harness.first);
    let stable_table = PendingRequestTable::new();
    let endpoint = LegacyNetworkSession::for_test(stable_table.clone());
    let bridge = LegacyRequestBridge::from_network_session(&harness.first, &response, &endpoint)
        .expect("canonical network bridge");

    assert_eq!(
        bridge.canonical_session_id,
        SessionId::from_session_owner(harness.first.session_id())
    );
    assert!(bridge.channel.shares_inner(bridge.context.legacy_channel()));
    assert!(bridge.channel.is_canonical_network_owner(&harness.first));

    let owner = bridge
        .channel
        .pending_request_owner()
        .expect("network bridge response owner")
        .clone();
    let foreign_table = PendingRequestTable::new();
    let (foreign_sender, _foreign_receiver) = oneshot::channel();
    assert!(foreign_table
        .register_for_owner(&owner, 71, RequestDeadline::from_timeout_millis(1_000), foreign_sender,)
        .is_err());

    let (sender, receiver) = oneshot::channel();
    let _guard = stable_table
        .register_for_owner(&owner, 71, RequestDeadline::from_timeout_millis(1_000), sender)
        .expect("bridge owner belongs to stable adapter table");
    assert!(stable_table.complete_response_for_owner(
        &owner,
        71,
        RemotingCommand::create_response_command_with_code(0).set_opaque(71),
    ));
    assert_eq!(
        receiver
            .await
            .expect("stable table completion")
            .expect("response result")
            .opaque(),
        71
    );

    drop((bridge, response));
    harness.shutdown().await;
}

#[tokio::test]
async fn network_bridge_request_apis_use_the_canonical_writer_and_stable_endpoint_table() {
    let mut harness = NetworkHarness::new().await;
    let response = harness.plan_response(&harness.first);
    let endpoint = LegacyNetworkSession::for_test(PendingRequestTable::new());
    let bridge = LegacyRequestBridge::from_network_session(&harness.first, &response, &endpoint)
        .expect("canonical network bridge");

    let send_channel = bridge.channel.clone();
    let completion_channel = bridge.channel.clone();
    let peer = &mut harness.first_peer;
    let (returned, ()) = tokio::join!(
        send_channel.send_wait_response(RemotingCommand::create_remoting_command(601).set_opaque(8_601), 1_000,),
        async move {
            let outbound = peer
                .receive_command()
                .await
                .expect("bridge peer remains open")
                .expect("bridge request frame decodes");
            assert_eq!(outbound.code(), 601);
            assert_eq!(outbound.opaque(), 8_601);
            assert!(completion_channel.complete_pending_response(
                RemotingCommand::create_response_command_with_code(0).set_opaque(outbound.opaque()),
            ));
        },
    );
    assert_eq!(returned.expect("stable endpoint completion").opaque(), 8_601);

    bridge
        .channel
        .send(RemotingCommand::create_remoting_command(602).set_opaque(8_602), None)
        .await
        .expect("bridge send uses canonical queued writer");
    let sent = harness
        .first_peer
        .receive_command()
        .await
        .expect("bridge peer remains open")
        .expect("bridge send frame decodes");
    assert_eq!((sent.code(), sent.opaque()), (602, 8_602));

    bridge
        .channel
        .send_command(RemotingCommand::create_remoting_command(0).set_opaque(8_604))
        .await
        .expect("request type wins over a response-code-shaped numeric code");
    let sent_command = harness
        .first_peer
        .receive_command()
        .await
        .expect("bridge peer remains open")
        .expect("bridge public send command frame decodes");
    assert_eq!((sent_command.code(), sent_command.opaque()), (0, 8_604));
    assert!(!sent_command.is_response_type());
    assert_eq!(bridge.channel.legacy_response_terminal_state(), None);

    let mut borrowed_request = RemotingCommand::create_remoting_command(0).set_opaque(8_605);
    bridge
        .channel
        .send_command_ref(&mut borrowed_request)
        .await
        .expect("borrowed request type bypasses the legacy response slot");
    let sent_command_ref = harness
        .first_peer
        .receive_command()
        .await
        .expect("bridge peer remains open")
        .expect("bridge public borrowed command frame decodes");
    assert_eq!((sent_command_ref.code(), sent_command_ref.opaque()), (0, 8_605));
    assert!(!sent_command_ref.is_response_type());
    assert_eq!(bridge.channel.legacy_response_terminal_state(), None);

    bridge
        .channel
        .send_oneway(RemotingCommand::create_remoting_command(603).set_opaque(8_603), 1_000)
        .await
        .expect("bridge one-way uses canonical queued writer");
    let sent_oneway = harness
        .first_peer
        .receive_command()
        .await
        .expect("bridge peer remains open")
        .expect("bridge one-way frame decodes");
    assert_eq!((sent_oneway.code(), sent_oneway.opaque()), (603, 8_603));
    assert!(sent_oneway.is_oneway_rpc());
    assert_eq!(bridge.channel.legacy_response_terminal_state(), None);

    drop((bridge, response));
    harness.shutdown().await;
}

#[tokio::test]
async fn dropping_borrowed_bridge_keeps_session_owner_live_until_canonical_close() {
    let mut harness = NetworkHarness::new().await;
    let response = harness.plan_response(&harness.first);
    let endpoint = LegacyNetworkSession::for_test(PendingRequestTable::new());
    let first_bridge = LegacyRequestBridge::from_network_session(&harness.first, &response, &endpoint)
        .expect("first borrowed network bridge");
    drop(first_bridge);

    let second_bridge = LegacyRequestBridge::from_network_session(&harness.first, &response, &endpoint)
        .expect("later bridge keeps the same session owner");
    let send_channel = second_bridge.channel.clone();
    let endpoint_for_completion = endpoint.clone();
    let peer = &mut harness.first_peer;
    let (returned, ()) = tokio::join!(
        send_channel.send_wait_response(RemotingCommand::create_remoting_command(604).set_opaque(8_604), 1_000),
        async move {
            let outbound = peer
                .receive_command()
                .await
                .expect("peer stays open after first bridge drop")
                .expect("later bridge request is written");
            assert!(endpoint_for_completion.response_table().complete_response_for_owner(
                endpoint_for_completion.owner(),
                outbound.opaque(),
                RemotingCommand::create_response_command_with_code(0).set_opaque(outbound.opaque()),
            ));
        },
    );
    assert_eq!(returned.expect("later request correlates").opaque(), 8_604);

    let closing_channel = second_bridge.channel.clone();
    let endpoint_for_close = endpoint.clone();
    let peer = &mut harness.first_peer;
    let (closed, ()) = tokio::join!(
        closing_channel.send_wait_response(RemotingCommand::create_remoting_command(605).set_opaque(8_605), 5_000),
        async move {
            let outbound = peer
                .receive_command()
                .await
                .expect("peer remains open until canonical close")
                .expect("pending request is written before close");
            assert_eq!(outbound.opaque(), 8_605);
            assert_eq!(
                endpoint_for_close
                    .response_table()
                    .close_owner(endpoint_for_close.owner(), || {
                        rocketmq_error::RocketMQError::network_connection_failed(
                            "test_session_close",
                            "canonical owner closed",
                        )
                    }),
                1
            );
        },
    );
    assert!(closed.is_err(), "only canonical owner close releases the waiter");

    drop((second_bridge, response));
    harness.shutdown().await;
}

#[tokio::test]
async fn network_bridge_fails_closed_for_cross_session_same_id_writer_and_transport_mismatches() {
    let harness = NetworkHarness::new().await;
    assert_ne!(harness.first.session_id(), harness.second.session_id());
    let first_response = harness.plan_response(&harness.first);
    let second_response = harness.plan_response(&harness.second);
    let table = PendingRequestTable::new();
    let endpoint = LegacyNetworkSession::for_test(table.clone());
    let bridge = LegacyRequestBridge::from_network_session(&harness.first, &first_response, &endpoint)
        .expect("first canonical bridge");

    assert!(matches!(
        bridge.validate_network(&harness.second, &second_response),
        Err(LegacyProcessorAdapterError::SessionMismatch)
    ));

    let second_id = SessionId::from_session_owner(harness.second.session_id());
    let mut same_id_different_writer = harness
        .first
        .legacy_processor_channel(first_response.clone(), table, endpoint.owner().clone())
        .expect("first writer channel");
    same_id_different_writer.set_canonical_session_id_for_test(second_id);
    let forged = LegacyRequestBridge {
        context: Arc::new(ConnectionHandlerContextWrapper::new(same_id_different_writer.clone())),
        channel: same_id_different_writer,
        canonical_session_id: second_id,
    };
    assert!(matches!(
        forged.validate_network(&harness.second, &second_response),
        Err(LegacyProcessorAdapterError::WriterOwnerMismatch)
    ));

    let (local, _receiver) = ResponseSink::local();
    let local_endpoint = LegacyNetworkSession::for_test(PendingRequestTable::new());
    assert!(matches!(
        LegacyRequestBridge::from_network_session(&harness.first, &local, &local_endpoint),
        Err(LegacyProcessorAdapterError::TransportKindMismatch)
    ));

    drop((bridge, forged, first_response, second_response, local));
    harness.shutdown().await;
}

struct EmbeddedHarness {
    runtime: RuntimeOwner,
    task_group: TaskGroup,
    record: EmbeddedSessionRecord,
    response_table: PendingRequestTable,
}

impl EmbeddedHarness {
    fn new(name: &'static str, session_id: u64) -> Self {
        let runtime = RuntimeOwner::new(RuntimeConfig::default()).expect("embedded bridge runtime owner");
        let task_group = runtime.root_context().component(name).task_group().clone();
        Self {
            runtime,
            task_group,
            record: EmbeddedSessionRecord::new(session_id),
            response_table: PendingRequestTable::new(),
        }
    }

    fn control(&self) -> RequestControlView {
        let view = self.record.view();
        RequestControlView::from_meta(
            &RequestMeta::new(Instant::now(), None),
            view.state().clone(),
            &self.task_group,
        )
    }

    async fn shutdown(self) {
        drop(self.record);
        let report = self.runtime.shutdown_tasks().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}

#[tokio::test]
async fn embedded_bridge_rejects_cross_completion_owner_and_network_transport() {
    let harness = EmbeddedHarness::new("legacy-embedded-owner", 9_794);
    let (first, _first_receiver) = ResponseSink::local_plan(harness.control());
    let (second, _second_receiver) = ResponseSink::local_plan(harness.control());
    let bridge = LegacyRequestBridge::from_embedded_session(
        &harness.record,
        &first,
        &harness.task_group,
        harness.response_table.clone(),
    )
    .expect("canonical embedded bridge");

    let owner = bridge
        .channel
        .pending_request_owner()
        .expect("embedded bridge response owner")
        .clone();
    let foreign_table = PendingRequestTable::new();
    let (foreign_sender, _foreign_receiver) = oneshot::channel();
    assert!(foreign_table
        .register_for_owner(&owner, 72, RequestDeadline::from_timeout_millis(1_000), foreign_sender,)
        .is_err());
    let (sender, receiver) = oneshot::channel();
    let _guard = harness
        .response_table
        .register_for_owner(&owner, 72, RequestDeadline::from_timeout_millis(1_000), sender)
        .expect("embedded bridge owner belongs to stable endpoint table");
    assert!(harness.response_table.complete_response_for_owner(
        &owner,
        72,
        RemotingCommand::create_response_command_with_code(0).set_opaque(72),
    ));
    assert_eq!(
        receiver
            .await
            .expect("embedded stable table completion")
            .expect("embedded response result")
            .opaque(),
        72
    );

    assert!(matches!(
        bridge.validate_embedded(&harness.record, &second, &harness.task_group),
        Err(LegacyProcessorAdapterError::CompletionOwnerMismatch)
    ));

    let (legacy_local, _legacy_receiver) = ResponseSink::local();
    assert!(matches!(
        LegacyRequestBridge::from_embedded_session(
            &harness.record,
            &legacy_local,
            &harness.task_group,
            harness.response_table.clone(),
        ),
        Err(LegacyProcessorAdapterError::CompletionOwnerMismatch)
    ));

    let network_harness = NetworkHarness::new().await;
    let network = network_harness.plan_response(&network_harness.first);
    assert!(matches!(
        LegacyRequestBridge::from_embedded_session(
            &harness.record,
            &network,
            &harness.task_group,
            harness.response_table.clone(),
        ),
        Err(LegacyProcessorAdapterError::TransportKindMismatch)
    ));

    drop((bridge, first, second, legacy_local, network));
    network_harness.shutdown().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn embedded_constructor_rejects_foreign_session_control_from_an_equal_numbered_runtime_group() {
    let first = EmbeddedHarness::new("legacy-embedded-session-first", 9_796);
    let second = EmbeddedHarness::new("legacy-embedded-session-second", 9_797);
    assert_eq!(first.task_group.id(), second.task_group.id());

    let (foreign_response, _foreign_receiver) = ResponseSink::local_plan(second.control());
    assert!(matches!(
        LegacyRequestBridge::from_embedded_session(
            &first.record,
            &foreign_response,
            &second.task_group,
            first.response_table.clone(),
        ),
        Err(LegacyProcessorAdapterError::CompletionOwnerMismatch)
    ));

    drop(foreign_response);
    second.shutdown().await;
    first.shutdown().await;
}

#[tokio::test]
async fn embedded_constructor_rejects_foreign_task_owner_despite_the_same_group_id() {
    let first = EmbeddedHarness::new("legacy-embedded-task-first", 9_798);
    let second = EmbeddedHarness::new("legacy-embedded-task-second", 9_799);
    assert_eq!(first.task_group.id(), second.task_group.id());

    let (response, _receiver) = ResponseSink::local_plan(first.control());
    assert!(matches!(
        LegacyRequestBridge::from_embedded_session(
            &first.record,
            &response,
            &second.task_group,
            first.response_table.clone(),
        ),
        Err(LegacyProcessorAdapterError::CompletionOwnerMismatch)
    ));

    drop(response);
    second.shutdown().await;
    first.shutdown().await;
}

enum EmbeddedDirectPath {
    Channel,
    ChannelRef,
    Context,
}

async fn embedded_direct_write_then_none(path: EmbeddedDirectPath) {
    let harness = EmbeddedHarness::new("legacy-embedded-direct-none", 9_795);
    let (response, receiver) = ResponseSink::local_plan(harness.control());
    let bridge = LegacyRequestBridge::from_embedded_session(
        &harness.record,
        &response,
        &harness.task_group,
        harness.response_table.clone(),
    )
    .expect("canonical embedded bridge");
    let command = RemotingCommand::create_response_command_with_code(0)
        .set_opaque(811)
        .set_body(Bytes::from_static(b"embedded direct response"));

    let processor_result: Option<RemotingCommand> = match path {
        EmbeddedDirectPath::Channel => {
            bridge
                .channel
                .send_command(command)
                .await
                .expect("public channel direct write uses plan completion owner");
            None
        }
        EmbeddedDirectPath::ChannelRef => {
            let mut command = command;
            bridge
                .channel
                .send_command_ref(&mut command)
                .await
                .expect("public borrowed channel direct write uses plan completion owner");
            None
        }
        EmbeddedDirectPath::Context => {
            bridge
                .context
                .try_write_response(command)
                .await
                .expect("context direct write uses plan completion owner");
            None
        }
    };
    assert!(processor_result.is_none(), "legacy None remains ambiguous");

    let plan = receiver.receive().await.expect("one direct plan handoff");
    assert_eq!(plan.response_code(), 0);
    let ResponseBody::Bytes(body) = plan.test_body() else {
        panic!("direct response body must remain owned bytes");
    };
    assert_eq!(body.as_ref(), b"embedded direct response");
    let duplicate = bridge
        .context
        .try_write_response(RemotingCommand::create_response_command_with_code(1).set_opaque(811))
        .await;
    assert!(matches!(
        duplicate,
        Err(ResponseError::AlreadyCompleted {
            state: crate::dispatch::ResponseTerminalState::Completed
        })
    ));

    drop((bridge, response));
    harness.shutdown().await;
}

#[tokio::test]
async fn embedded_channel_direct_write_then_none_hands_off_once_without_a_central_send() {
    embedded_direct_write_then_none(EmbeddedDirectPath::Channel).await;
}

#[tokio::test]
async fn embedded_borrowed_channel_direct_write_then_none_hands_off_once_without_a_central_send() {
    embedded_direct_write_then_none(EmbeddedDirectPath::ChannelRef).await;
}

#[tokio::test]
async fn embedded_context_direct_write_then_none_hands_off_once_without_a_central_send() {
    embedded_direct_write_then_none(EmbeddedDirectPath::Context).await;
}
