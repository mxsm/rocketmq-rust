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

#![cfg(feature = "test-support")]

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::RpcClientError;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_runtime::RuntimeContext;
use rocketmq_security_api::OutboundSigner;
use rocketmq_security_api::Secret;
use rocketmq_security_api::SecurityRequestView;
use rocketmq_security_api::Signature;
use rocketmq_security_api::SigningError;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::DefaultRequestProcessor;
use rocketmq_transport::api::GoAwayPolicy;
use rocketmq_transport::api::RPCHook;
use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::api::TlsClientConfig;
use rocketmq_transport::api::TlsConfig;
use rocketmq_transport::api::TlsMode;
use rocketmq_transport::api::TlsServerRuntime;
use rocketmq_transport::api::TransportClient;
use rocketmq_transport::api::TransportClientConfig;
use rocketmq_transport::api::TransportSecurity;
use rocketmq_transport::test_support::ConnectionHandler;
use rocketmq_transport::test_support::SessionHandle;
use rocketmq_transport::test_support::TransportListener;

#[derive(Clone, Copy)]
struct Reply {
    code: i32,
    delay: Duration,
    body: &'static [u8],
    late_success: Option<(usize, &'static [u8])>,
}

impl Reply {
    const fn immediate(code: i32, body: &'static [u8]) -> Self {
        Self {
            code,
            delay: Duration::ZERO,
            body,
            late_success: None,
        }
    }

    const fn delayed(code: i32, delay: Duration, body: &'static [u8]) -> Self {
        Self {
            code,
            delay,
            body,
            late_success: None,
        }
    }

    const fn with_late_success_after_calls(mut self, expected_calls: usize, body: &'static [u8]) -> Self {
        self.late_success = Some((expected_calls, body));
        self
    }
}

struct ScriptedHandler {
    replies: Vec<Reply>,
    calls: AtomicUsize,
    completed: AtomicUsize,
    observed: Mutex<Vec<(u64, RemotingCommand)>>,
    changed: tokio::sync::Notify,
    completion_changed: tokio::sync::Notify,
    first_entered: Option<Arc<tokio::sync::Notify>>,
    first_release: Option<Arc<tokio::sync::Notify>>,
}

impl ScriptedHandler {
    fn new(replies: Vec<Reply>) -> Self {
        Self {
            replies,
            calls: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            observed: Mutex::new(Vec::new()),
            changed: tokio::sync::Notify::new(),
            completion_changed: tokio::sync::Notify::new(),
            first_entered: None,
            first_release: None,
        }
    }

    fn gated_first(
        replies: Vec<Reply>,
        first_entered: Arc<tokio::sync::Notify>,
        first_release: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            replies,
            calls: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            observed: Mutex::new(Vec::new()),
            changed: tokio::sync::Notify::new(),
            completion_changed: tokio::sync::Notify::new(),
            first_entered: Some(first_entered),
            first_release: Some(first_release),
        }
    }

    async fn wait_for_calls(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.calls.load(Ordering::SeqCst) >= expected {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("scripted server did not receive the expected requests");
    }

    async fn wait_for_completions(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let changed = self.completion_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.completed.load(Ordering::SeqCst) >= expected {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("scripted server did not complete the expected responses");
    }

    fn observations(&self) -> Vec<(u64, RemotingCommand)> {
        self.observed.lock().expect("observation lock").clone()
    }
}

impl ConnectionHandler for ScriptedHandler {
    fn connected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    fn command(
        &self,
        session: SessionHandle,
        command: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            self.observed
                .lock()
                .expect("observation lock")
                .push((session.session_id(), command.clone()));
            self.changed.notify_waiters();
            if call == 0 {
                if let Some(first_entered) = &self.first_entered {
                    first_entered.notify_one();
                }
                if let Some(first_release) = &self.first_release {
                    first_release.notified().await;
                }
            }
            let reply = self
                .replies
                .get(call)
                .copied()
                .unwrap_or_else(|| Reply::immediate(ResponseCode::SystemError.to_i32(), b"unexpected"));
            if !reply.delay.is_zero() {
                tokio::time::sleep(reply.delay).await;
            }
            let mut connection = session.connection();
            let _ = connection
                .send_command(
                    RemotingCommand::create_response_command_with_code(reply.code)
                        .set_opaque(command.opaque())
                        .set_body(reply.body),
                )
                .await;
            if let Some((expected_calls, late_body)) = reply.late_success {
                self.wait_for_calls(expected_calls).await;
                let _ = connection
                    .send_command(
                        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                            .set_opaque(command.opaque())
                            .set_body(late_body),
                    )
                    .await;
            }
            self.completed.fetch_add(1, Ordering::SeqCst);
            self.completion_changed.notify_waiters();
        })
    }
}

#[derive(Default)]
struct CountingHook {
    before: AtomicUsize,
    after: AtomicUsize,
}

struct RetryHeader;

impl CommandCustomHeader for RetryHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([(
            CheetahString::from_static_str("typedRetryHeader"),
            CheetahString::from_static_str("preserved"),
        )]))
    }
}

impl RPCHook for CountingHook {
    fn do_before_request(&self, _remote_addr: SocketAddr, request: &mut RemotingCommand) -> RocketMQResult<()> {
        self.before.fetch_add(1, Ordering::SeqCst);
        request.ensure_ext_fields_initialized();
        request.add_ext_field("hooked", "true");
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: SocketAddr,
        _request: &RemotingCommand,
        response: &mut RemotingCommand,
    ) -> RocketMQResult<()> {
        self.after.fetch_add(1, Ordering::SeqCst);
        response.ensure_ext_fields_initialized();
        response.add_ext_field("afterHook", "true");
        Ok(())
    }
}

#[derive(Default)]
struct CountingSigner {
    calls: AtomicUsize,
}

impl OutboundSigner for CountingSigner {
    fn sign(&self, _request: SecurityRequestView<'_>) -> Result<Signature, SigningError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(Signature::new(vec![(
            CheetahString::from_static_str("connectionSignature"),
            Secret::new(CheetahString::from_string(call.to_string())),
        )]))
    }
}

async fn start_server<H>(runtime: &RuntimeContext, name: &'static str, handler: Arc<H>, tls_enabled: bool) -> SocketAddr
where
    H: ConnectionHandler,
{
    let service = runtime.service_context(name);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind scripted server");
    let address = listener.local_addr().expect("scripted server address");
    let mut tls = TlsConfig::default();
    if tls_enabled {
        tls.test_mode_enable = true;
        tls.server.mode = TlsMode::Permissive;
    } else {
        tls.server.mode = TlsMode::Disabled;
    }
    let transport = TransportListener::new(
        listener,
        service.task_group().clone(),
        TlsServerRuntime::initialize_with_service_context(tls, &service)
            .await
            .expect("initialize scripted server TLS"),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
        Duration::from_secs(1),
    );
    service
        .spawn_service("go-away-scripted-listener", async move {
            let _ = transport.run(handler).await;
        })
        .expect("spawn scripted listener");
    address
}

async fn start_client(
    runtime: &RuntimeContext,
    name: &'static str,
    policy: Option<GoAwayPolicy>,
    tls_enabled: bool,
) -> Arc<TransportClient> {
    start_client_with_security(runtime, name, policy, tls_enabled, None).await
}

async fn start_client_with_security(
    runtime: &RuntimeContext,
    name: &'static str,
    policy: Option<GoAwayPolicy>,
    tls_enabled: bool,
    transport_security: Option<Arc<TransportSecurity>>,
) -> Arc<TransportClient> {
    let mut tls = TlsConfig::default();
    if tls_enabled {
        tls.enable = true;
        tls.test_mode_enable = true;
        tls.client = TlsClientConfig {
            auth_server: false,
            ..TlsClientConfig::default()
        };
    }
    let config = Arc::new(TransportClientConfig {
        tls,
        ..TransportClientConfig::default()
    });
    let mut builder = TransportClient::builder(config, DefaultRequestProcessor, runtime.service_context(name));
    if let Some(policy) = policy {
        builder = builder.go_away_policy(policy);
    }
    if let Some(transport_security) = transport_security {
        builder = builder.transport_security(transport_security);
    }
    let client = Arc::new(builder.build().expect("build persistent client"));
    client.start().await.expect("start persistent client");
    client
}

fn retry_safe_reads() -> GoAwayPolicy {
    GoAwayPolicy::enabled_for_request_codes([RequestCode::GetBrokerClusterInfo.to_i32()])
}

async fn shutdown(runtime: RuntimeContext, clients: &[Arc<TransportClient>]) {
    for client in clients {
        client.shutdown();
    }
    let report = runtime.shutdown_tasks(Duration::from_secs(2)).await;
    report.assert_no_task_leak().expect("GO_AWAY lifecycle tasks");
}

#[tokio::test]
async fn go_away_retries_once_on_a_replacement_connection() {
    let runtime = RuntimeContext::from_current("go-away-replacement-test");
    let handler = Arc::new(ScriptedHandler::new(vec![
        Reply::immediate(ResponseCode::GoAway.to_i32(), b"retire")
            .with_late_success_after_calls(2, b"late-old-session"),
        Reply::immediate(ResponseCode::Success.to_i32(), b"replacement"),
    ]));
    let address = start_server(&runtime, "go-away-replacement-server", handler.clone(), false).await;
    let signer = Arc::new(CountingSigner::default());
    let client = start_client_with_security(
        &runtime,
        "go-away-replacement-client",
        Some(retry_safe_reads()),
        false,
        Some(Arc::new(TransportSecurity::development_insecure_loopback(
            None,
            Some(signer.clone()),
        ))),
    )
    .await;
    let hook = Arc::new(CountingHook::default());
    client.register_rpc_hook(hook.clone());
    let target = CheetahString::from_string(address.to_string());
    let mut request = RemotingCommand::create_request_command(RequestCode::GetBrokerClusterInfo, RetryHeader)
        .set_body("payload")
        .set_language(LanguageCode::GO)
        .set_version(412)
        .set_remark("logical-request")
        .set_serialize_type(SerializeType::ROCKETMQ);
    request.ensure_ext_fields_initialized();
    request.add_ext_field("original", "value");

    let response = client
        .invoke_request_with_deadline(Some(&target), request, RequestDeadline::after(Duration::from_secs(2)))
        .await
        .expect("GO_AWAY retry should succeed");

    assert_eq!(response.code(), ResponseCode::Success.to_i32());
    assert_eq!(response.body().map(|body| body.as_ref()), Some(b"replacement".as_ref()));
    assert_eq!(
        response
            .ext_fields()
            .and_then(|fields| fields.get("afterHook"))
            .map(CheetahString::as_str),
        Some("true")
    );
    let observed = handler.observations();
    assert_eq!(observed.len(), 2);
    assert_ne!(observed[0].0, observed[1].0, "retry must use a replacement session");
    assert_ne!(observed[0].1.opaque(), observed[1].1.opaque());
    assert_eq!(observed[0].1.code(), observed[1].1.code());
    assert_eq!(observed[0].1.language(), observed[1].1.language());
    assert_eq!(observed[0].1.version(), observed[1].1.version());
    assert_eq!(observed[0].1.serialize_type(), observed[1].1.serialize_type());
    assert_eq!(observed[0].1.flag(), observed[1].1.flag());
    assert_eq!(observed[0].1.body(), observed[1].1.body());
    assert_eq!(observed[0].1.remark(), observed[1].1.remark());
    for (_, request) in &observed {
        assert_eq!(
            request
                .ext_fields()
                .and_then(|fields| fields.get("original"))
                .map(CheetahString::as_str),
            Some("value")
        );
        assert_eq!(
            request
                .ext_fields()
                .and_then(|fields| fields.get("hooked"))
                .map(CheetahString::as_str),
            Some("true")
        );
        assert_eq!(
            request
                .ext_fields()
                .and_then(|fields| fields.get("typedRetryHeader"))
                .map(CheetahString::as_str),
            Some("preserved")
        );
    }
    assert_eq!(hook.before.load(Ordering::SeqCst), 1);
    assert_eq!(hook.after.load(Ordering::SeqCst), 1);
    assert_eq!(signer.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        observed[0]
            .1
            .ext_fields()
            .and_then(|fields| fields.get("connectionSignature"))
            .map(CheetahString::as_str),
        Some("1")
    );
    assert_eq!(
        observed[1]
            .1
            .ext_fields()
            .and_then(|fields| fields.get("connectionSignature"))
            .map(CheetahString::as_str),
        Some("2")
    );
    assert_eq!(client.snapshot().pending.count, 0);
    assert_eq!(client.snapshot().connection_count, 1);

    shutdown(runtime, &[client]).await;
}

#[tokio::test]
async fn nameserver_routing_retires_the_actual_selected_endpoint() {
    let runtime = RuntimeContext::from_current("go-away-nameserver-routing-test");
    let handler = Arc::new(ScriptedHandler::new(vec![
        Reply::immediate(ResponseCode::GoAway.to_i32(), b"retire"),
        Reply::immediate(ResponseCode::Success.to_i32(), b"replacement"),
    ]));
    let address = start_server(&runtime, "go-away-nameserver-routing-server", handler.clone(), false).await;
    let client = start_client(
        &runtime,
        "go-away-nameserver-routing-client",
        Some(retry_safe_reads()),
        false,
    )
    .await;
    let identity = CheetahString::from_string(address.to_string());
    client.update_name_server_address_list(vec![identity]).await;

    let response = client
        .invoke_request_with_deadline(
            None,
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            RequestDeadline::after(Duration::from_secs(2)),
        )
        .await
        .expect("NameServer GO_AWAY retry");

    assert_eq!(response.code(), ResponseCode::Success.to_i32());
    let observed = handler.observations();
    assert_eq!(observed.len(), 2);
    assert_ne!(observed[0].0, observed[1].0);
    assert_eq!(client.snapshot().connection_count, 1);
    assert_eq!(client.snapshot().pending.count, 0);

    shutdown(runtime, &[client]).await;
}

#[tokio::test]
async fn a_second_go_away_fails_without_an_unbounded_retry() {
    let runtime = RuntimeContext::from_current("go-away-bound-test");
    let handler = Arc::new(ScriptedHandler::new(vec![
        Reply::immediate(ResponseCode::GoAway.to_i32(), b"first"),
        Reply::immediate(ResponseCode::GoAway.to_i32(), b"second"),
    ]));
    let address = start_server(&runtime, "go-away-bound-server", handler.clone(), false).await;
    let client = start_client(&runtime, "go-away-bound-client", Some(retry_safe_reads()), false).await;
    let hook = Arc::new(CountingHook::default());
    client.register_rpc_hook(hook.clone());
    let target = CheetahString::from_string(address.to_string());

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.invoke_request_with_deadline(
            Some(&target),
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            RequestDeadline::after(Duration::from_secs(1)),
        ),
    )
    .await
    .expect("bounded retry must complete");
    let error = match result {
        Ok(_) => panic!("second GO_AWAY must be an error"),
        Err(error) => error,
    };

    let RocketMQError::Rpc(RpcClientError::UnexpectedResponseCode { code, code_name }) = error else {
        panic!("second GO_AWAY must preserve the typed unexpected-response error");
    };
    assert_eq!(code, ResponseCode::GoAway.to_i32());
    assert_eq!(code_name, "GO_AWAY after replacement-connection retry");
    assert_eq!(handler.calls.load(Ordering::SeqCst), 2);
    assert_eq!(hook.before.load(Ordering::SeqCst), 1);
    assert_eq!(hook.after.load(Ordering::SeqCst), 0);
    assert_eq!(client.snapshot().pending.count, 0);
    assert_eq!(client.snapshot().connection_count, 0);

    shutdown(runtime, &[client]).await;
}

#[tokio::test]
async fn disabled_and_non_allowlisted_requests_preserve_single_attempt_behavior() {
    let runtime = RuntimeContext::from_current("go-away-policy-test");
    let handler = Arc::new(ScriptedHandler::new(vec![
        Reply::immediate(ResponseCode::GoAway.to_i32(), b"disabled"),
        Reply::immediate(ResponseCode::GoAway.to_i32(), b"side-effect"),
    ]));
    let address = start_server(&runtime, "go-away-policy-server", handler.clone(), false).await;
    let target = CheetahString::from_string(address.to_string());
    let disabled = start_client(&runtime, "go-away-disabled-client", None, false).await;
    let allowlisted = start_client(&runtime, "go-away-allowlisted-client", Some(retry_safe_reads()), false).await;

    let disabled_response = disabled
        .invoke_request(
            Some(&target),
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            1_000,
        )
        .await
        .expect("disabled policy preserves the response");
    let side_effect_response = allowlisted
        .invoke_request(
            Some(&target),
            RemotingCommand::create_remoting_command(RequestCode::SendMessage),
            1_000,
        )
        .await
        .expect("non-allowlisted request preserves the response");

    assert_eq!(disabled_response.code(), ResponseCode::GoAway.to_i32());
    assert_eq!(side_effect_response.code(), ResponseCode::GoAway.to_i32());
    assert_eq!(handler.calls.load(Ordering::SeqCst), 2);

    shutdown(runtime, &[disabled, allowlisted]).await;
}

#[tokio::test]
async fn retry_uses_only_the_original_deadline_budget() {
    let runtime = RuntimeContext::from_current("go-away-deadline-test");
    let handler = Arc::new(ScriptedHandler::new(vec![
        Reply::delayed(ResponseCode::GoAway.to_i32(), Duration::from_millis(100), b"retire"),
        Reply::delayed(ResponseCode::Success.to_i32(), Duration::from_millis(150), b"too-late"),
    ]));
    let address = start_server(&runtime, "go-away-deadline-server", handler.clone(), false).await;
    let client = start_client(&runtime, "go-away-deadline-client", Some(retry_safe_reads()), false).await;
    let target = CheetahString::from_string(address.to_string());

    let result = tokio::time::timeout(
        Duration::from_millis(220),
        client.invoke_request_with_deadline(
            Some(&target),
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            RequestDeadline::after(Duration::from_millis(180)),
        ),
    )
    .await;

    assert!(matches!(result, Ok(Err(_))), "the immutable request deadline must win");
    handler.wait_for_calls(2).await;
    assert_eq!(client.snapshot().pending.count, 0);

    shutdown(runtime, &[client]).await;
}

#[tokio::test]
async fn an_exhausted_deadline_never_writes_a_second_attempt() {
    let runtime = RuntimeContext::from_current("go-away-exhausted-deadline-test");
    let handler = Arc::new(ScriptedHandler::new(vec![Reply::delayed(
        ResponseCode::GoAway.to_i32(),
        Duration::from_millis(80),
        b"too-late",
    )]));
    let address = start_server(&runtime, "go-away-exhausted-deadline-server", handler.clone(), false).await;
    let client = start_client(
        &runtime,
        "go-away-exhausted-deadline-client",
        Some(retry_safe_reads()),
        false,
    )
    .await;
    let target = CheetahString::from_string(address.to_string());

    let result = client
        .invoke_request_with_deadline(
            Some(&target),
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            RequestDeadline::after(Duration::from_millis(30)),
        )
        .await;

    assert!(result.is_err());
    handler.wait_for_completions(1).await;
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    assert_eq!(client.snapshot().pending.count, 0);

    shutdown(runtime, &[client]).await;
}

#[tokio::test]
async fn retiring_the_old_session_does_not_remove_a_concurrent_replacement() {
    let runtime = RuntimeContext::from_current("go-away-cache-race-test");
    let first_entered = Arc::new(tokio::sync::Notify::new());
    let first_release = Arc::new(tokio::sync::Notify::new());
    let handler = Arc::new(ScriptedHandler::gated_first(
        vec![
            Reply::immediate(ResponseCode::GoAway.to_i32(), b"retire"),
            Reply::immediate(ResponseCode::Success.to_i32(), b"concurrent"),
            Reply::immediate(ResponseCode::Success.to_i32(), b"replacement"),
        ],
        first_entered.clone(),
        first_release.clone(),
    ));
    let address = start_server(&runtime, "go-away-cache-race-server", handler.clone(), false).await;
    let client = start_client(&runtime, "go-away-cache-race-client", Some(retry_safe_reads()), false).await;
    let target = CheetahString::from_string(address.to_string());

    let primary_client = client.clone();
    let primary_target = target.clone();
    let primary = tokio::spawn(async move {
        primary_client
            .invoke_request_with_deadline(
                Some(&primary_target),
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                RequestDeadline::after(Duration::from_secs(2)),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), first_entered.notified())
        .await
        .expect("first request should reach the old session");

    let concurrent_client = client.clone();
    let concurrent_target = target.clone();
    let concurrent = tokio::spawn(async move {
        concurrent_client
            .invoke_request_with_deadline(
                Some(&concurrent_target),
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                RequestDeadline::after(Duration::from_secs(2)),
            )
            .await
    });
    handler.wait_for_calls(2).await;
    first_release.notify_one();

    let primary_response = primary.await.expect("primary task").expect("primary response");
    let concurrent_response = concurrent.await.expect("concurrent task").expect("concurrent response");
    assert_eq!(primary_response.code(), ResponseCode::Success.to_i32());
    assert_eq!(concurrent_response.code(), ResponseCode::Success.to_i32());
    let observed = handler.observations();
    assert_eq!(observed.len(), 3);
    assert_eq!(
        observed[0].0, observed[1].0,
        "concurrent work started on the old session"
    );
    assert_ne!(observed[0].0, observed[2].0, "retry must install a replacement session");
    assert_eq!(client.snapshot().pending.count, 0);
    assert_eq!(client.snapshot().connection_count, 1);

    shutdown(runtime, &[client]).await;
}

#[tokio::test]
async fn a_short_go_away_request_does_not_truncate_an_older_pending_deadline() {
    let runtime = RuntimeContext::from_current("go-away-independent-drain-deadline-test");
    let handler = Arc::new(ScriptedHandler::new(vec![
        Reply::delayed(
            ResponseCode::Success.to_i32(),
            Duration::from_millis(700),
            b"older-long-request",
        ),
        Reply::immediate(ResponseCode::GoAway.to_i32(), b"retire"),
        Reply::immediate(ResponseCode::Success.to_i32(), b"replacement"),
    ]));
    let address = start_server(
        &runtime,
        "go-away-independent-drain-deadline-server",
        handler.clone(),
        false,
    )
    .await;
    let client = start_client(
        &runtime,
        "go-away-independent-drain-deadline-client",
        Some(retry_safe_reads()),
        false,
    )
    .await;
    let target = CheetahString::from_string(address.to_string());

    let older_client = client.clone();
    let older_target = target.clone();
    let older = tokio::spawn(async move {
        older_client
            .invoke_request_with_deadline(
                Some(&older_target),
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                RequestDeadline::after(Duration::from_secs(2)),
            )
            .await
    });
    handler.wait_for_calls(1).await;

    let short_response = client
        .invoke_request_with_deadline(
            Some(&target),
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            RequestDeadline::after(Duration::from_millis(500)),
        )
        .await
        .expect("short request should reconnect within its own budget");
    let older_response = older
        .await
        .expect("older request task")
        .expect("older request keeps its longer deadline while the old session drains");

    assert_eq!(
        short_response.body().map(|body| body.as_ref()),
        Some(b"replacement".as_ref())
    );
    assert_eq!(
        older_response.body().map(|body| body.as_ref()),
        Some(b"older-long-request".as_ref())
    );
    assert_eq!(client.snapshot().pending.count, 0);
    assert_eq!(client.snapshot().connection_count, 1);

    shutdown(runtime, &[client]).await;
}

#[tokio::test]
async fn oneway_requests_do_not_enter_the_response_retry_policy() {
    let runtime = RuntimeContext::from_current("go-away-oneway-test");
    let handler = Arc::new(ScriptedHandler::new(vec![Reply::immediate(
        ResponseCode::GoAway.to_i32(),
        b"ignored",
    )]));
    let address = start_server(&runtime, "go-away-oneway-server", handler.clone(), false).await;
    let client = start_client(&runtime, "go-away-oneway-client", Some(retry_safe_reads()), false).await;
    let target = CheetahString::from_string(address.to_string());

    client
        .invoke_request_oneway_with_deadline(
            &target,
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            RequestDeadline::after(Duration::from_secs(1)),
        )
        .await
        .expect("oneway write");
    handler.wait_for_calls(1).await;
    tokio::task::yield_now().await;
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    assert_eq!(client.snapshot().pending.count, 0);

    shutdown(runtime, &[client]).await;
}

#[tokio::test]
#[cfg(feature = "tls")]
async fn tls_go_away_retry_uses_a_replacement_connection() {
    let runtime = RuntimeContext::from_current("go-away-tls-test");
    let handler = Arc::new(ScriptedHandler::new(vec![
        Reply::immediate(ResponseCode::GoAway.to_i32(), b"retire"),
        Reply::immediate(ResponseCode::Success.to_i32(), b"replacement"),
    ]));
    let address = start_server(&runtime, "go-away-tls-server", handler.clone(), true).await;
    let client = start_client(&runtime, "go-away-tls-client", Some(retry_safe_reads()), true).await;
    let target = CheetahString::from_string(address.to_string());

    let response = client
        .invoke_request_with_deadline(
            Some(&target),
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            RequestDeadline::after(Duration::from_secs(2)),
        )
        .await
        .expect("TLS GO_AWAY retry");

    assert_eq!(response.code(), ResponseCode::Success.to_i32());
    let observed = handler.observations();
    assert_eq!(observed.len(), 2);
    assert_ne!(observed[0].0, observed[1].0);
    assert_eq!(client.snapshot().pending.count, 0);

    shutdown(runtime, &[client]).await;
}
