// Copyright 2023 The RocketMQ Rust Authors
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

use std::future;
use std::sync::Arc;

use crate::config::ServerConfig;
#[cfg(feature = "tls")]
use crate::config::TlsConfig;
#[cfg(feature = "tls")]
use crate::config::TlsMode;
#[cfg(feature = "tls")]
use crate::config::TlsServerConfig;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use super::*;
use crate::clients::rocketmq_tokio_client::TransportClient;
use crate::clients::LegacyDefaultRequestProcessor as DefaultRequestProcessor;
use crate::runtime::config::client_config::TransportClientConfig;

use self::runtime_test_support::test_service_context;
#[cfg(all(test, not(doctest)))]
use super::connection_handler::SessionCommandInterceptor;
#[cfg(all(test, not(doctest)))]
use super::connection_handler::TestDeferredResponse;
#[cfg(all(test, not(doctest)))]
use super::connection_handler::TestRequestHook;
#[cfg(all(test, not(doctest)))]
use super::connection_handler::TestRequestHookResult;
use super::launch::run_with_report;
use super::launch::run_with_report_with_service_context;
use super::lifecycle_events::enqueue_lifecycle_event;
use super::lifecycle_events::run_lifecycle_event_dispatcher;
use super::lifecycle_events::LifecycleEventConfig;
use super::lifecycle_events::LifecycleEventPublishOutcome;
use super::lifecycle_events::LifecycleEventPublisher;
use super::shutdown::new_remoting_server_task_group_with_service_context;

#[derive(Clone)]
struct CorrelatingV1Processor;

impl RequestProcessor for CorrelatingV1Processor {
    async fn process_request(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) -> RocketMQResult<Option<rocketmq_protocol::protocol::remoting_command::RemotingCommand>> {
        let correlated = channel
            .send_wait_response(
                rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(8_801)
                    .set_opaque(request.opaque()),
                30_000,
            )
            .await?;
        Ok(Some(
            rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_response_command_with_code(
                rocketmq_protocol::code::response_code::ResponseCode::Success,
            )
            .set_opaque(request.opaque())
            .set_body(correlated.body().cloned().unwrap_or_default()),
        ))
    }
}

#[cfg(test)]
mod runtime_test_support {
    use super::*;

    pub(super) fn test_service_context(name: &'static str) -> ChildServiceContext {
        RuntimeContext::from_current(name).service_context("remoting-server-service")
    }
}

#[cfg(all(test, not(doctest)))]
impl SessionCommandInterceptor for Option<TestRequestHook> {
    fn intercept(&self, code: i32, opaque: i32, channel: Channel, request_executor_group: TaskGroup) -> bool {
        let Some(hook) = self.as_ref() else {
            return false;
        };
        let response_channel = channel.clone();
        let deferred_response: TestDeferredResponse = Box::new(move |response| {
            Box::pin(async move {
                let _ = response_channel.send_command(response.set_opaque(opaque)).await;
            })
        });
        matches!(
            hook(code, opaque, channel, request_executor_group, deferred_response),
            TestRequestHookResult::Intercept
        )
    }
}

struct ConnectSignalListener {
    connected: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    disconnected: std::sync::Mutex<Option<oneshot::Sender<()>>>,
}

struct SlowLifecycleListener {
    first_delivery: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    disconnected_delivery: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    deliveries: std::sync::atomic::AtomicUsize,
}

struct RequireTransportSignature {
    calls: std::sync::atomic::AtomicUsize,
}

impl rocketmq_security_api::RequestPolicy for RequireTransportSignature {
    fn evaluate_authenticated(
        &self,
        context: rocketmq_security_api::AuthenticatedRequestContext<'_>,
    ) -> rocketmq_security_api::Decision {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if context.request().fields().contains_key("TransportSignature") {
            rocketmq_security_api::Decision::Allow
        } else {
            rocketmq_security_api::Decision::deny("missing transport signature")
        }
    }
}

struct RemotingMarkerSigner;

type DelayedResponse = (String, TestDeferredResponse);

async fn await_server_startup(startup: oneshot::Receiver<RocketMQResult<SocketAddr>>) -> SocketAddr {
    tokio::time::timeout(Duration::from_secs(1), startup)
        .await
        .expect("remoting server startup acknowledgement deadline")
        .expect("remoting server startup sender")
        .expect("remoting server should bind")
}

#[tokio::test(start_paused = true)]
async fn lifecycle_event_queue_reports_capacity_overload_and_shutdown() {
    let (sender, receiver) = mpsc::channel(1);
    let cancellation = CancellationToken::new();

    let first = enqueue_lifecycle_event(&sender, 1_u8, Duration::from_millis(5), &cancellation).await;
    assert_eq!(first, LifecycleEventPublishOutcome::Queued);

    let overloaded = enqueue_lifecycle_event(&sender, 2_u8, Duration::from_millis(5), &cancellation).await;
    assert_eq!(overloaded, LifecycleEventPublishOutcome::DeadlineExpired);

    cancellation.cancel();
    let shutting_down = enqueue_lifecycle_event(&sender, 3_u8, Duration::from_millis(5), &cancellation).await;
    assert_eq!(shutting_down, LifecycleEventPublishOutcome::ShuttingDown);

    drop(receiver);
}

#[tokio::test]
async fn lifecycle_event_queue_reports_closed_dispatcher() {
    let (sender, receiver) = mpsc::channel(1);
    drop(receiver);

    let outcome = enqueue_lifecycle_event(&sender, 1_u8, Duration::from_millis(5), &CancellationToken::new()).await;

    assert_eq!(outcome, LifecycleEventPublishOutcome::DispatcherClosed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_lifecycle_listener_allows_later_callback_before_release() {
    let context = RuntimeContext::from_current("remoting-lifecycle-overload-test");
    let service = context.service_context("remoting-lifecycle-overload");
    let task_group = service.task_group().clone();
    let harness = crate::local::LocalRequestHarness::new(task_group.clone())
        .await
        .expect("local channel harness");
    let channel = harness.channel();
    let remote_addr = harness.remote_address();
    let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let (first_delivery_tx, first_delivery_rx) = oneshot::channel();
    let (disconnected_delivery_tx, disconnected_delivery_rx) = oneshot::channel();
    let listener = Arc::new(SlowLifecycleListener {
        first_delivery: std::sync::Mutex::new(Some(first_delivery_tx)),
        disconnected_delivery: std::sync::Mutex::new(Some(disconnected_delivery_tx)),
        release: Arc::clone(&release),
        deliveries: std::sync::atomic::AtomicUsize::new(0),
    });
    let (sender, receiver) = mpsc::channel(1);
    let cancellation = task_group.cancellation_token();
    let config = LifecycleEventConfig {
        queue_capacity: 1,
        publish_timeout: Duration::from_millis(5),
        drain_timeout: Duration::from_millis(100),
        listener_callback_budget: Duration::from_millis(1),
    };
    let telemetry = TransportTelemetry::noop();
    task_group
        .spawn_service(
            "remoting-lifecycle-overload-dispatcher",
            run_lifecycle_event_dispatcher(
                receiver,
                listener.clone(),
                cancellation.clone(),
                config,
                service.metadata_io().clone(),
                telemetry.clone(),
            ),
        )
        .expect("event dispatcher should spawn");
    let publisher = LifecycleEventPublisher {
        sender,
        publish_timeout: config.publish_timeout,
        cancellation,
        telemetry,
    };

    assert_eq!(
        publisher
            .publish(TokioEvent::new(
                ConnectionNetEvent::CONNECTED(remote_addr),
                remote_addr,
                channel.clone(),
            ))
            .await,
        LifecycleEventPublishOutcome::Queued
    );
    first_delivery_rx
        .await
        .expect("slow listener should receive the first event");
    assert_eq!(
        publisher
            .publish(TokioEvent::new(
                ConnectionNetEvent::DISCONNECTED,
                remote_addr,
                channel.clone(),
            ))
            .await,
        LifecycleEventPublishOutcome::Queued
    );
    tokio::time::timeout(Duration::from_secs(1), disconnected_delivery_rx)
        .await
        .expect("disconnected callback should run after the first callback deadline")
        .expect("disconnected callback should be delivered before release");

    {
        let (released, condition) = release.as_ref();
        *released.lock().expect("slow listener release lock") = true;
        condition.notify_all();
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while service.metadata_io().snapshot().blocking_still_running != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed-out lifecycle callback should leave the blocking executor after release");

    let channel_report = channel.close_with_report(Duration::from_secs(1)).await;
    assert!(channel_report.is_healthy(), "{}", channel_report.to_json());

    task_group.cancel();
    drop(publisher);
    let report = task_group.shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(listener.deliveries.load(std::sync::atomic::Ordering::Acquire), 2);
}

impl rocketmq_security_api::OutboundSigner for RemotingMarkerSigner {
    fn sign(
        &self,
        _request: rocketmq_security_api::SecurityRequestView<'_>,
    ) -> Result<rocketmq_security_api::Signature, rocketmq_security_api::SigningError> {
        Ok(rocketmq_security_api::Signature::new(vec![(
            cheetah_string::CheetahString::from_static_str("TransportSignature"),
            rocketmq_security_api::Secret::new(cheetah_string::CheetahString::from_static_str("signed")),
        )]))
    }
}

impl ChannelEventListener for ConnectSignalListener {
    fn on_channel_connect(&self, _remote_addr: &str, _channel: &Channel) {
        if let Some(sender) = self.connected.lock().expect("connect signal lock").take() {
            let _ = sender.send(());
        }
    }

    fn on_channel_close(&self, _remote_addr: &str, _channel: &Channel) {
        if let Some(sender) = self.disconnected.lock().expect("disconnect signal lock").take() {
            let _ = sender.send(());
        }
    }

    fn on_channel_exception(&self, _remote_addr: &str, _channel: &Channel) {}

    fn on_channel_idle(&self, _remote_addr: &str, _channel: &Channel) {}

    fn on_channel_active(&self, _remote_addr: &str, _channel: &Channel) {}
}

impl ChannelEventListener for SlowLifecycleListener {
    fn on_channel_connect(&self, _remote_addr: &str, _channel: &Channel) {
        let delivery = self.deliveries.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if delivery != 0 {
            return;
        }
        if let Some(sender) = self.first_delivery.lock().expect("first delivery lock").take() {
            let _ = sender.send(());
        }
        let (released, condition) = self.release.as_ref();
        let mut released = released.lock().expect("slow listener release lock");
        while !*released {
            released = condition.wait(released).expect("slow listener release wait");
        }
    }

    fn on_channel_close(&self, _remote_addr: &str, _channel: &Channel) {
        self.deliveries.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if let Some(sender) = self
            .disconnected_delivery
            .lock()
            .expect("disconnect delivery lock")
            .take()
        {
            let _ = sender.send(());
        }
    }

    fn on_channel_exception(&self, _remote_addr: &str, _channel: &Channel) {}

    fn on_channel_idle(&self, _remote_addr: &str, _channel: &Channel) {}

    fn on_channel_active(&self, _remote_addr: &str, _channel: &Channel) {}
}

#[tokio::test]
async fn remoting_server_task_group_from_service_context_is_parented() {
    let context = RuntimeContext::from_current("remoting-server-context-test");
    let service = context.service_context("remoting-server-service");

    let task_group = new_remoting_server_task_group_with_service_context(&service);

    assert_eq!(task_group.parent_id(), Some(service.task_group().id()));
    assert_eq!(task_group.name(), "rocketmq.remoting.server");

    let report = service.task_group().shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn run_with_report_returns_component_report_without_retaining_completed_child() {
    let context = RuntimeContext::from_current("remoting-server-parent-report-test");
    let service = context.service_context("remoting-server-parent");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_task = tokio::spawn(run_with_report_with_service_context(
        service.clone(),
        listener,
        async {
            let _ = shutdown_rx.await;
        },
        DefaultRequestProcessor,
        None,
        Vec::new(),
        None,
    ));

    let _ = shutdown_tx.send(());
    let report = tokio::time::timeout(Duration::from_secs(3), server_task)
        .await
        .expect("server should shut down before timeout")
        .expect("server task should not panic")
        .expect("server should return shutdown report");
    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(report.name, "rocketmq.remoting.server");

    let parent_report = service.task_group().shutdown(Duration::from_secs(1)).await;
    assert!(parent_report.is_healthy(), "{}", parent_report.to_json());
    assert!(parent_report.children.is_empty(), "{}", parent_report.to_json());
}

#[tokio::test]
async fn run_with_shutdown_bind_error_returns_without_panicking() {
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_string(),
        listen_port: 70000,
        ..ServerConfig::default()
    });
    let mut server = TransportServer::<DefaultRequestProcessor>::new(
        config,
        test_service_context("remoting-server-bind-error-test"),
    );

    server
        .run_with_shutdown(DefaultRequestProcessor, None, future::pending::<()>())
        .await;
}

#[tokio::test]
async fn startup_rejects_zero_lifecycle_event_queue_capacity() {
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 0,
        ..ServerConfig::default()
    });
    let mut server = TransportServer::<DefaultRequestProcessor>::new(
        config,
        test_service_context("remoting-server-invalid-event-config-test"),
    );
    server.lifecycle_event_config.queue_capacity = 0;
    let (startup_tx, startup_rx) = oneshot::channel();

    let report = server
        .run_with_shutdown_report_and_startup(DefaultRequestProcessor, None, future::pending::<()>(), startup_tx)
        .await;

    assert!(report.is_none());
    let error = startup_rx
        .await
        .expect("startup error should be reported")
        .expect_err("zero event queue capacity must be rejected");
    assert!(matches!(
        error,
        RocketMQError::ConfigInvalidValue {
            key: "channelEventQueueCapacity",
            ..
        }
    ));
}

#[tokio::test]
async fn run_with_shutdown_report_bind_error_returns_none() {
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_string(),
        listen_port: 70000,
        ..ServerConfig::default()
    });
    let mut server = TransportServer::<DefaultRequestProcessor>::new(
        config,
        test_service_context("remoting-server-report-bind-error-test"),
    );

    let report = server
        .run_with_shutdown_report(DefaultRequestProcessor, None, future::pending::<()>())
        .await;

    assert!(report.is_none());
}

#[tokio::test]
async fn startup_acknowledgement_reports_bind_failure() {
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_string(),
        listen_port: 70000,
        ..ServerConfig::default()
    });
    let mut server = TransportServer::<DefaultRequestProcessor>::new(
        config,
        test_service_context("remoting-server-startup-failure-test"),
    );
    let (startup_tx, startup_rx) = oneshot::channel();

    let report = server
        .run_with_shutdown_report_and_startup(DefaultRequestProcessor, None, future::pending::<()>(), startup_tx)
        .await;

    assert!(report.is_none());
    let error = startup_rx
        .await
        .expect("startup acknowledgement should be sent")
        .expect_err("invalid port must fail startup");
    assert!(error.to_string().contains("remoting-server-bind"));
}

#[tokio::test]
async fn checked_entries_share_capability_validation_and_typed_bind_failures() {
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 0,
        ..ServerConfig::default()
    });
    let mut server = TransportServer::<DefaultRequestProcessor>::new(
        config,
        test_service_context("remoting-server-checked-configuration-test"),
    );
    server.lifecycle_event_config.queue_capacity = 0;
    let (startup_tx, startup_rx) = oneshot::channel();

    let result = server
        .try_run_with_shutdown_report_and_startup(DefaultRequestProcessor, None, future::pending::<()>(), startup_tx)
        .await;

    assert!(matches!(
        result,
        Err(ServerStartError::Configuration {
            stage: "lifecycle_events",
            ..
        })
    ));
    assert!(matches!(
        startup_rx.await.expect("startup error should be reported"),
        Err(ServerStartError::Configuration {
            stage: "lifecycle_events",
            ..
        })
    ));

    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 70000,
        ..ServerConfig::default()
    });
    let mut server = TransportServer::<DefaultRequestProcessor>::new(
        config,
        test_service_context("remoting-server-checked-bind-test"),
    );
    let result = server
        .try_run_with_shutdown_report(DefaultRequestProcessor, None, future::pending::<()>())
        .await;
    assert!(matches!(
        result,
        Err(ServerStartError::Bind {
            stage: "listener.bind",
            ..
        })
    ));

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pre-bound listener should bind");
    let mut server = TransportServer::<DefaultRequestProcessor>::new(
        Arc::new(ServerConfig::default()),
        test_service_context("remoting-server-checked-pre-bound-configuration-test"),
    );
    server.lifecycle_event_config.queue_capacity = 0;
    let (startup_tx, startup_rx) = oneshot::channel();
    let result = server
        .try_serve_bound_listener_until_with_startup(
            listener,
            DefaultRequestProcessor,
            None,
            None,
            future::pending::<()>(),
            startup_tx,
        )
        .await;
    assert!(matches!(
        result,
        Err(ServerStartError::Configuration {
            stage: "lifecycle_events",
            ..
        })
    ));
    assert!(matches!(
        startup_rx.await.expect("pre-bound startup error should be reported"),
        Err(ServerStartError::Configuration {
            stage: "lifecycle_events",
            ..
        })
    ));
}

#[tokio::test]
async fn prepared_security_state_distinguishes_legacy_insecure_and_secure_profiles() {
    let context = RuntimeContext::from_current("remoting-server-security-state-test");
    let service = context.service_context("remoting-server-security-state");
    let config = Arc::new(ServerConfig::default());

    let mut unconfigured = TransportServer::<DefaultRequestProcessor>::new(config.clone(), service.component("legacy"));
    let prepared = unconfigured
        .prepare_server(DefaultRequestProcessor, None)
        .await
        .expect("legacy fallback should prepare");
    assert_eq!(
        prepared.security_state,
        super::capabilities::ServerSecurityState::Unconfigured
    );
    drop(prepared);

    let mut insecure = TransportServer::<DefaultRequestProcessor>::new(config.clone(), service.component("insecure"))
        .with_transport_security(
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            None,
        );
    let prepared = insecure
        .prepare_server(DefaultRequestProcessor, None)
        .await
        .expect("explicit insecure profile should prepare");
    assert_eq!(
        prepared.security_state,
        super::capabilities::ServerSecurityState::ExplicitInsecureLoopback
    );
    drop(prepared);

    let mut secure = TransportServer::<DefaultRequestProcessor>::new(config, service.component("secure"))
        .with_transport_security(Arc::new(TransportSecurity::secure_enforced(None, None)), None);
    let prepared = secure
        .prepare_server(DefaultRequestProcessor, None)
        .await
        .expect("secure profile should prepare without a legacy fallback");
    assert_eq!(
        prepared.security_state,
        super::capabilities::ServerSecurityState::Secure
    );
    drop(prepared);

    let report = service.task_group().shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn checked_config_startup_acknowledgement_is_sent_after_prepare() {
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_string(),
        listen_port: 0,
        ..ServerConfig::default()
    });
    let mut server =
        TransportServer::<DefaultRequestProcessor>::new(config, test_service_context("remoting-server-startup-test"));
    let (startup_tx, startup_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        server
            .try_run_with_shutdown_report_and_startup(
                DefaultRequestProcessor,
                None,
                async {
                    let _ = shutdown_rx.await;
                },
                startup_tx,
            )
            .await
    });

    let bound_address = startup_rx
        .await
        .expect("startup acknowledgement should be sent")
        .expect("listener should become ready");
    assert_ne!(bound_address.port(), 0);
    TcpStream::connect(bound_address)
        .await
        .expect("acknowledged listener should accept connections");

    let _ = shutdown_tx.send(());
    let report = server_task
        .await
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn checked_pre_bound_startup_acknowledgement_is_sent_after_prepare() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pre-bound listener should bind");
    let mut server = TransportServer::<DefaultRequestProcessor>::new(
        Arc::new(ServerConfig::default()),
        test_service_context("remoting-server-pre-bound-startup-test"),
    );
    let (startup_tx, startup_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        server
            .try_serve_bound_listener_until_with_startup(
                listener,
                DefaultRequestProcessor,
                None,
                None,
                async {
                    let _ = shutdown_rx.await;
                },
                startup_tx,
            )
            .await
    });

    let bound_address = startup_rx
        .await
        .expect("pre-bound startup acknowledgement should be sent")
        .expect("pre-bound listener should become ready");
    TcpStream::connect(bound_address)
        .await
        .expect("acknowledged pre-bound listener should accept connections");

    let _ = shutdown_tx.send(());
    let report = server_task
        .await
        .expect("pre-bound server task should not panic")
        .expect("pre-bound server should report shutdown");
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn checked_startup_reports_task_spawn_failure_before_readiness() {
    let service = test_service_context("remoting-server-task-spawn-failure-test");
    let report = service.task_group().shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pre-bound listener should bind");
    let mut server =
        TransportServer::<DefaultRequestProcessor>::new(Arc::new(ServerConfig::default()), service.clone());
    let lifecycle_listener = Arc::new(ConnectSignalListener {
        connected: std::sync::Mutex::new(None),
        disconnected: std::sync::Mutex::new(None),
    });
    let (startup_tx, startup_rx) = oneshot::channel();
    let result = server
        .try_serve_bound_listener_until_with_startup(
            listener,
            DefaultRequestProcessor,
            None,
            Some(lifecycle_listener),
            future::pending::<()>(),
            startup_tx,
        )
        .await;
    assert!(matches!(
        result,
        Err(ServerStartError::TaskSpawn {
            stage: "lifecycle_event_dispatcher.spawn",
            ..
        })
    ));
    assert!(matches!(
        startup_rx.await.expect("task spawn failure should be reported"),
        Err(ServerStartError::TaskSpawn {
            stage: "lifecycle_event_dispatcher.spawn",
            ..
        })
    ));
    let parent_report = service.task_group().shutdown(Duration::from_secs(1)).await;
    assert!(parent_report.is_healthy(), "{}", parent_report.to_json());
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn checked_startup_reports_typed_tls_failure_before_readiness() {
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 0,
        tls_config: TlsConfig {
            server: TlsServerConfig {
                mode: TlsMode::Enforcing,
                cert_path: Some("missing-cert.pem".to_owned()),
                key_path: Some("missing-key.pem".to_owned()),
                ..Default::default()
            },
            ..Default::default()
        },
        ..ServerConfig::default()
    });
    let mut server = TransportServer::<DefaultRequestProcessor>::new(
        config,
        test_service_context("remoting-server-checked-tls-failure-test"),
    );
    let (startup_tx, startup_rx) = oneshot::channel();
    let result = server
        .try_run_with_shutdown_report_and_startup(DefaultRequestProcessor, None, future::pending::<()>(), startup_tx)
        .await;
    assert!(matches!(
        result,
        Err(ServerStartError::Tls {
            stage: "tls.initialize",
            ..
        })
    ));
    assert!(matches!(
        startup_rx.await.expect("TLS startup error should be reported"),
        Err(ServerStartError::Tls {
            stage: "tls.initialize",
            ..
        })
    ));
}

#[tokio::test]
async fn run_shutdown_drains_connection_tasks() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let addr = listener.local_addr().expect("listener should have local addr");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_task = tokio::spawn(run_with_report(
        test_service_context("remoting-server-drain-test"),
        listener,
        async {
            let _ = shutdown_rx.await;
        },
        DefaultRequestProcessor,
        None,
        Vec::new(),
        None,
    ));

    let mut clients = Vec::new();
    for _ in 0..4 {
        clients.push(TcpStream::connect(addr).await.expect("client should connect"));
    }
    drop(clients);

    let _ = shutdown_tx.send(());
    let report = tokio::time::timeout(Duration::from_secs(3), server_task)
        .await
        .expect("server should shut down before timeout")
        .expect("server task should not panic")
        .expect("server should return shutdown report");
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn public_client_and_server_exchange_through_canonical_transport_session() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let addr = listener.local_addr().expect("listener address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let service = test_service_context("remoting-server-exchange-test");
    let server_task = tokio::spawn(run_with_report(
        service.clone(),
        listener,
        async {
            let _ = shutdown_rx.await;
        },
        DefaultRequestProcessor,
        None,
        Vec::new(),
        None,
    ));
    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        service.component("client"),
    );
    let remote_addr = cheetah_string::CheetahString::from_string(addr.to_string());
    let request = rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(105);
    let opaque = request.opaque();
    let response = client
        .invoke_request(Some(&remote_addr), request, 1_000)
        .await
        .expect("echo response");
    assert_eq!(response.code(), 105);
    assert_eq!(response.opaque(), opaque);

    let client_report = client.shutdown_with_report(Duration::from_secs(1)).await;
    assert!(client_report.is_healthy());
    let _ = shutdown_tx.send(());
    let report = server_task.await.unwrap().unwrap();
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn remoting_control_response_uses_reserve_when_data_writer_budget_is_full() {
    let limits = AdmissionLimits {
        queued: ResourceLimit { count: 4, bytes: 4096 },
        control_reserve: ResourceLimit { count: 2, bytes: 2048 },
        ..AdmissionLimits::default()
    };
    let admission = Arc::new(AdmissionController::new(limits));
    let scope = crate::admission::AdmissionScope::new("127.0.0.1".parse().expect("loopback address"));
    let _data_one = admission
        .try_acquire(
            crate::admission::AdmissionResource::Queued,
            scope,
            1,
            crate::admission::AdmissionClass::Data,
        )
        .unwrap();
    let _data_two = admission
        .try_acquire(
            crate::admission::AdmissionResource::Queued,
            scope,
            1,
            crate::admission::AdmissionClass::Data,
        )
        .unwrap();
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 0,
        ..ServerConfig::default()
    });
    let mut server = TransportServer::<DefaultRequestProcessor>::new(
        config,
        test_service_context("remoting-server-control-reserve-test"),
    )
    .with_admission_controller(admission);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (startup_tx, startup_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        server
            .run_with_shutdown_report_and_startup(
                DefaultRequestProcessor,
                None,
                async {
                    let _ = shutdown_rx.await;
                },
                startup_tx,
            )
            .await
    });
    let addr = await_server_startup(startup_rx).await;

    let mut client = crate::connection::Connection::new(TcpStream::connect(addr).await.unwrap());
    client
        .send_command(
            rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(
                rocketmq_protocol::code::request_code::RequestCode::HeartBeat,
            )
            .set_opaque(71),
        )
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_millis(250), client.receive_command())
        .await
        .expect("control response should consume the reserved writer budget")
        .unwrap()
        .unwrap();
    assert_eq!(
        response.code(),
        rocketmq_protocol::code::request_code::RequestCode::HeartBeat as i32
    );
    assert_eq!(response.opaque(), 71);

    let _ = shutdown_tx.send(());
    let report = server_task.await.unwrap().unwrap();
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn command_snapshots_reuse_the_fixed_request_executor_owner() {
    const COMMAND_COUNT: usize = 128;

    let request_executor_group = Arc::new(std::sync::Mutex::new(None::<TaskGroup>));
    let request_executor_group_for_hook = request_executor_group.clone();
    let hook: TestRequestHook = Arc::new(move |_code, _opaque, _channel, task_group, _deferred_response| {
        let mut captured = request_executor_group_for_hook
            .lock()
            .expect("request executor group lock");
        if captured.is_none() {
            *captured = Some(task_group);
        }
        TestRequestHookResult::Continue
    });
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 0,
        ..ServerConfig::default()
    });
    let mut server =
        TransportServer::<DefaultRequestProcessor>::new(config, test_service_context("remoting-server-snapshot-test"))
            .with_test_request_hook(hook);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (startup_tx, startup_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        server
            .run_with_shutdown_report_and_startup(
                DefaultRequestProcessor,
                None,
                async {
                    let _ = shutdown_rx.await;
                },
                startup_tx,
            )
            .await
    });
    let addr = await_server_startup(startup_rx).await;

    let mut client = crate::connection::Connection::new(TcpStream::connect(addr).await.unwrap());
    for index in 0..COMMAND_COUNT {
        let opaque = 1_000 + i32::try_from(index).unwrap();
        client
            .send_command(
                rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(
                    rocketmq_protocol::code::request_code::RequestCode::SendMessage,
                )
                .set_opaque(opaque),
            )
            .await
            .unwrap();
        let response = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
            .await
            .expect("snapshot response deadline")
            .expect("snapshot response frame")
            .expect("snapshot response command");
        assert_eq!(response.opaque(), opaque);
    }

    let request_executor_group = request_executor_group
        .lock()
        .expect("request executor group lock")
        .clone()
        .expect("request executor group");
    assert_eq!(request_executor_group.component_count(), 0);

    let _ = shutdown_tx.send(());
    let report = server_task.await.unwrap().unwrap();
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn delayed_data_response_keeps_its_request_admission_class_after_control_request() {
    let limits = AdmissionLimits {
        queued: ResourceLimit { count: 4, bytes: 4096 },
        control_reserve: ResourceLimit { count: 2, bytes: 2048 },
        ..AdmissionLimits::default()
    };
    let admission = Arc::new(AdmissionController::new(limits));
    let scope = crate::admission::AdmissionScope::new("127.0.0.1".parse().expect("loopback address"));
    let delayed: Arc<std::sync::Mutex<Option<DelayedResponse>>> = Arc::new(std::sync::Mutex::new(None));
    let first_seen = Arc::new(tokio::sync::Notify::new());
    let second_seen = Arc::new(tokio::sync::Notify::new());
    let request_executor_group = Arc::new(std::sync::Mutex::new(None::<TaskGroup>));
    let delayed_for_hook = delayed.clone();
    let first_seen_for_hook = first_seen.clone();
    let second_seen_for_hook = second_seen.clone();
    let request_executor_group_for_hook = request_executor_group.clone();
    let hook: TestRequestHook = Arc::new(
        move |code, opaque, channel, task_group, deferred_response| match opaque {
            81 => {
                assert_eq!(
                    code,
                    rocketmq_protocol::code::request_code::RequestCode::SendMessage as i32
                );
                *request_executor_group_for_hook
                    .lock()
                    .expect("request executor group lock") = Some(task_group);
                *delayed_for_hook.lock().expect("delayed response lock") =
                    Some((channel.channel_id().to_owned(), deferred_response));
                first_seen_for_hook.notify_one();
                TestRequestHookResult::Intercept
            }
            82 => {
                assert_eq!(
                    code,
                    rocketmq_protocol::code::request_code::RequestCode::HeartBeat as i32
                );
                let delayed_channel_id = delayed_for_hook
                    .lock()
                    .expect("delayed response lock")
                    .as_ref()
                    .map(|(channel_id, _)| channel_id.clone())
                    .expect("first request snapshot");
                assert_eq!(channel.channel_id(), delayed_channel_id);
                second_seen_for_hook.notify_one();
                TestRequestHookResult::Continue
            }
            _ => TestRequestHookResult::Continue,
        },
    );
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 0,
        ..ServerConfig::default()
    });
    let mut server = TransportServer::<DefaultRequestProcessor>::new(
        config,
        test_service_context("remoting-server-admission-snapshot-test"),
    )
    .with_admission_controller(admission.clone())
    .with_test_request_hook(hook);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (startup_tx, startup_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        server
            .run_with_shutdown_report_and_startup(
                DefaultRequestProcessor,
                None,
                async {
                    let _ = shutdown_rx.await;
                },
                startup_tx,
            )
            .await
    });
    let addr = await_server_startup(startup_rx).await;

    let mut client = crate::connection::Connection::new(TcpStream::connect(addr).await.unwrap());
    client
        .send_command(
            rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(
                rocketmq_protocol::code::request_code::RequestCode::SendMessage,
            )
            .set_opaque(81),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), first_seen.notified())
        .await
        .expect("first data request should reach the processor");
    client
        .send_command(
            rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(
                rocketmq_protocol::code::request_code::RequestCode::HeartBeat,
            )
            .set_opaque(82),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), second_seen.notified())
        .await
        .expect("second control request should reach the processor");
    let control_response = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
        .await
        .expect("control response should use its request snapshot")
        .expect("control response frame")
        .expect("control response command");
    assert_eq!(control_response.opaque(), 82);
    let retained_request_executor_group = request_executor_group
        .lock()
        .expect("request executor group lock")
        .clone()
        .expect("request executor group");
    assert_eq!(retained_request_executor_group.component_count(), 0);

    let _data_one = admission
        .try_acquire(
            crate::admission::AdmissionResource::Queued,
            scope,
            1,
            crate::admission::AdmissionClass::Data,
        )
        .unwrap();
    let _data_two = admission
        .try_acquire(
            crate::admission::AdmissionResource::Queued,
            scope,
            1,
            crate::admission::AdmissionClass::Data,
        )
        .unwrap();

    let (_channel_id, delayed_write) = delayed
        .lock()
        .expect("delayed response lock")
        .take()
        .expect("first request snapshot");
    delayed_write(
        rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_response_command_with_code(
            rocketmq_protocol::code::response_code::ResponseCode::Success,
        ),
    )
    .await;
    assert_eq!(retained_request_executor_group.component_count(), 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), client.receive_command())
            .await
            .is_err(),
        "the delayed data response must not borrow the later control request reserve"
    );

    let _ = shutdown_tx.send(());
    let report = server_task.await.unwrap().unwrap();
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn production_remoting_client_and_server_use_injected_transport_security() {
    let policy = Arc::new(RequireTransportSignature {
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let security = Arc::new(crate::security::TransportSecurity::development_insecure_loopback(
        Some(policy.clone()),
        None,
    ));
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 0,
        ..ServerConfig::default()
    });
    let service = test_service_context("remoting-server-security-test");
    let mut server = TransportServer::<DefaultRequestProcessor>::new(config, service.clone())
        .with_transport_security(security, Some(rocketmq_security_api::Principal::new("remoting-test")));
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (startup_tx, startup_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        server
            .run_with_shutdown_report_and_startup(
                DefaultRequestProcessor,
                None,
                async {
                    let _ = shutdown_rx.await;
                },
                startup_tx,
            )
            .await
    });
    let addr = await_server_startup(startup_rx).await;

    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        service.component("client"),
    )
    .with_transport_security(Arc::new(
        crate::security::TransportSecurity::development_insecure_loopback(None, Some(Arc::new(RemotingMarkerSigner))),
    ));
    let remote = cheetah_string::CheetahString::from_string(addr.to_string());
    let response = client
        .invoke_request(
            Some(&remote),
            rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(105),
            1_000,
        )
        .await
        .expect("signed high-level request");
    assert_eq!(response.code(), 105);
    assert_eq!(policy.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    let client_report = client.shutdown_with_report(Duration::from_secs(1)).await;
    assert!(client_report.is_healthy());
    let _ = shutdown_tx.send(());
    let report = server_task.await.unwrap().expect("server report");
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn run_shutdown_cancels_connection_before_tls_peek_completes() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let addr = listener.local_addr().expect("listener should have local addr");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_task = tokio::spawn(run_with_report(
        test_service_context("remoting-server-tls-peek-test"),
        listener,
        async {
            let _ = shutdown_rx.await;
        },
        DefaultRequestProcessor,
        None,
        Vec::new(),
        None,
    ));

    let client = TcpStream::connect(addr).await.expect("client should connect");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let _ = shutdown_tx.send(());
    let report = tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server should shut down even when a connection has not sent its first byte")
        .expect("server task should not panic")
        .expect("server should return shutdown report");
    drop(client);

    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn run_shutdown_delivers_disconnect_before_lifecycle_dispatcher_stops() {
    let context = RuntimeContext::from_current("remoting-server-channel-report-test");
    let service = context.service_context("remoting-server-channel-report");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let addr = listener.local_addr().expect("listener should have local addr");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (connected_tx, connected_rx) = oneshot::channel::<()>();
    let (disconnected_tx, disconnected_rx) = oneshot::channel::<()>();
    let channel_listener = std::sync::Arc::new(ConnectSignalListener {
        connected: std::sync::Mutex::new(Some(connected_tx)),
        disconnected: std::sync::Mutex::new(Some(disconnected_tx)),
    });
    let server_task = tokio::spawn(run_with_report_with_service_context(
        service,
        listener,
        async {
            let _ = shutdown_rx.await;
        },
        DefaultRequestProcessor,
        None,
        Vec::new(),
        Some(channel_listener),
    ));

    let mut client = TcpStream::connect(addr).await.expect("client should connect");
    client
        .write_all(&[0])
        .await
        .expect("client should send first byte for TLS/plaintext detection");
    tokio::time::timeout(Duration::from_secs(3), connected_rx)
        .await
        .expect("server should accept connection before timeout")
        .expect("connect signal should be sent");
    let _ = shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(3), disconnected_rx)
        .await
        .expect("disconnect event should be delivered before shutdown")
        .expect("disconnect signal should be sent");
    let report = tokio::time::timeout(Duration::from_secs(3), server_task)
        .await
        .expect("server should shut down before timeout")
        .expect("server task should not panic")
        .expect("server should return shutdown report");
    drop(client);

    assert!(report.is_healthy(), "{}", report.to_json());
    assert_eq!(report.leaked, 0, "{}", report.to_json());
    assert_eq!(report.detached_still_running, 0, "{}", report.to_json());
    assert!(report.remaining_tasks.is_empty(), "{}", report.to_json());
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn run_shutdown_report_includes_tls_reload_task() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let context = RuntimeContext::from_current("remoting-server-tls-report-test");
    let service = context.service_context("remoting-server-tls-report");
    let config = Arc::new(ServerConfig {
        tls_config: TlsConfig {
            test_mode_enable: true,
            server: TlsServerConfig {
                mode: TlsMode::Permissive,
                ..Default::default()
            },
            ..Default::default()
        },
        ..ServerConfig::default()
    });
    let mut server = TransportServer::new(config, service);
    let server_task = tokio::spawn(async move {
        server
            .try_serve_bound_listener_until(listener, DefaultRequestProcessor, None, None, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let _ = shutdown_tx.send(());
    let report = tokio::time::timeout(Duration::from_secs(3), server_task)
        .await
        .expect("server should shut down before timeout")
        .expect("server task should not panic")
        .expect("server should return shutdown report");

    assert!(report.is_healthy(), "{}", report.to_json());
    let tls_report = report
        .children
        .iter()
        .find(|child| child.name == "rocketmq-transport.tls")
        .expect("remoting shutdown report should include tls reload task group");
    assert!(tls_report.is_healthy(), "{}", tls_report.to_json());
    assert_eq!(tls_report.leaked, 0, "{}", tls_report.to_json());
}

#[tokio::test]
async fn v1_network_response_frames_complete_the_exact_session_owner() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind V1 correlation listener");
    let address = listener.local_addr().expect("V1 correlation listener address");
    let mut server = TransportServer::new(
        Arc::new(ServerConfig::default()),
        test_service_context("remoting-server-v1-correlation"),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (startup_tx, startup_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        server
            .try_serve_bound_listener_until_with_startup(
                listener,
                CorrelatingV1Processor,
                None,
                None,
                async {
                    let _ = shutdown_rx.await;
                },
                startup_tx,
            )
            .await
    });
    assert_eq!(
        startup_rx
            .await
            .expect("V1 startup channel")
            .expect("V1 startup succeeds"),
        address
    );

    let mut first = crate::connection::Connection::new(TcpStream::connect(address).await.expect("first V1 client"));
    let mut second = crate::connection::Connection::new(TcpStream::connect(address).await.expect("second V1 client"));
    first
        .send_command(RemotingCommand::create_remoting_command(8_800).set_opaque(41))
        .await
        .expect("first inbound request");
    second
        .send_command(RemotingCommand::create_remoting_command(8_800).set_opaque(41))
        .await
        .expect("second inbound request");

    let first_outbound = tokio::time::timeout(Duration::from_secs(1), first.receive_command())
        .await
        .expect("first outbound deadline")
        .expect("first client connected")
        .expect("first outbound request");
    let second_outbound = tokio::time::timeout(Duration::from_secs(1), second.receive_command())
        .await
        .expect("second outbound deadline")
        .expect("second client connected")
        .expect("second outbound request");
    assert_eq!((first_outbound.code(), first_outbound.opaque()), (8_801, 41));
    assert_eq!((second_outbound.code(), second_outbound.opaque()), (8_801, 41));

    first
        .send_command(
            RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                .set_opaque(41)
                .set_body(b"first-owner".to_vec()),
        )
        .await
        .expect("first correlated response");
    second
        .send_command(
            RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                .set_opaque(41)
                .set_body(b"second-owner".to_vec()),
        )
        .await
        .expect("second correlated response");

    let first_final = tokio::time::timeout(Duration::from_secs(1), first.receive_command())
        .await
        .expect("first final deadline")
        .expect("first connection")
        .expect("first final response");
    let second_final = tokio::time::timeout(Duration::from_secs(1), second.receive_command())
        .await
        .expect("second final deadline")
        .expect("second connection")
        .expect("second final response");
    assert_eq!(first_final.opaque(), 41);
    assert_eq!(first_final.body(), Some(&bytes::Bytes::from_static(b"first-owner")));
    assert_eq!(second_final.opaque(), 41);
    assert_eq!(second_final.body(), Some(&bytes::Bytes::from_static(b"second-owner")));

    first
        .send_command(RemotingCommand::create_remoting_command(8_800).set_opaque(51))
        .await
        .expect("first concurrent request");
    first
        .send_command(RemotingCommand::create_remoting_command(8_800).set_opaque(52))
        .await
        .expect("second concurrent request");
    let first_pending = tokio::time::timeout(Duration::from_secs(1), first.receive_command())
        .await
        .expect("first concurrent outbound deadline")
        .expect("first connection")
        .expect("first concurrent outbound");
    let second_pending = tokio::time::timeout(Duration::from_secs(1), first.receive_command())
        .await
        .expect("second concurrent outbound deadline")
        .expect("first connection")
        .expect("second concurrent outbound");
    assert_ne!(first_pending.opaque(), second_pending.opaque());
    for outbound in [first_pending, second_pending] {
        first
            .send_command(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                    .set_opaque(outbound.opaque())
                    .set_body(format!("owner-{}", outbound.opaque()).into_bytes()),
            )
            .await
            .expect("complete same-session pending request");
    }
    let mut completed = Vec::new();
    for _ in 0..2 {
        let response = tokio::time::timeout(Duration::from_secs(1), first.receive_command())
            .await
            .expect("same-session final deadline")
            .expect("first connection")
            .expect("same-session final response");
        completed.push((response.opaque(), response.body().cloned()));
    }
    completed.sort_by_key(|(opaque, _)| *opaque);
    assert_eq!(
        completed,
        vec![
            (51, Some(bytes::Bytes::from_static(b"owner-51"))),
            (52, Some(bytes::Bytes::from_static(b"owner-52"))),
        ]
    );

    let mut closing = crate::connection::Connection::new(TcpStream::connect(address).await.expect("closing V1 client"));
    closing
        .send_command(RemotingCommand::create_remoting_command(8_800).set_opaque(61))
        .await
        .expect("closing inbound request");
    let _ = tokio::time::timeout(Duration::from_secs(1), closing.receive_command())
        .await
        .expect("closing outbound deadline")
        .expect("closing connection")
        .expect("closing outbound request");
    drop(closing);

    let _ = shutdown_tx.send(());
    let report = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("owner close releases pending waiter before drain")
        .expect("join V1 correlation server")
        .expect("V1 correlation shutdown report");
    assert!(report.is_healthy(), "{}", report.to_json());
}
