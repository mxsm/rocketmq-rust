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

use super::capabilities::RemotingServerRunCapabilities;
use super::connection_handler::SessionCommandInterceptor;
use super::connection_listener::ConnectionListener;
use super::shutdown::new_remoting_server_context;
use super::shutdown::shutdown_remoting_server;
use super::*;

impl<RP: RequestProcessor + Sync + 'static + Clone> TransportServer<RP> {
    pub async fn run(&mut self, request_processor: RP, channel_event_listener: Option<Arc<dyn ChannelEventListener>>) {
        self.run_with_shutdown(request_processor, channel_event_listener, wait_for_signal())
            .await;
    }

    pub async fn run_with_shutdown<S>(
        &mut self,
        request_processor: RP,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
    ) where
        S: Future,
    {
        let _ = self
            .run_with_shutdown_report(request_processor, channel_event_listener, shutdown)
            .await;
    }

    /// Serves an already-bound listener through the canonical server runtime.
    ///
    /// This entry point is intended for composition roots that must bind the
    /// socket themselves in order to publish an exact readiness transition.
    pub async fn serve_bound_listener_until<S>(
        &mut self,
        listener: TcpListener,
        request_processor: RP,
        conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
    ) -> Option<ShutdownReport>
    where
        S: Future,
    {
        let lifecycle_event_config = match self.lifecycle_event_config.validate() {
            Ok(config) => config,
            Err(error) => {
                error!(%error, "invalid remoting lifecycle event configuration");
                return None;
            }
        };
        let rpc_hooks = self.rpc_hooks.take().unwrap_or_default();
        let remoting_context = new_remoting_server_context(&self.service_context);
        let tls_runtime =
            match TlsServerRuntime::initialize_with_service_context(self.config.tls_config.clone(), &remoting_context)
                .await
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    error!(%error, "failed to initialize remoting server TLS runtime");
                    return None;
                }
            };
        #[cfg(all(test, not(doctest)))]
        let command_interceptor: Arc<dyn SessionCommandInterceptor> = Arc::new(self.test_request_hook.clone());
        #[cfg(not(test))]
        let command_interceptor: Arc<dyn SessionCommandInterceptor> = Arc::new(());
        run_with_tls_config_report(
            listener,
            shutdown,
            request_processor,
            conn_disconnect_notify,
            rpc_hooks,
            channel_event_listener,
            self.authorized_dispatcher.clone(),
            RemotingServerRunCapabilities {
                tls_runtime,
                task_group: remoting_context.task_group().clone(),
                file_region_blocking: remoting_context.storage_io().clone(),
                file_transfer_mode: self.config.file_transfer_mode,
                frame_limits: self.frame_limits,
                process_budget: self.service_context.process_budget(),
                transport_security: self.transport_security.clone(),
                transport_principal: self.transport_principal.clone(),
                admission: self.admission.clone(),
                command_interceptor,
                telemetry: self.telemetry.clone(),
                lifecycle_event_config,
                proxy_protocol: self.proxy_protocol.clone(),
            },
        )
        .await
    }

    #[doc(hidden)]
    pub async fn run_with_shutdown_report<S>(
        &mut self,
        request_processor: RP,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
    ) -> Option<ShutdownReport>
    where
        S: Future,
    {
        self.run_with_shutdown_report_inner(request_processor, channel_event_listener, shutdown, None)
            .await
    }

    /// Runs the server and reports whether its listener is ready before entering the accept loop.
    ///
    /// The startup signal is sent only after the socket is bound and the remoting runtime and TLS
    /// state have been initialized. This prevents lifecycle owners from treating a spawned server
    /// task as a bound, production-ready listener.
    #[doc(hidden)]
    pub async fn run_with_shutdown_report_and_startup<S>(
        &mut self,
        request_processor: RP,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
        startup: oneshot::Sender<RocketMQResult<SocketAddr>>,
    ) -> Option<ShutdownReport>
    where
        S: Future,
    {
        self.run_with_shutdown_report_inner(request_processor, channel_event_listener, shutdown, Some(startup))
            .await
    }

    async fn run_with_shutdown_report_inner<S>(
        &mut self,
        request_processor: RP,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
        mut startup: Option<oneshot::Sender<RocketMQResult<SocketAddr>>>,
    ) -> Option<ShutdownReport>
    where
        S: Future,
    {
        let lifecycle_event_config = match self.lifecycle_event_config.validate() {
            Ok(config) => config,
            Err(error) => {
                error!(%error, "Invalid remoting lifecycle event configuration");
                notify_server_startup(&mut startup, Err(error));
                return None;
            }
        };
        let addr = format!("{}:{}", self.config.bind_address, self.config.listen_port);
        let listener = match TcpListener::bind(&addr).await {
            Ok(listener) => listener,
            Err(err) => {
                error!(addr = %addr, error = %err, "failed to bind remoting_server");
                notify_server_startup(
                    &mut startup,
                    Err(RocketMQError::network_connection_failed(
                        "remoting-server-bind",
                        format!("{addr}: {err}"),
                    )),
                );
                return None;
            }
        };
        let local_addr = match listener.local_addr() {
            Ok(local_addr) => local_addr,
            Err(error) => {
                error!(addr = %addr, %error, "failed to read bound remoting server address");
                notify_server_startup(
                    &mut startup,
                    Err(RocketMQError::network_connection_failed(
                        "remoting-server-local-address",
                        format!("{addr}: {error}"),
                    )),
                );
                return None;
            }
        };
        let rpc_hooks = self.rpc_hooks.take().unwrap_or_default();
        let remoting_context = new_remoting_server_context(&self.service_context);
        let task_group = remoting_context.task_group().clone();
        let tls_runtime =
            match TlsServerRuntime::initialize_with_service_context(self.config.tls_config.clone(), &remoting_context)
                .await
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    error!(%error, "failed to initialize remoting server TLS runtime");
                    notify_server_startup(
                        &mut startup,
                        Err(RocketMQError::network_connection_failed(
                            "remoting-server-tls",
                            error.to_string(),
                        )),
                    );
                    return None;
                }
            };
        info!("Starting remoting_server at: {}", addr);
        notify_server_startup(&mut startup, Ok(local_addr));
        let (notify_conn_disconnect, _) = broadcast::channel::<SocketAddr>(100);
        #[cfg(all(test, not(doctest)))]
        let command_interceptor: Arc<dyn SessionCommandInterceptor> = Arc::new(self.test_request_hook.clone());
        #[cfg(not(test))]
        let command_interceptor: Arc<dyn SessionCommandInterceptor> = Arc::new(());
        run_with_tls_config_report(
            listener,
            shutdown,
            request_processor,
            Some(notify_conn_disconnect),
            rpc_hooks,
            channel_event_listener,
            self.authorized_dispatcher.clone(),
            RemotingServerRunCapabilities {
                tls_runtime,
                task_group,
                file_region_blocking: remoting_context.storage_io().clone(),
                file_transfer_mode: self.config.file_transfer_mode,
                frame_limits: self.frame_limits,
                process_budget: self.service_context.process_budget(),
                transport_security: self.transport_security.clone(),
                transport_principal: self.transport_principal.clone(),
                admission: self.admission.clone(),
                command_interceptor,
                telemetry: self.telemetry.clone(),
                lifecycle_event_config,
                proxy_protocol: self.proxy_protocol.clone(),
            },
        )
        .await
    }
}

fn notify_server_startup(
    startup: &mut Option<oneshot::Sender<RocketMQResult<SocketAddr>>>,
    result: RocketMQResult<SocketAddr>,
) {
    if let Some(startup) = startup.take() {
        let _ = startup.send(result);
    }
}

#[cfg(test)]
pub(super) async fn run_with_report<RP: RequestProcessor + Sync + 'static + Clone>(
    service_context: ChildServiceContext,
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
) -> Option<ShutdownReport> {
    run_with_report_with_service_context(
        service_context,
        listener,
        shutdown,
        request_processor,
        conn_disconnect_notify,
        rpc_hooks,
        channel_event_listener,
    )
    .await
}

#[cfg(test)]
pub(super) async fn run_with_report_with_service_context<RP: RequestProcessor + Sync + 'static + Clone>(
    service_context: ChildServiceContext,
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
) -> Option<ShutdownReport> {
    run_with_report_with_service_context_and_telemetry(
        service_context,
        listener,
        shutdown,
        request_processor,
        conn_disconnect_notify,
        rpc_hooks,
        channel_event_listener,
        TransportTelemetry::noop(),
    )
    .await
}

/// Runs a remoting server under an explicit service context and transport telemetry instance.
///
/// The supplied telemetry capability is propagated to accepted connections, derived channels,
/// request metrics guards, and request tracing spans.
// These arguments are independent composition capabilities owned by the remoting server runtime.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
async fn run_with_report_with_service_context_and_telemetry<RP: RequestProcessor + Sync + 'static + Clone>(
    service_context: ChildServiceContext,
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
    telemetry: TransportTelemetry,
) -> Option<ShutdownReport> {
    let remoting_context = new_remoting_server_context(&service_context);
    let tls_runtime =
        match TlsServerRuntime::initialize_with_service_context(Default::default(), &remoting_context).await {
            Ok(runtime) => runtime,
            Err(error) => {
                error!(%error, "failed to initialize remoting server TLS runtime");
                return None;
            }
        };
    run_with_tls_config_report(
        listener,
        shutdown,
        request_processor,
        conn_disconnect_notify,
        rpc_hooks,
        channel_event_listener,
        None,
        RemotingServerRunCapabilities {
            tls_runtime,
            task_group: remoting_context.task_group().clone(),
            file_region_blocking: remoting_context.storage_io().clone(),
            file_transfer_mode: FileTransferMode::Auto,
            frame_limits: FrameLimits::java_compatibility(),
            process_budget: service_context.process_budget(),
            transport_security: None,
            transport_principal: None,
            admission: None,
            command_interceptor: Arc::new(()),
            telemetry,
            lifecycle_event_config: LifecycleEventConfig::default(),
            proxy_protocol: ProxyProtocolConfig::default(),
        },
    )
    .await
}

pub(super) async fn run_with_tls_config_report<RP: RequestProcessor + Sync + 'static + Clone>(
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
    authorized_dispatcher: Option<Arc<AuthorizedCommandDispatcher<RP>>>,
    capabilities: RemotingServerRunCapabilities,
) -> Option<ShutdownReport> {
    let RemotingServerRunCapabilities {
        tls_runtime,
        task_group,
        file_region_blocking,
        file_transfer_mode,
        frame_limits,
        process_budget,
        transport_security,
        transport_principal,
        admission,
        command_interceptor,
        telemetry,
        lifecycle_event_config,
        proxy_protocol,
    } = capabilities;
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    let lifecycle_shutdown = CancellationToken::new();
    let mut admission_limits = AdmissionLimits::default();
    admission_limits.connections = ResourceLimit {
        count: DEFAULT_MAX_CONNECTIONS,
        ..admission_limits.connections
    };
    admission_limits.handshakes = ResourceLimit {
        count: DEFAULT_MAX_CONNECTIONS,
        ..admission_limits.handshakes
    };
    let admission = match authorized_dispatcher.as_ref() {
        Some(dispatcher) => dispatcher.boundary().admission_controller(),
        None => match admission {
            Some(admission) => admission,
            None => match AdmissionController::try_new_with_budget(admission_limits, &process_budget) {
                Ok(admission) => Arc::new(admission),
                Err(error) => {
                    error!(%error, "failed to initialize transport admission budgets");
                    return None;
                }
            },
        },
    };
    let dispatcher = match authorized_dispatcher {
        Some(dispatcher) => dispatcher,
        None => {
            let security = transport_security
                .unwrap_or_else(|| Arc::new(TransportSecurity::development_insecure_loopback(None, None)));
            match AuthorizedCommandDispatcher::try_new(
                request_processor,
                rpc_hooks,
                &process_budget,
                telemetry.clone(),
                security,
                admission,
            ) {
                Ok(dispatcher) => Arc::new(dispatcher),
                Err(error) => {
                    error!(%error, "failed to initialize authorized command dispatcher");
                    return None;
                }
            }
        }
    };
    let mut listener = ConnectionListener {
        listener: Some(listener),
        shutdown_complete_tx,
        conn_disconnect_notify,
        channel_event_listener,
        dispatcher,
        tls_runtime,
        task_group: task_group.clone(),
        file_region_blocking,
        file_transfer_mode,
        frame_limits,
        proxy_protocol,
        transport_principal,
        command_interceptor,
        telemetry,
        lifecycle_event_config,
        lifecycle_shutdown: lifecycle_shutdown.clone(),
        lifecycle_dispatcher_task: None,
    };

    tokio::select! {
        res = listener.run() => {
            // If an error is received here, accepting connections from the TCP
            // listener failed multiple times and the remoting_server is giving up and
            // shutting down.
            //
            // Errors encountered when handling individual connections do not
            // bubble up to this point.
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("Shutdown now.....");
        }
    }

    Some(shutdown_remoting_server(listener, task_group, lifecycle_shutdown, &mut shutdown_complete_rx).await)
}
