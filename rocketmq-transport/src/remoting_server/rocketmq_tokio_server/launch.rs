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

use rocketmq_security_api::SecurityBootstrapProfile;

use super::capabilities::PreparedServer;
use super::capabilities::RemotingServerRunCapabilities;
use super::capabilities::ServerSecurityState;
use super::connection_handler::SessionCommandInterceptor;
use super::connection_listener::ConnectionListener;
use super::lifecycle_events::run_lifecycle_event_dispatcher;
use super::lifecycle_events::LifecycleEventPublisher;
use super::shutdown::new_remoting_server_context;
use super::shutdown::shutdown_remoting_server;
use super::*;

enum StartupNotifier {
    Checked(oneshot::Sender<Result<SocketAddr, ServerStartError>>),
    Compatibility(oneshot::Sender<RocketMQResult<SocketAddr>>),
}

fn notify_server_startup(startup: &mut Option<StartupNotifier>, result: Result<SocketAddr, ServerStartError>) {
    if let Some(startup) = startup.take() {
        match startup {
            StartupNotifier::Checked(sender) => {
                let _ = sender.send(result);
            }
            StartupNotifier::Compatibility(sender) => {
                let _ = sender.send(result.map_err(legacy_start_error));
            }
        }
    }
}

fn legacy_start_error(error: ServerStartError) -> RocketMQError {
    match error {
        ServerStartError::Configuration { source, .. } => legacy_configuration_error(source),
        ServerStartError::Bind { address, detail, .. } => {
            RocketMQError::network_connection_failed("remoting-server-bind", format!("{address}: {detail}"))
        }
        ServerStartError::LocalAddress { address, detail, .. } => {
            RocketMQError::network_connection_failed("remoting-server-local-address", format!("{address}: {detail}"))
        }
        ServerStartError::Tls { detail, .. } => RocketMQError::network_connection_failed("remoting-server-tls", detail),
        error => RocketMQError::network_connection_failed("remoting-server-startup", error.to_string()),
    }
}

fn legacy_configuration_error(source: SharedRocketMQError) -> RocketMQError {
    match source.as_error() {
        RocketMQError::ConfigInvalidValue { key, value, reason } => RocketMQError::ConfigInvalidValue {
            key,
            value: value.clone(),
            reason: reason.clone(),
        },
        _ => source.into_error(),
    }
}

fn compatibility_report(result: Result<ShutdownReport, ServerStartError>) -> Option<ShutdownReport> {
    match result {
        Ok(report) => Some(report),
        Err(error) => {
            error!(%error, "remoting server startup failed");
            None
        }
    }
}

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

    /// Serves an already-bound listener and returns a typed startup failure.
    ///
    /// # Errors
    ///
    /// Returns [`ServerStartError`] when listener inspection, server capability
    /// preparation, or lifecycle task creation fails before accepting connections.
    pub async fn try_serve_bound_listener_until<S>(
        &mut self,
        listener: TcpListener,
        request_processor: RP,
        conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
    ) -> Result<ShutdownReport, ServerStartError>
    where
        S: Future,
    {
        self.try_serve_bound_listener_until_inner(
            listener,
            request_processor,
            conn_disconnect_notify,
            channel_event_listener,
            shutdown,
            None,
        )
        .await
    }

    /// Serves an already-bound listener and publishes readiness only after all
    /// startup capabilities have been constructed.
    ///
    /// # Errors
    ///
    /// Returns [`ServerStartError`] and sends the same typed failure through
    /// `startup` when preparation fails. A successful readiness signal means
    /// the listener, TLS runtime, dispatcher, admission boundary, and optional
    /// lifecycle event task are ready to serve connections.
    pub async fn try_serve_bound_listener_until_with_startup<S>(
        &mut self,
        listener: TcpListener,
        request_processor: RP,
        conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
        startup: oneshot::Sender<Result<SocketAddr, ServerStartError>>,
    ) -> Result<ShutdownReport, ServerStartError>
    where
        S: Future,
    {
        self.try_serve_bound_listener_until_inner(
            listener,
            request_processor,
            conn_disconnect_notify,
            channel_event_listener,
            shutdown,
            Some(StartupNotifier::Checked(startup)),
        )
        .await
    }

    /// Compatibility wrapper for composition roots that still use `Option`.
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
        compatibility_report(
            self.try_serve_bound_listener_until(
                listener,
                request_processor,
                conn_disconnect_notify,
                channel_event_listener,
                shutdown,
            )
            .await,
        )
    }

    /// Runs a configuration-bound server and returns a typed startup failure.
    ///
    /// # Errors
    ///
    /// Returns [`ServerStartError`] when binding or any subsequent capability
    /// preparation step fails before the server can accept connections.
    pub async fn try_run_with_shutdown_report<S>(
        &mut self,
        request_processor: RP,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
    ) -> Result<ShutdownReport, ServerStartError>
    where
        S: Future,
    {
        self.try_run_with_shutdown_report_inner(request_processor, channel_event_listener, shutdown, None)
            .await
    }

    /// Runs a configuration-bound server and publishes readiness only after
    /// its runtime, security, admission, dispatcher, and lifecycle task are ready.
    ///
    /// # Errors
    ///
    /// Returns [`ServerStartError`] and sends the same typed failure through
    /// `startup` on failure. A successful readiness signal is emitted only
    /// after every fallible startup capability has completed.
    pub async fn try_run_with_shutdown_report_and_startup<S>(
        &mut self,
        request_processor: RP,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
        startup: oneshot::Sender<Result<SocketAddr, ServerStartError>>,
    ) -> Result<ShutdownReport, ServerStartError>
    where
        S: Future,
    {
        self.try_run_with_shutdown_report_inner(
            request_processor,
            channel_event_listener,
            shutdown,
            Some(StartupNotifier::Checked(startup)),
        )
        .await
    }

    /// Compatibility wrapper retaining the historic `Option` shutdown report.
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
        compatibility_report(
            self.try_run_with_shutdown_report(request_processor, channel_event_listener, shutdown)
                .await,
        )
    }

    /// Compatibility wrapper retaining the historic startup acknowledgement.
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
        compatibility_report(
            self.try_run_with_shutdown_report_inner(
                request_processor,
                channel_event_listener,
                shutdown,
                Some(StartupNotifier::Compatibility(startup)),
            )
            .await,
        )
    }

    async fn try_serve_bound_listener_until_inner<S>(
        &mut self,
        listener: TcpListener,
        request_processor: RP,
        conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
        mut startup: Option<StartupNotifier>,
    ) -> Result<ShutdownReport, ServerStartError>
    where
        S: Future,
    {
        let address = listener.local_addr().map_err(|error| ServerStartError::LocalAddress {
            stage: "listener.local_addr",
            address: "pre-bound listener".to_owned(),
            detail: error.to_string(),
        });
        let address = match address {
            Ok(address) => address,
            Err(error) => {
                notify_server_startup(&mut startup, Err(error.clone()));
                return Err(error);
            }
        };
        let prepared = self.prepare_server(request_processor, channel_event_listener).await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                notify_server_startup(&mut startup, Err(error.clone()));
                return Err(error);
            }
        };
        notify_server_startup(&mut startup, Ok(address));
        serve_prepared(listener, shutdown, conn_disconnect_notify, prepared).await
    }

    async fn try_run_with_shutdown_report_inner<S>(
        &mut self,
        request_processor: RP,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
        mut startup: Option<StartupNotifier>,
    ) -> Result<ShutdownReport, ServerStartError>
    where
        S: Future,
    {
        let address = format!("{}:{}", self.config.bind_address, self.config.listen_port);
        let listener = match TcpListener::bind(&address).await {
            Ok(listener) => listener,
            Err(error) => {
                let error = ServerStartError::Bind {
                    stage: "listener.bind",
                    address,
                    detail: error.to_string(),
                };
                notify_server_startup(&mut startup, Err(error.clone()));
                return Err(error);
            }
        };
        let local_address = match listener.local_addr() {
            Ok(local_address) => local_address,
            Err(error) => {
                let error = ServerStartError::LocalAddress {
                    stage: "listener.local_addr",
                    address,
                    detail: error.to_string(),
                };
                notify_server_startup(&mut startup, Err(error.clone()));
                return Err(error);
            }
        };
        let prepared = match self.prepare_server(request_processor, channel_event_listener).await {
            Ok(prepared) => prepared,
            Err(error) => {
                notify_server_startup(&mut startup, Err(error.clone()));
                return Err(error);
            }
        };
        info!(address = %local_address, "starting remoting server");
        notify_server_startup(&mut startup, Ok(local_address));
        let (conn_disconnect_notify, _) = broadcast::channel::<SocketAddr>(100);
        serve_prepared(listener, shutdown, Some(conn_disconnect_notify), prepared).await
    }

    pub(super) async fn prepare_server(
        &mut self,
        request_processor: RP,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
    ) -> Result<PreparedServer<RP>, ServerStartError> {
        let lifecycle_event_config =
            self.lifecycle_event_config
                .validate()
                .map_err(|error| ServerStartError::Configuration {
                    stage: "lifecycle_events",
                    detail: error.to_string(),
                    source: SharedRocketMQError::new(error),
                })?;
        self.frame_limits
            .validate()
            .map_err(|error| ServerStartError::Configuration {
                stage: "frame_limits",
                detail: error.to_string(),
                source: SharedRocketMQError::new(error),
            })?;
        self.proxy_protocol
            .validate()
            .map_err(|error| ServerStartError::Configuration {
                stage: "proxy_protocol",
                detail: error.to_string(),
                source: SharedRocketMQError::new(error),
            })?;

        let remoting_context = new_remoting_server_context(&self.service_context);
        let tls_runtime =
            TlsServerRuntime::initialize_with_service_context(self.config.tls_config.clone(), &remoting_context)
                .await
                .map_err(|error| ServerStartError::Tls {
                    stage: "tls.initialize",
                    detail: error.to_string(),
                })?;
        let cleanup_tls_runtime = tls_runtime.clone();
        let cleanup_task_group = remoting_context.task_group().clone();
        let rpc_hooks = self.rpc_hooks.take().unwrap_or_default();
        #[cfg(all(test, not(doctest)))]
        let command_interceptor: Arc<dyn SessionCommandInterceptor> = Arc::new(self.test_request_hook.clone());
        #[cfg(not(test))]
        let command_interceptor: Arc<dyn SessionCommandInterceptor> = Arc::new(());
        let capabilities = RemotingServerRunCapabilities {
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
        };
        match prepare_capabilities(
            request_processor,
            rpc_hooks,
            channel_event_listener,
            self.authorized_dispatcher.clone(),
            capabilities,
        ) {
            Ok(prepared) => Ok(prepared),
            Err(error) => {
                cleanup_failed_preparation(cleanup_tls_runtime, cleanup_task_group).await;
                Err(error)
            }
        }
    }
}

async fn cleanup_failed_preparation(tls_runtime: TlsServerRuntime, task_group: TaskGroup) {
    let deadline = task_group
        .shutdown_deadline()
        .unwrap_or_else(|| ShutdownDeadline::after(Duration::from_secs(30)));
    task_group.cancel();
    if let Some(report) = tls_runtime
        .shutdown_gracefully(deadline.remaining().min(Duration::from_secs(3)))
        .await
    {
        report.log_if_unhealthy();
    }
    task_group.shutdown_until(deadline).await.log_if_unhealthy();
}

fn prepare_capabilities<RP: RequestProcessor + Sync + 'static + Clone>(
    request_processor: RP,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
    authorized_dispatcher: Option<Arc<AuthorizedCommandDispatcher<RP>>>,
    capabilities: RemotingServerRunCapabilities,
) -> Result<PreparedServer<RP>, ServerStartError> {
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
            None => Arc::new(
                AdmissionController::try_new_with_budget(admission_limits, &process_budget).map_err(|error| {
                    ServerStartError::Admission {
                        stage: "admission.initialize",
                        detail: error.to_string(),
                    }
                })?,
            ),
        },
    };
    let (dispatcher, security_state) = match authorized_dispatcher {
        Some(dispatcher) => {
            let state = security_state(dispatcher.boundary().security_profile());
            (dispatcher, state)
        }
        None => {
            let (security, state) = match transport_security {
                Some(security) => {
                    let state = security_state(security.profile());
                    (security, state)
                }
                None => {
                    warn!("remoting server has no security profile; using legacy development-insecure fallback");
                    (
                        Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
                        ServerSecurityState::Unconfigured,
                    )
                }
            };
            let dispatcher = AuthorizedCommandDispatcher::try_new(
                request_processor,
                rpc_hooks,
                &process_budget,
                telemetry.clone(),
                security,
                admission,
            )
            .map_err(|error| ServerStartError::Dispatcher {
                stage: "dispatcher.initialize",
                detail: error.to_string(),
            })?;
            (Arc::new(dispatcher), state)
        }
    };
    let lifecycle_shutdown = CancellationToken::new();
    let (event_publisher, lifecycle_dispatcher_task) = prepare_lifecycle_event_dispatcher(
        channel_event_listener,
        &task_group,
        lifecycle_event_config,
        lifecycle_shutdown.clone(),
        telemetry.clone(),
    )?;
    info!(?security_state, "remoting server startup capabilities prepared");
    Ok(PreparedServer {
        dispatcher,
        capabilities: RemotingServerRunCapabilities {
            tls_runtime,
            task_group,
            file_region_blocking,
            file_transfer_mode,
            frame_limits,
            process_budget,
            transport_security: None,
            transport_principal,
            admission: None,
            command_interceptor,
            telemetry,
            lifecycle_event_config,
            proxy_protocol,
        },
        event_publisher,
        lifecycle_shutdown,
        lifecycle_dispatcher_task,
        security_state,
    })
}

fn security_state(profile: SecurityBootstrapProfile) -> ServerSecurityState {
    match profile {
        SecurityBootstrapProfile::DevelopmentInsecureLoopback => ServerSecurityState::ExplicitInsecureLoopback,
        SecurityBootstrapProfile::SecureEnforced => ServerSecurityState::Secure,
    }
}

fn prepare_lifecycle_event_dispatcher(
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
    task_group: &TaskGroup,
    lifecycle_event_config: LifecycleEventConfig,
    lifecycle_shutdown: CancellationToken,
    telemetry: TransportTelemetry,
) -> Result<(Option<LifecycleEventPublisher>, Option<TaskId>), ServerStartError> {
    let Some(listener) = channel_event_listener else {
        return Ok((None, None));
    };
    let (sender, receiver) = mpsc::channel(lifecycle_event_config.queue_capacity);
    let publisher = LifecycleEventPublisher {
        sender,
        publish_timeout: lifecycle_event_config.publish_timeout,
        cancellation: lifecycle_shutdown.clone(),
        telemetry: telemetry.clone(),
    };
    let task_id = task_group
        .spawn_service(
            "rocketmq.remoting.event_dispatcher",
            run_lifecycle_event_dispatcher(
                receiver,
                listener,
                lifecycle_shutdown,
                lifecycle_event_config,
                telemetry,
            ),
        )
        .map_err(|error| ServerStartError::TaskSpawn {
            stage: "lifecycle_event_dispatcher.spawn",
            detail: error.to_string(),
        })?;
    Ok((Some(publisher), Some(task_id)))
}

async fn serve_prepared<RP: RequestProcessor + Sync + 'static + Clone>(
    listener: TcpListener,
    shutdown: impl Future,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    prepared: PreparedServer<RP>,
) -> Result<ShutdownReport, ServerStartError> {
    let PreparedServer {
        dispatcher,
        capabilities,
        event_publisher,
        lifecycle_shutdown,
        lifecycle_dispatcher_task,
        security_state,
    } = prepared;
    debug_assert!(matches!(
        security_state,
        ServerSecurityState::Unconfigured | ServerSecurityState::ExplicitInsecureLoopback | ServerSecurityState::Secure
    ));
    let RemotingServerRunCapabilities {
        tls_runtime,
        task_group,
        file_region_blocking,
        file_transfer_mode,
        frame_limits,
        process_budget: _,
        transport_security: _,
        transport_principal,
        admission: _,
        command_interceptor,
        telemetry,
        lifecycle_event_config: _,
        proxy_protocol,
    } = capabilities;
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    let mut listener = ConnectionListener {
        listener: Some(listener),
        shutdown_complete_tx,
        conn_disconnect_notify,
        event_publisher,
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
        lifecycle_dispatcher_task,
    };

    tokio::select! {
        res = listener.run() => {
            if let Err(error) = res {
                error!(cause = %error, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("Shutdown now.....");
        }
    }

    Ok(shutdown_remoting_server(listener, task_group, lifecycle_shutdown, &mut shutdown_complete_rx).await)
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
    let mut server = TransportServer::new(Arc::new(ServerConfig::default()), service_context);
    for hook in rpc_hooks {
        server.register_rpc_hook(hook);
    }
    server
        .serve_bound_listener_until(
            listener,
            request_processor,
            conn_disconnect_notify,
            channel_event_listener,
            shutdown,
        )
        .await
}
