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

use super::connection_handler::V2ConnectionHandler;
use super::shutdown::new_remoting_server_context;
use super::*;
use crate::dispatch::AuthorizedCommandDispatcherV2;
use crate::runtime::processor_v2::RequestProcessorV2;

/// Network server for the explicit V2 request-processor contract.
///
/// The server owns one statically selected V2 route and consumes itself at
/// startup so capability preparation and hook installation can happen once.
pub struct TransportServerV2<P> {
    config: Arc<ServerConfig>,
    service_context: ChildServiceContext,
    request_processor: Option<P>,
    dispatcher: Option<Arc<AuthorizedCommandDispatcherV2<P>>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    transport_security: Option<Arc<TransportSecurity>>,
    transport_principal: Option<Principal>,
    admission: Option<Arc<AdmissionController>>,
    session_registry: Option<Arc<crate::v2_session_registry::V2SessionRegistry>>,
    telemetry: TransportTelemetry,
    frame_limits: FrameLimits,
    proxy_protocol: ProxyProtocolConfig,
    #[cfg(test)]
    write_preflight_barrier: Option<crate::write_strategy::WritePreflightBarrier>,
    #[cfg(test)]
    test_request_deadline: Option<Duration>,
}

struct PreparedV2Server<P> {
    dispatcher: Arc<AuthorizedCommandDispatcherV2<P>>,
    tls_runtime: TlsServerRuntime,
    task_group: TaskGroup,
    file_region_blocking: BlockingExecutor,
    file_transfer_mode: FileTransferMode,
    transport_principal: Option<Principal>,
    telemetry: TransportTelemetry,
    frame_limits: FrameLimits,
    proxy_protocol: ProxyProtocolConfig,
    session_registry: Option<Arc<crate::v2_session_registry::V2SessionRegistry>>,
    #[cfg(test)]
    write_preflight_barrier: Option<crate::write_strategy::WritePreflightBarrier>,
    #[cfg(test)]
    test_request_deadline: Option<Duration>,
}

impl<P> TransportServerV2<P>
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    /// Creates a V2 server that will move `request_processor` into its dispatcher at startup.
    #[must_use]
    pub fn new(config: Arc<ServerConfig>, service_context: ChildServiceContext, request_processor: P) -> Self {
        Self {
            config,
            service_context,
            request_processor: Some(request_processor),
            dispatcher: None,
            rpc_hooks: Vec::new(),
            transport_security: None,
            transport_principal: None,
            admission: None,
            session_registry: None,
            telemetry: TransportTelemetry::noop(),
            frame_limits: FrameLimits::java_compatibility(),
            proxy_protocol: ProxyProtocolConfig::default(),
            #[cfg(test)]
            write_preflight_barrier: None,
            #[cfg(test)]
            test_request_deadline: None,
        }
    }

    /// Creates a V2 server with an explicit lifecycle owner.
    #[must_use]
    pub fn new_with_service_context(
        config: Arc<ServerConfig>,
        service_context: ChildServiceContext,
        request_processor: P,
    ) -> Self {
        Self::new(config, service_context, request_processor)
    }

    /// Creates a V2 server bound to one transport recorder.
    #[must_use]
    pub fn new_with_telemetry(
        config: Arc<ServerConfig>,
        service_context: ChildServiceContext,
        request_processor: P,
        telemetry: TransportTelemetry,
    ) -> Self {
        Self {
            telemetry,
            ..Self::new(config, service_context, request_processor)
        }
    }

    /// Creates a V2 server using an already-authorized dispatcher.
    #[must_use]
    pub fn new_with_authorized_dispatcher(
        config: Arc<ServerConfig>,
        service_context: ChildServiceContext,
        dispatcher: Arc<AuthorizedCommandDispatcherV2<P>>,
    ) -> Self {
        Self {
            config,
            service_context,
            request_processor: None,
            dispatcher: Some(dispatcher),
            rpc_hooks: Vec::new(),
            transport_security: None,
            transport_principal: None,
            admission: None,
            session_registry: None,
            telemetry: TransportTelemetry::noop(),
            frame_limits: FrameLimits::java_compatibility(),
            proxy_protocol: ProxyProtocolConfig::default(),
            #[cfg(test)]
            write_preflight_barrier: None,
            #[cfg(test)]
            test_request_deadline: None,
        }
    }

    /// Replaces the no-op transport recorder before startup.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: TransportTelemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Applies one validated frame profile to every accepted connection.
    ///
    /// # Errors
    ///
    /// Returns an error if `frame_limits` is not a valid transport profile.
    pub fn try_with_frame_limits(mut self, frame_limits: FrameLimits) -> RocketMQResult<Self> {
        frame_limits.validate()?;
        self.frame_limits = frame_limits;
        Ok(self)
    }

    /// Enables trusted PROXY v1/v2 negotiation before TLS and Remoting decoding.
    ///
    /// # Errors
    ///
    /// Returns an error if the PROXY protocol policy is inconsistent.
    pub fn try_with_proxy_protocol(mut self, config: ProxyProtocolConfig) -> RocketMQResult<Self> {
        config.validate()?;
        self.proxy_protocol = config;
        Ok(self)
    }

    /// Appends one hook to the server's ordered startup contribution.
    pub fn register_rpc_hook(&mut self, hook: Arc<dyn RPCHook>) {
        self.rpc_hooks.push(hook);
    }

    /// Installs transport authorization for accepted sessions.
    #[must_use]
    pub fn with_transport_security(mut self, security: Arc<TransportSecurity>, principal: Option<Principal>) -> Self {
        self.transport_security = Some(security);
        self.transport_principal = principal;
        self
    }

    #[doc(hidden)]
    #[must_use]
    pub fn with_admission_controller(mut self, admission: Arc<AdmissionController>) -> Self {
        self.admission = Some(admission);
        self
    }

    /// Publishes V2 session lifecycle into a composition-owned registry.
    #[must_use]
    pub fn with_session_registry(mut self, registry: Arc<crate::v2_session_registry::V2SessionRegistry>) -> Self {
        self.session_registry = Some(registry);
        self
    }

    /// Selects an existing dispatcher and immediately releases any automatic processor source.
    #[must_use]
    pub fn with_authorized_dispatcher(mut self, dispatcher: Arc<AuthorizedCommandDispatcherV2<P>>) -> Self {
        drop(self.request_processor.take());
        self.dispatcher = Some(dispatcher);
        self
    }

    #[cfg(test)]
    fn with_write_preflight_barrier(mut self, barrier: crate::write_strategy::WritePreflightBarrier) -> Self {
        self.write_preflight_barrier = Some(barrier);
        self
    }

    #[cfg(test)]
    fn with_test_request_deadline(mut self, deadline: Duration) -> Self {
        self.test_request_deadline = Some(deadline);
        self
    }

    /// Binds the configured address and serves until `shutdown` resolves.
    ///
    /// # Errors
    ///
    /// Returns a typed startup error for binding, capability, or lifecycle failures.
    pub async fn try_run_with_shutdown_report<S>(self, shutdown: S) -> Result<ShutdownReport, ServerStartError>
    where
        S: Future,
    {
        self.try_run_with_shutdown_report_inner(shutdown, None).await
    }

    /// Binds and publishes readiness after every startup capability is prepared.
    ///
    /// # Errors
    ///
    /// Returns and reports the same typed startup failure through `startup`.
    pub async fn try_run_with_shutdown_report_and_startup<S>(
        self,
        shutdown: S,
        startup: oneshot::Sender<Result<SocketAddr, ServerStartError>>,
    ) -> Result<ShutdownReport, ServerStartError>
    where
        S: Future,
    {
        self.try_run_with_shutdown_report_inner(shutdown, Some(startup)).await
    }

    /// Serves an already-bound listener until `shutdown` resolves.
    ///
    /// # Errors
    ///
    /// Returns a typed startup error for listener inspection or capability preparation.
    pub async fn try_serve_bound_listener_until<S>(
        self,
        listener: TcpListener,
        conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
        shutdown: S,
    ) -> Result<ShutdownReport, ServerStartError>
    where
        S: Future,
    {
        self.try_serve_bound_listener_until_inner(listener, conn_disconnect_notify, shutdown, None)
            .await
    }

    /// Serves a bound listener and publishes readiness after capability preparation.
    ///
    /// # Errors
    ///
    /// Returns and reports the same typed startup failure through `startup`.
    pub async fn try_serve_bound_listener_until_with_startup<S>(
        self,
        listener: TcpListener,
        conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
        shutdown: S,
        startup: oneshot::Sender<Result<SocketAddr, ServerStartError>>,
    ) -> Result<ShutdownReport, ServerStartError>
    where
        S: Future,
    {
        self.try_serve_bound_listener_until_inner(listener, conn_disconnect_notify, shutdown, Some(startup))
            .await
    }

    async fn try_run_with_shutdown_report_inner<S>(
        self,
        shutdown: S,
        mut startup: Option<oneshot::Sender<Result<SocketAddr, ServerStartError>>>,
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
                notify_startup(&mut startup, Err(error.clone()));
                return Err(error);
            }
        };
        let local_address = match listener.local_addr() {
            Ok(address) => address,
            Err(error) => {
                let error = ServerStartError::LocalAddress {
                    stage: "listener.local_addr",
                    address,
                    detail: error.to_string(),
                };
                notify_startup(&mut startup, Err(error.clone()));
                return Err(error);
            }
        };
        let prepared = match self.prepare().await {
            Ok(prepared) => prepared,
            Err(error) => {
                notify_startup(&mut startup, Err(error.clone()));
                return Err(error);
            }
        };
        notify_startup(&mut startup, Ok(local_address));
        serve_v2(listener, shutdown, Some(broadcast::channel(100).0), prepared).await
    }

    async fn try_serve_bound_listener_until_inner<S>(
        self,
        listener: TcpListener,
        conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
        shutdown: S,
        mut startup: Option<oneshot::Sender<Result<SocketAddr, ServerStartError>>>,
    ) -> Result<ShutdownReport, ServerStartError>
    where
        S: Future,
    {
        let address = match listener.local_addr() {
            Ok(address) => address,
            Err(error) => {
                let error = ServerStartError::LocalAddress {
                    stage: "listener.local_addr",
                    address: "pre-bound listener".to_owned(),
                    detail: error.to_string(),
                };
                notify_startup(&mut startup, Err(error.clone()));
                return Err(error);
            }
        };
        let prepared = match self.prepare().await {
            Ok(prepared) => prepared,
            Err(error) => {
                notify_startup(&mut startup, Err(error.clone()));
                return Err(error);
            }
        };
        notify_startup(&mut startup, Ok(address));
        serve_v2(listener, shutdown, conn_disconnect_notify, prepared).await
    }

    async fn prepare(mut self) -> Result<PreparedV2Server<P>, ServerStartError> {
        self.frame_limits
            .validate()
            .map_err(|error| configuration_error("frame_limits", error))?;
        self.proxy_protocol
            .validate()
            .map_err(|error| configuration_error("proxy_protocol", error))?;

        let injected = self.dispatcher.as_ref();
        if let (Some(dispatcher), Some(security)) = (injected, self.transport_security.as_ref()) {
            if !dispatcher.boundary().has_security_owner(security) {
                return Err(configuration_message(
                    "authorized_dispatcher.security",
                    "configured security does not own the injected dispatcher boundary",
                ));
            }
        }
        if let (Some(dispatcher), Some(admission)) = (injected, self.admission.as_ref()) {
            if !dispatcher.boundary().has_admission_owner(admission) {
                return Err(configuration_message(
                    "authorized_dispatcher.admission",
                    "configured admission does not own the injected dispatcher boundary",
                ));
            }
        }
        if self.dispatcher.is_none() && self.request_processor.is_none() {
            return Err(configuration_message(
                "request_processor",
                "automatic V2 startup has no processor source",
            ));
        }

        let admission = match (self.dispatcher.as_ref(), self.admission.take()) {
            (Some(dispatcher), _) => dispatcher.boundary().admission_controller(),
            (None, Some(admission)) => admission,
            (None, None) => {
                let mut limits = AdmissionLimits::default();
                limits.connections = ResourceLimit {
                    count: DEFAULT_MAX_CONNECTIONS,
                    ..limits.connections
                };
                limits.handshakes = ResourceLimit {
                    count: DEFAULT_MAX_CONNECTIONS,
                    ..limits.handshakes
                };
                Arc::new(
                    AdmissionController::try_new_with_budget(limits, &self.service_context.process_budget()).map_err(
                        |error| ServerStartError::Admission {
                            stage: "admission.initialize",
                            detail: error.to_string(),
                        },
                    )?,
                )
            }
        };
        let security = match (self.dispatcher.as_ref(), self.transport_security.take()) {
            (Some(dispatcher), _) => dispatcher.boundary().security_owner(),
            (None, Some(security)) => security,
            (None, None) => {
                warn!("V2 remoting server has no security profile; using development-insecure fallback");
                Arc::new(TransportSecurity::development_insecure_loopback(None, None))
            }
        };
        let dispatcher = match self.dispatcher.take() {
            Some(dispatcher) => {
                drop(self.request_processor.take());
                dispatcher
            }
            None => {
                let Some(processor) = self.request_processor.take() else {
                    return Err(configuration_message(
                        "request_processor",
                        "automatic V2 startup has no processor source",
                    ));
                };
                Arc::new(AuthorizedCommandDispatcherV2::new(
                    processor,
                    Vec::new(),
                    security,
                    admission,
                ))
            }
        };

        let remoting_context = new_remoting_server_context(&self.service_context);
        let tls_runtime =
            TlsServerRuntime::initialize_with_service_context(self.config.tls_config.clone(), &remoting_context)
                .await
                .map_err(|error| ServerStartError::Tls {
                    stage: "tls.initialize",
                    detail: error.to_string(),
                })?;

        // Hook mutation is deliberately the final preparation step. Failed
        // validation and boundary conflicts cannot contaminate a shared dispatcher.
        for hook in self.rpc_hooks {
            dispatcher.register_rpc_hook(hook);
        }

        Ok(PreparedV2Server {
            dispatcher,
            tls_runtime,
            task_group: remoting_context.task_group().clone(),
            file_region_blocking: remoting_context.storage_io().clone(),
            file_transfer_mode: self.config.file_transfer_mode,
            transport_principal: self.transport_principal,
            telemetry: self.telemetry,
            frame_limits: self.frame_limits,
            proxy_protocol: self.proxy_protocol,
            session_registry: self.session_registry,
            #[cfg(test)]
            write_preflight_barrier: self.write_preflight_barrier,
            #[cfg(test)]
            test_request_deadline: self.test_request_deadline,
        })
    }
}

fn notify_startup(
    startup: &mut Option<oneshot::Sender<Result<SocketAddr, ServerStartError>>>,
    result: Result<SocketAddr, ServerStartError>,
) {
    if let Some(startup) = startup.take() {
        let _ = startup.send(result);
    }
}

fn configuration_error(stage: &'static str, error: RocketMQError) -> ServerStartError {
    ServerStartError::Configuration {
        stage,
        detail: error.to_string(),
        source: SharedRocketMQError::new(error),
    }
}

fn configuration_message(stage: &'static str, detail: &'static str) -> ServerStartError {
    configuration_error(
        stage,
        RocketMQError::ConfigInvalidValue {
            key: stage,
            value: "conflict".to_owned(),
            reason: detail.to_owned(),
        },
    )
}

async fn serve_v2<P>(
    listener: TcpListener,
    shutdown: impl Future,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    prepared: PreparedV2Server<P>,
) -> Result<ShutdownReport, ServerStartError>
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    let PreparedV2Server {
        dispatcher,
        tls_runtime,
        task_group,
        file_region_blocking,
        file_transfer_mode,
        transport_principal,
        telemetry,
        frame_limits,
        proxy_protocol,
        session_registry,
        #[cfg(test)]
        write_preflight_barrier,
        #[cfg(test)]
        test_request_deadline,
    } = prepared;
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    let route = Arc::new(V2ConnectionHandler {
        shutdown_complete_tx: shutdown_complete_tx.clone(),
        conn_disconnect_notify,
        dispatcher: dispatcher.clone(),
        session_registry,
    });
    let transport = TransportListener::new(
        listener,
        task_group.clone(),
        tls_runtime.clone(),
        dispatcher.boundary().admission_controller(),
        DEFAULT_TLS_HANDSHAKE_TIMEOUT,
    )
    .with_authorized_dispatch(dispatcher.boundary(), transport_principal)
    .with_file_region_io(file_region_blocking, file_transfer_mode)
    .with_validated_frame_limits(frame_limits)
    .with_validated_proxy_protocol(proxy_protocol)
    .with_telemetry(telemetry);
    #[cfg(test)]
    let transport = match write_preflight_barrier {
        Some(barrier) => transport.with_write_preflight_barrier(barrier),
        None => transport,
    };
    #[cfg(test)]
    let transport = match test_request_deadline {
        Some(deadline) => transport.with_test_request_deadline(deadline),
        None => transport,
    };

    tokio::select! {
        result = transport.run_authorized(route) => {
            if let Err(error) = result {
                error!(cause = %error, "V2 remoting listener stopped with an error");
            }
        }
        _ = shutdown => {
            info!("Shutting down V2 remoting server");
        }
    }

    let deadline = task_group
        .shutdown_deadline()
        .unwrap_or_else(|| ShutdownDeadline::after(Duration::from_secs(30)));
    task_group.cancel();
    drop(shutdown_complete_tx);
    let _ = tokio::time::timeout(deadline.remaining(), shutdown_complete_rx.recv()).await;
    let tls_report = tls_runtime
        .shutdown_gracefully(deadline.remaining().min(Duration::from_secs(3)))
        .await;
    let mut report = task_group.shutdown_until(deadline).await;
    if let Some(tls_report) = tls_report {
        report.children.push(tls_report);
    }
    report.log_if_unhealthy();
    Ok(report)
}

#[cfg(test)]
mod tests;
