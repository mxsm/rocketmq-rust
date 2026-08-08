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

use std::net::SocketAddr;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::ChildServiceContext;

use crate::admission::estimated_connection_retained_bytes;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::base::pending_request_table::materialize_and_estimate_remoting_command_retained_bytes;
use crate::base::pending_request_table::PendingRequestLimits;
use crate::base::pending_request_table::PendingRequestTable;
use crate::base::pending_request_table::PendingRequestUsage;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::config::SocketOptions;
use crate::config::TlsConfig;
use crate::connection::Connection;
use crate::deadline::RequestDeadline;
use crate::security::TransportSecurity;
use crate::telemetry::TransportTelemetry;
#[cfg(feature = "tls")]
use crate::tls::connect_tls_stream;
#[cfg(not(feature = "tls"))]
use crate::tls::tls_disabled_error;
use rocketmq_security_api::PeerInfo;

pub struct ConnectedTransport {
    connection: Connection,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    negotiated_tls: bool,
    socket_nodelay: bool,
}

impl ConnectedTransport {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    pub fn socket_nodelay(&self) -> bool {
        self.socket_nodelay
    }

    pub fn into_parts(self) -> (Connection, SocketAddr, SocketAddr) {
        (self.connection, self.local_addr, self.remote_addr)
    }

    /// Splits the connection, socket addresses, and actual TLS-negotiation result.
    pub fn into_parts_with_tls(self) -> (Connection, SocketAddr, SocketAddr, bool) {
        (self.connection, self.local_addr, self.remote_addr, self.negotiated_tls)
    }
}

/// Connects TCP, negotiates optional TLS, and installs the canonical framed transport under one
/// absolute deadline.
pub async fn connect_with_config(
    address: &str,
    tls_config: &TlsConfig,
    frame_limits: FrameLimits,
    deadline: RequestDeadline,
) -> RocketMQResult<ConnectedTransport> {
    connect_with_config_and_telemetry(address, tls_config, frame_limits, deadline, TransportTelemetry::noop()).await
}

/// Connects a framed TCP/TLS transport bound to one explicit telemetry instance.
pub async fn connect_with_config_and_telemetry(
    address: &str,
    tls_config: &TlsConfig,
    frame_limits: FrameLimits,
    deadline: RequestDeadline,
    telemetry: TransportTelemetry,
) -> RocketMQResult<ConnectedTransport> {
    connect_with_config_options_and_telemetry(
        address,
        tls_config,
        frame_limits,
        SocketOptions::default(),
        deadline,
        telemetry,
    )
    .await
}

/// Connects with explicit symmetric TCP socket policy applied before TLS negotiation.
pub async fn connect_with_config_options_and_telemetry(
    address: &str,
    tls_config: &TlsConfig,
    frame_limits: FrameLimits,
    socket_options: SocketOptions,
    deadline: RequestDeadline,
    telemetry: TransportTelemetry,
) -> RocketMQResult<ConnectedTransport> {
    let stream = deadline
        .timeout(tokio::net::TcpStream::connect(address))
        .await
        .map_err(|_| RocketMQError::network_connection_timeout(address, deadline.budget_millis()))??;
    socket_options
        .apply(&stream)
        .map_err(|error| RocketMQError::network_connection_failed(address, error.to_string()))?;
    let socket_nodelay = stream
        .nodelay()
        .map_err(|error| RocketMQError::network_connection_failed(address, error.to_string()))?;
    let local_addr = stream.local_addr()?;
    let remote_addr = stream.peer_addr()?;
    let negotiated_tls = tls_config.enable;
    let connection = if negotiated_tls {
        #[cfg(feature = "tls")]
        {
            let server_name = server_name_from_address(address);
            let tls_stream = deadline
                .timeout(connect_tls_stream(stream, &server_name, tls_config))
                .await
                .map_err(|_| RocketMQError::network_connection_timeout(address, deadline.budget_millis()))??;
            Connection::new_with_tls_stream_and_limits(tls_stream, frame_limits).with_telemetry(telemetry)
        }
        #[cfg(not(feature = "tls"))]
        {
            let _ = stream;
            return Err(tls_disabled_error());
        }
    } else {
        Connection::new_with_limits(stream, frame_limits).with_telemetry(telemetry)
    };
    Ok(ConnectedTransport {
        connection,
        local_addr,
        remote_addr,
        negotiated_tls,
        socket_nodelay,
    })
}

#[cfg(feature = "tls")]
fn server_name_from_address(address: &str) -> String {
    if let Ok(socket_addr) = address.parse::<SocketAddr>() {
        return socket_addr.ip().to_string();
    }
    address
        .rsplit_once(':')
        .map_or(address, |(host, _)| host)
        .trim_matches(['[', ']'])
        .to_string()
}

/// Canonical low-level request client. Higher-level routing remains outside transport.
pub struct TransportClient {
    _service_context: ChildServiceContext,
    admission: Arc<AdmissionController>,
    pending: PendingRequestTable,
    next_opaque: AtomicI32,
    security: Arc<TransportSecurity>,
    telemetry: TransportTelemetry,
}

impl TransportClient {
    /// Builds a client with fail-closed pending-request budget validation.
    pub fn try_new(service_context: ChildServiceContext, admission: Arc<AdmissionController>) -> RocketMQResult<Self> {
        Self::try_new_with_security(
            service_context,
            admission,
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
        )
    }

    /// Compatibility constructor for existing embedders.
    ///
    /// # Panics
    ///
    /// Panics if pending-request resource-budget validation fails. New
    /// production composition should use [`Self::try_new`].
    pub fn new(service_context: ChildServiceContext, admission: Arc<AdmissionController>) -> Self {
        Self::try_new(service_context, admission)
            .unwrap_or_else(|error| panic!("invalid Transport client resource limits: {error}"))
    }

    pub fn try_new_with_security(
        service_context: ChildServiceContext,
        admission: Arc<AdmissionController>,
        security: Arc<TransportSecurity>,
    ) -> RocketMQResult<Self> {
        let process_budget = service_context.process_budget();
        Ok(Self {
            _service_context: service_context,
            admission,
            pending: PendingRequestTable::try_with_limits_and_budget(
                PendingRequestLimits {
                    max_count: 65_536,
                    max_bytes: 256 * 1024 * 1024,
                    ..PendingRequestLimits::default()
                },
                &process_budget,
            )?,
            next_opaque: AtomicI32::new(1),
            security,
            telemetry: TransportTelemetry::noop(),
        })
    }

    /// Compatibility constructor for existing embedders with custom security.
    ///
    /// # Panics
    ///
    /// Panics if pending-request resource-budget validation fails. New
    /// production composition should use [`Self::try_new_with_security`].
    pub fn new_with_security(
        service_context: ChildServiceContext,
        admission: Arc<AdmissionController>,
        security: Arc<TransportSecurity>,
    ) -> Self {
        Self::try_new_with_security(service_context, admission, security)
            .unwrap_or_else(|error| panic!("invalid Transport client resource limits: {error}"))
    }

    /// Binds newly opened connections to one explicit telemetry instance.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: TransportTelemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn pending_usage(&self) -> PendingRequestUsage {
        self.pending.usage()
    }

    pub async fn invoke(
        &self,
        address: SocketAddr,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> RocketMQResult<RemotingCommand> {
        let tls_config = TlsConfig::default();
        self.invoke_with_config(address, request, &tls_config, deadline).await
    }

    /// Invokes one request through the canonical TCP/TLS connection boundary.
    ///
    /// The configured TLS mode applies to this invocation only. Pending-request
    /// ownership, admission permits, response correlation, and connection
    /// shutdown retain the same fail-closed behavior as [`Self::invoke`].
    pub async fn invoke_with_config(
        &self,
        address: SocketAddr,
        mut request: RemotingCommand,
        tls_config: &TlsConfig,
        deadline: RequestDeadline,
    ) -> RocketMQResult<RemotingCommand> {
        let connected = connect_with_config_and_telemetry(
            &address.to_string(),
            tls_config,
            FrameLimits::default(),
            deadline,
            self.telemetry.clone(),
        )
        .await?;
        let (mut connection, local_addr, remote_addr, negotiated_tls) = connected.into_parts_with_tls();
        let scope = AdmissionScope::new(remote_addr.ip()).with_session(remote_addr.port() as u64);
        let peer = PeerInfo::new(remote_addr, negotiated_tls);
        deadline.ensure_before_send(address.to_string())?;
        self.security
            .sign(&mut request, Some(&peer))
            .map_err(|error| RocketMQError::network_connection_failed(address.to_string(), error.to_string()))?;
        deadline.ensure_before_send(address.to_string())?;
        let retained_bytes = materialize_and_estimate_remoting_command_retained_bytes(&mut request);
        let _connection_permit = self
            .admission
            .try_acquire(
                AdmissionResource::Connection,
                scope,
                estimated_connection_retained_bytes(),
                AdmissionClass::Data,
            )
            .map_err(|error| RocketMQError::network_connection_failed(address.to_string(), error.to_string()))?;
        let _inflight_permit = self
            .admission
            .try_acquire(
                AdmissionResource::Inflight,
                AdmissionScope::new(local_addr.ip()).with_session(remote_addr.port() as u64),
                retained_bytes,
                AdmissionClass::Data,
            )
            .map_err(|error| RocketMQError::network_connection_failed(address.to_string(), error.to_string()))?;

        let owner = self.pending.new_owner();
        let opaque = self.next_opaque.fetch_add(1, Ordering::Relaxed);
        request.set_opaque_mut(opaque);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let guard = self
            .pending
            .register_for_owner_with_bytes(&owner, opaque, deadline, retained_bytes, sender)?;

        if let Err(error) = connection
            .send_command_with_deadline(request, deadline, address.to_string())
            .await
        {
            guard.complete(Err(error));
            let _ = deadline.timeout(connection.shutdown()).await;
            return receiver.await.map_err(|_| {
                RocketMQError::network_connection_failed(address.to_string(), "send completion dropped")
            })?;
        }

        match deadline.timeout(connection.receive_command()).await {
            Ok(Some(Ok(response))) => {
                let response_opaque = response.opaque();
                if !self
                    .pending
                    .complete_response_for_owner(&owner, response_opaque, response)
                {
                    guard.complete(Err(RocketMQError::network_connection_failed(
                        address.to_string(),
                        format!("unexpected response opaque {response_opaque}; expected {opaque}"),
                    )));
                }
            }
            Ok(Some(Err(error))) => {
                guard.complete(Err(error));
            }
            Ok(None) => {
                self.pending.close_owner(&owner, || {
                    RocketMQError::network_connection_failed(address.to_string(), "connection closed before response")
                });
            }
            Err(_) => {
                guard.expire(address.to_string());
            }
        }
        let _ = deadline.timeout(connection.shutdown()).await;
        receiver
            .await
            .map_err(|_| RocketMQError::network_connection_failed(address.to_string(), "response completion dropped"))?
    }
}
