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
use crate::clients::nameserver_endpoint::ConnectTarget;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::config::SocketOptions;
use crate::config::TlsConfig;
use crate::connection::Connection;
use crate::deadline::RequestDeadline;
use crate::error_helpers::admission_queue_saturated;
use crate::error_helpers::connection_failed_for_remote;
use crate::error_helpers::connection_failed_without_source_for_remote;
use crate::error_helpers::connection_timeout_caused_by;
use crate::error_helpers::dns_failed;
use crate::error_helpers::dns_failed_without_source;
use crate::error_helpers::endpoint_invalid;
use crate::error_helpers::network;
use crate::error_helpers::response_timeout_caused_by_for_remote;
use crate::error_helpers::TransportStage;
#[cfg(feature = "socks")]
use crate::runtime::config::client_config::TransportClientConfig;
use crate::security::TransportSecurity;
use crate::telemetry::TransportTelemetry;
#[cfg(feature = "tls")]
use crate::tls::connect_tls_stream;
#[cfg(not(feature = "tls"))]
use crate::tls::tls_disabled_error;
use rocketmq_security_api::PeerInfo;

#[allow(
    dead_code,
    reason = "diagnostic fields are exposed only through the feature-gated test and benchmark adapters"
)]
pub struct ConnectedTransport {
    connection: Connection,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    negotiated_tls: bool,
    socket_nodelay: bool,
}

#[allow(
    dead_code,
    reason = "diagnostic accessors are exposed only through the feature-gated test and benchmark adapters"
)]
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
#[allow(
    dead_code,
    reason = "the no-telemetry convenience wrapper is exposed only by test_support and benchmark_support"
)]
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
    connect_legacy_address_with_options_and_telemetry(
        address,
        tls_config,
        #[cfg(feature = "socks")]
        None,
        frame_limits,
        socket_options,
        deadline,
        telemetry,
    )
    .await
}

/// Connects to a physical socket while preserving a distinct logical TLS authority.
pub async fn connect_target_with_config_options_and_telemetry(
    target: &ConnectTarget,
    tls_config: &TlsConfig,
    frame_limits: FrameLimits,
    socket_options: SocketOptions,
    deadline: RequestDeadline,
    telemetry: TransportTelemetry,
) -> RocketMQResult<ConnectedTransport> {
    connect_physical_with_options_and_telemetry(
        target.socket_addr(),
        target.authority(),
        target.tls_server_name(),
        tls_config,
        #[cfg(feature = "socks")]
        None,
        frame_limits,
        socket_options,
        deadline,
        telemetry,
    )
    .await
}

#[cfg(feature = "socks")]
pub(crate) async fn connect_with_transport_config_and_telemetry(
    address: &str,
    transport_config: &TransportClientConfig,
    frame_limits: FrameLimits,
    deadline: RequestDeadline,
    telemetry: TransportTelemetry,
) -> RocketMQResult<ConnectedTransport> {
    connect_legacy_address_with_options_and_telemetry(
        address,
        &transport_config.tls,
        Some(&transport_config.socks_proxy),
        frame_limits,
        SocketOptions::default(),
        deadline,
        telemetry,
    )
    .await
}

#[cfg(feature = "socks")]
pub(crate) async fn connect_target_with_transport_config_and_telemetry(
    target: &ConnectTarget,
    transport_config: &TransportClientConfig,
    frame_limits: FrameLimits,
    deadline: RequestDeadline,
    telemetry: TransportTelemetry,
) -> RocketMQResult<ConnectedTransport> {
    if transport_config.socks_proxy.is_empty() {
        return connect_target_with_config_options_and_telemetry(
            target,
            &transport_config.tls,
            frame_limits,
            SocketOptions::default(),
            deadline,
            telemetry,
        )
        .await;
    }
    connect_physical_with_options_and_telemetry(
        target.socket_addr(),
        target.authority(),
        target.tls_server_name(),
        &transport_config.tls,
        Some(&transport_config.socks_proxy),
        frame_limits,
        SocketOptions::default(),
        deadline,
        telemetry,
    )
    .await
}

async fn connect_legacy_address_with_options_and_telemetry(
    address: &str,
    tls_config: &TlsConfig,
    #[cfg(feature = "socks")] socks_proxy: Option<&crate::socks::SocksProxyConfig>,
    frame_limits: FrameLimits,
    socket_options: SocketOptions,
    deadline: RequestDeadline,
    telemetry: TransportTelemetry,
) -> RocketMQResult<ConnectedTransport> {
    let stream = connect_legacy_tcp(
        address,
        #[cfg(feature = "socks")]
        socks_proxy,
        deadline,
    )
    .await?;
    socket_options
        .apply(&stream)
        .map_err(|source| network(connection_failed_for_remote(address, TransportStage::Connect, source)))?;
    let socket_nodelay = stream
        .nodelay()
        .map_err(|source| network(connection_failed_for_remote(address, TransportStage::Connect, source)))?;
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
                .map_err(|source| network(connection_timeout_caused_by(address, deadline.budget_millis(), source)))??;
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

#[allow(
    clippy::too_many_arguments,
    reason = "physical address, logical authority, TLS identity, and socket policy are independent connection inputs"
)]
async fn connect_physical_with_options_and_telemetry(
    socket_addr: SocketAddr,
    authority: &str,
    tls_server_name: &str,
    tls_config: &TlsConfig,
    #[cfg(feature = "socks")] socks_proxy: Option<&crate::socks::SocksProxyConfig>,
    frame_limits: FrameLimits,
    socket_options: SocketOptions,
    deadline: RequestDeadline,
    telemetry: TransportTelemetry,
) -> RocketMQResult<ConnectedTransport> {
    let stream = connect_physical_tcp(
        socket_addr,
        authority,
        #[cfg(feature = "socks")]
        socks_proxy,
        deadline,
    )
    .await?;
    socket_options
        .apply(&stream)
        .map_err(|source| network(connection_failed_for_remote(authority, TransportStage::Connect, source)))?;
    let socket_nodelay = stream
        .nodelay()
        .map_err(|source| network(connection_failed_for_remote(authority, TransportStage::Connect, source)))?;
    let local_addr = stream.local_addr()?;
    let remote_addr = stream.peer_addr()?;
    let negotiated_tls = tls_config.enable;
    let connection = if negotiated_tls {
        #[cfg(feature = "tls")]
        {
            let tls_stream = deadline
                .timeout(connect_tls_stream(stream, tls_server_name, tls_config))
                .await
                .map_err(|source| {
                    network(connection_timeout_caused_by(
                        authority,
                        deadline.budget_millis(),
                        source,
                    ))
                })??;
            Connection::new_with_tls_stream_and_limits(tls_stream, frame_limits).with_telemetry(telemetry)
        }
        #[cfg(not(feature = "tls"))]
        {
            let _ = stream;
            let _ = tls_server_name;
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

async fn connect_legacy_tcp(
    address: &str,
    #[cfg(feature = "socks")] socks_proxy: Option<&crate::socks::SocksProxyConfig>,
    deadline: RequestDeadline,
) -> RocketMQResult<tokio::net::TcpStream> {
    #[cfg(feature = "socks")]
    if let Some(socks_proxy) = socks_proxy.filter(|config| !config.is_empty()) {
        return crate::socks::connect_target(socks_proxy, address, None, deadline).await;
    }
    if !valid_legacy_authority(address) {
        return Err(network(endpoint_invalid(!address.is_empty())));
    }
    if let Ok(socket_addr) = address.parse::<SocketAddr>() {
        return deadline
            .timeout(tokio::net::TcpStream::connect(socket_addr))
            .await
            .map_err(|source| network(connection_timeout_caused_by(address, deadline.budget_millis(), source)))?
            .map_err(|source| network(connection_failed_for_remote(address, TransportStage::Connect, source)));
    }

    let resolved = deadline
        .timeout(tokio::net::lookup_host(address))
        .await
        .map_err(|source| network(connection_timeout_caused_by(address, deadline.budget_millis(), source)))?
        .map_err(|source| network(dns_failed(source)))?;
    let mut last_connect_error = None;
    for socket_addr in resolved {
        match deadline.timeout(tokio::net::TcpStream::connect(socket_addr)).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(source)) => last_connect_error = Some(source),
            Err(source) => {
                return Err(network(connection_timeout_caused_by(
                    address,
                    deadline.budget_millis(),
                    source,
                )))
            }
        }
    }
    match last_connect_error {
        Some(source) => Err(network(connection_failed_for_remote(
            address,
            TransportStage::Connect,
            source,
        ))),
        None => Err(network(dns_failed_without_source())),
    }
}

fn valid_legacy_authority(address: &str) -> bool {
    if address.parse::<SocketAddr>().is_ok() {
        return true;
    }
    let Some((host, port)) = address.rsplit_once(':') else {
        return false;
    };
    !host.is_empty() && !host.as_bytes().contains(&0) && port.parse::<u16>().is_ok()
}

async fn connect_physical_tcp(
    socket_addr: SocketAddr,
    authority: &str,
    #[cfg(feature = "socks")] socks_proxy: Option<&crate::socks::SocksProxyConfig>,
    deadline: RequestDeadline,
) -> RocketMQResult<tokio::net::TcpStream> {
    #[cfg(feature = "socks")]
    if let Some(socks_proxy) = socks_proxy.filter(|config| !config.is_empty()) {
        return crate::socks::connect_target(socks_proxy, authority, Some(socket_addr), deadline).await;
    }
    deadline
        .timeout(tokio::net::TcpStream::connect(socket_addr))
        .await
        .map_err(|source| {
            network(connection_timeout_caused_by(
                authority,
                deadline.budget_millis(),
                source,
            ))
        })?
        .map_err(|source| network(connection_failed_for_remote(authority, TransportStage::Connect, source)))
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
pub struct OneShotTransportClient {
    _service_context: ChildServiceContext,
    admission: Arc<AdmissionController>,
    pending: PendingRequestTable,
    next_opaque: AtomicI32,
    security: Arc<TransportSecurity>,
    telemetry: TransportTelemetry,
    frame_limits: FrameLimits,
}

impl OneShotTransportClient {
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
            frame_limits: FrameLimits::default(),
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

    /// Binds every plaintext/TLS connection opened by this client to one frame profile.
    pub fn try_with_frame_limits(mut self, frame_limits: FrameLimits) -> RocketMQResult<Self> {
        frame_limits.validate()?;
        self.frame_limits = frame_limits;
        Ok(self)
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
            self.frame_limits,
            deadline,
            self.telemetry.clone(),
        )
        .await?;
        let (mut connection, local_addr, remote_addr, negotiated_tls) = connected.into_parts_with_tls();
        let scope = AdmissionScope::new(remote_addr.ip()).with_session(remote_addr.port() as u64);
        let peer = PeerInfo::new(remote_addr, negotiated_tls);
        deadline.ensure_before_send()?;
        self.security.sign(&mut request, Some(&peer)).map_err(|source| {
            network(connection_failed_for_remote(
                address.to_string(),
                TransportStage::Connect,
                source,
            ))
        })?;
        deadline.ensure_before_send()?;
        let retained_bytes = materialize_and_estimate_remoting_command_retained_bytes(&mut request);
        let _connection_permit = self
            .admission
            .try_acquire(
                AdmissionResource::Connection,
                scope,
                estimated_connection_retained_bytes(),
                AdmissionClass::Data,
            )
            .into_result()
            .map_err(|_| network(admission_queue_saturated(address.to_string())))?;
        let _inflight_permit = self
            .admission
            .try_acquire(
                AdmissionResource::Inflight,
                AdmissionScope::new(local_addr.ip()).with_session(remote_addr.port() as u64),
                retained_bytes,
                AdmissionClass::Data,
            )
            .into_result()
            .map_err(|_| network(admission_queue_saturated(address.to_string())))?;

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
                network(connection_failed_without_source_for_remote(
                    address.to_string(),
                    TransportStage::Closed,
                ))
            })?;
        }

        match deadline.timeout(connection.receive_command()).await {
            Ok(Some(Ok(response))) => {
                let response_opaque = response.opaque();
                if !self
                    .pending
                    .complete_response_for_owner(&owner, response_opaque, response)
                {
                    guard.complete(Err(network(connection_failed_without_source_for_remote(
                        address.to_string(),
                        TransportStage::Closed,
                    ))));
                }
            }
            Ok(Some(Err(error))) => {
                guard.complete(Err(error));
            }
            Ok(None) => {
                self.pending.close_owner(&owner, || {
                    network(connection_failed_without_source_for_remote(
                        address.to_string(),
                        TransportStage::Closed,
                    ))
                });
            }
            Err(source) => {
                let source =
                    response_timeout_caused_by_for_remote(address.to_string(), deadline.budget_millis(), source);
                guard.expire_with_network(source);
            }
        }
        let _ = deadline.timeout(connection.shutdown()).await;
        receiver.await.map_err(|_| {
            network(connection_failed_without_source_for_remote(
                address.to_string(),
                TransportStage::Closed,
            ))
        })?
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::time::Duration;

    use rocketmq_error::RocketMQError;

    use super::*;

    #[test]
    fn legacy_authority_validation_accepts_existing_direct_address_forms() {
        for address in ["127.0.0.1:9876", "broker.example:9876", "[::1]:9876", "::1:9876"] {
            assert!(valid_legacy_authority(address), "valid authority: {address}");
        }
        for address in [
            "",
            "broker.example",
            ":9876",
            "broker.example:bad",
            "broker.example:65536",
            "host\0name:9876",
        ] {
            assert!(!valid_legacy_authority(address), "invalid authority: {address:?}");
        }
    }

    #[tokio::test]
    async fn direct_invalid_authority_uses_source_free_endpoint_descriptor() {
        for (address, expected_context_len) in [
            ("", 0),
            ("broker.example", 1),
            ("broker.example:65536", 1),
            ("host\0name:9876", 1),
        ] {
            let error = match connect_with_config(
                address,
                &TlsConfig::default(),
                FrameLimits::default(),
                RequestDeadline::after(Duration::from_secs(1)),
            )
            .await
            {
                Ok(_) => panic!("invalid direct authority must fail before DNS or connect"),
                Err(error) => error,
            };
            let RocketMQError::Network(error) = error else {
                panic!("invalid direct authority must use the canonical Network carrier")
            };
            assert_eq!(error.code(), rocketmq_error::TRANSPORT_ENDPOINT_INVALID.code());
            assert_eq!(error.context().len(), expected_context_len);
            assert!(error.source().is_none());
            assert!(error
                .public_view()
                .expect("endpoint context matches its descriptor")
                .fields()
                .next()
                .is_none());
            assert_eq!(error.projection().remoting().code.as_i32(), 2);
        }
    }

    #[cfg(feature = "socks")]
    #[tokio::test]
    async fn socks_invalid_target_keeps_the_existing_configuration_error() {
        let config = crate::socks::SocksProxyConfig::parse_java_json(r#"{"example.com":{"addr":"127.0.0.1:1080"}}"#)
            .expect("valid SOCKS fixture");
        let error = connect_legacy_tcp("", Some(&config), RequestDeadline::after(Duration::from_secs(1)))
            .await
            .expect_err("SOCKS target parser must reject the invalid authority");
        assert_eq!(error.boundary_view().remoting().code.as_i32(), 29);
        assert!(matches!(
            error,
            RocketMQError::ConfigInvalidValue {
                key: "com.rocketmq.socks.proxy.config",
                ..
            }
        ));
    }

    #[cfg(feature = "socks")]
    #[tokio::test]
    async fn socks_pre_resolver_invalid_host_uses_source_free_endpoint_descriptor() {
        let config = crate::socks::SocksProxyConfig::parse_java_json(r#"{"example.com":{"addr":"127.0.0.1:1080"}}"#)
            .expect("valid SOCKS fixture");
        for authority in [":9876", "host\0name:9876"] {
            let error = connect_legacy_tcp(authority, Some(&config), RequestDeadline::after(Duration::from_secs(1)))
                .await
                .expect_err("SOCKS host must fail before resolver selection");
            let RocketMQError::Network(error) = error else {
                panic!("pre-resolver SOCKS host rejection must use Network")
            };
            assert_eq!(error.code(), rocketmq_error::TRANSPORT_ENDPOINT_INVALID.code());
            assert_eq!(error.context().len(), 1);
            assert!(error.source().is_none());
            assert!(error
                .public_view()
                .expect("endpoint context matches its descriptor")
                .fields()
                .next()
                .is_none());
            assert_eq!(error.projection().remoting().code.as_i32(), 2);
        }
    }
}
