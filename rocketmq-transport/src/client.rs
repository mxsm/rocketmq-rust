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
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::ShutdownDeadline;

use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::base::pending_request_table::PendingRequestLimits;
use crate::base::pending_request_table::PendingRequestTable;
use crate::base::pending_request_table::PendingRequestUsage;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::config::TlsConfig;
use crate::connection::Connection;
use crate::security::TransportSecurity;
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
}

impl ConnectedTransport {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
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
    deadline: ShutdownDeadline,
) -> RocketMQResult<ConnectedTransport> {
    let timeout_at = tokio::time::Instant::from_std(deadline.instant());
    let stream = tokio::time::timeout_at(timeout_at, tokio::net::TcpStream::connect(address))
        .await
        .map_err(|_| RocketMQError::network_timeout(address, deadline.remaining()))??;
    let local_addr = stream.local_addr()?;
    let remote_addr = stream.peer_addr()?;
    let negotiated_tls = tls_config.enable;
    let connection = if negotiated_tls {
        #[cfg(feature = "tls")]
        {
            let server_name = server_name_from_address(address);
            let tls_stream = tokio::time::timeout_at(timeout_at, connect_tls_stream(stream, &server_name, tls_config))
                .await
                .map_err(|_| RocketMQError::network_timeout(address, deadline.remaining()))??;
            Connection::new_with_stream_and_limits(tls_stream, frame_limits)
        }
        #[cfg(not(feature = "tls"))]
        {
            let _ = stream;
            return Err(tls_disabled_error());
        }
    } else {
        Connection::new_with_limits(stream, frame_limits)
    };
    Ok(ConnectedTransport {
        connection,
        local_addr,
        remote_addr,
        negotiated_tls,
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
    _service_context: ServiceContext,
    admission: Arc<AdmissionController>,
    pending: PendingRequestTable,
    next_opaque: AtomicI32,
    security: Arc<TransportSecurity>,
}

impl TransportClient {
    pub fn new(service_context: ServiceContext, admission: Arc<AdmissionController>) -> Self {
        Self::new_with_security(service_context, admission, Arc::new(TransportSecurity::new(None, None)))
    }

    pub fn new_with_security(
        service_context: ServiceContext,
        admission: Arc<AdmissionController>,
        security: Arc<TransportSecurity>,
    ) -> Self {
        Self {
            _service_context: service_context,
            admission,
            pending: PendingRequestTable::with_limits(PendingRequestLimits {
                max_count: 65_536,
                max_bytes: 256 * 1024 * 1024,
            }),
            next_opaque: AtomicI32::new(1),
            security,
        }
    }

    pub fn pending_usage(&self) -> PendingRequestUsage {
        self.pending.usage()
    }

    pub async fn invoke(
        &self,
        address: SocketAddr,
        mut request: RemotingCommand,
        deadline: ShutdownDeadline,
    ) -> RocketMQResult<RemotingCommand> {
        let timeout_at = tokio::time::Instant::from_std(deadline.instant());
        let stream = tokio::time::timeout_at(timeout_at, tokio::net::TcpStream::connect(address))
            .await
            .map_err(|_| RocketMQError::network_timeout(address.to_string(), deadline.remaining()))??;
        let local_ip = stream.local_addr()?.ip();
        let scope = AdmissionScope::new(address.ip()).with_session(address.port() as u64);
        let peer = PeerInfo::new(address, false);
        self.security
            .sign(&mut request, Some(&peer))
            .map_err(|error| RocketMQError::network_connection_failed(address.to_string(), error.to_string()))?;
        let retained_bytes = request.body().map_or(0, bytes::Bytes::len);
        let _connection_permit = self
            .admission
            .try_acquire(AdmissionResource::Connection, scope, 0, AdmissionClass::Data)
            .map_err(|error| RocketMQError::network_connection_failed(address.to_string(), error.to_string()))?;
        let _inflight_permit = self
            .admission
            .try_acquire(
                AdmissionResource::Inflight,
                AdmissionScope::new(local_ip).with_session(address.port() as u64),
                retained_bytes,
                AdmissionClass::Data,
            )
            .map_err(|error| RocketMQError::network_connection_failed(address.to_string(), error.to_string()))?;

        let mut connection = Connection::new(stream);
        let owner = self.pending.new_owner();
        let opaque = self.next_opaque.fetch_add(1, Ordering::Relaxed);
        request.set_opaque_mut(opaque);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let guard = self.pending.register_for_owner_with_bytes(
            &owner,
            opaque,
            deadline.remaining().as_millis().min(u128::from(u64::MAX)) as u64,
            retained_bytes,
            sender,
        )?;

        if let Err(error) = tokio::time::timeout_at(timeout_at, connection.send_command(request))
            .await
            .map_err(|_| RocketMQError::network_timeout(address.to_string(), deadline.remaining()))?
        {
            guard.complete(Err(error));
            let _ = connection.shutdown().await;
            return receiver.await.map_err(|_| {
                RocketMQError::network_connection_failed(address.to_string(), "send completion dropped")
            })?;
        }

        match tokio::time::timeout_at(timeout_at, connection.receive_command()).await {
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
                guard.expire(
                    "transport_response",
                    deadline.remaining().as_millis().min(u128::from(u64::MAX)) as u64,
                );
            }
        }
        let _ = connection.shutdown().await;
        receiver
            .await
            .map_err(|_| RocketMQError::network_connection_failed(address.to_string(), "response completion dropped"))?
    }
}
