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

//! Explicit test fixtures excluded from default production builds.

use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;

use rocketmq_error::RocketMQResult;
use rocketmq_runtime::TaskGroup;

use crate::base::pending_request_table::PendingRequestTable;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;

pub use crate::client::connect_with_config;
pub use crate::client::connect_with_config_and_telemetry;
pub use crate::client::connect_with_config_options_and_telemetry;
pub use crate::codec::remoting_command_codec::RemotingCommandCodec;
pub use crate::connection::transport_io_snapshot;
pub use crate::connection::Connection;
pub use crate::local::LocalRequestHarness;
pub use crate::server::run_connected_session;
pub use crate::server::run_connected_session_with_io_policy;
pub use crate::server::ConnectionHandler;
pub use crate::server::SessionHandle;
pub use crate::server::SessionIoPolicy;
pub use crate::server::SessionProcessor;
pub use crate::server::SessionTransportServer;
pub use crate::server::SessionTransportServerConfig;
pub use crate::server::TransportListener;
#[cfg(not(feature = "tls"))]
pub use crate::tls::tls_disabled_error;
pub use crate::write_strategy::FrameWriteMode;
pub use crate::write_strategy::FrameWriter;
pub use crate::writer_runtime::MicroBatchConfig;
pub use crate::writer_runtime::WriterQueueConfig;

/// Builds a real transport-backed channel for downstream tests without
/// exposing response-table or channel ownership internals.
pub struct TestChannelBuilder {
    connection: Connection,
    task_group: TaskGroup,
    local_address: SocketAddr,
    remote_address: SocketAddr,
}

impl TestChannelBuilder {
    #[must_use]
    pub fn new(connection: Connection, task_group: TaskGroup) -> Self {
        let unspecified = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        Self {
            connection,
            task_group,
            local_address: unspecified,
            remote_address: unspecified,
        }
    }

    #[must_use]
    pub fn addresses(mut self, local_address: SocketAddr, remote_address: SocketAddr) -> Self {
        self.local_address = local_address;
        self.remote_address = remote_address;
        self
    }

    /// Creates the channel and registers its sole send task under the supplied task group.
    ///
    /// # Errors
    ///
    /// Returns an error when the lifecycle owner cannot register the send task.
    pub fn build(self) -> RocketMQResult<Channel> {
        let inner =
            ChannelInner::try_new_with_pending_requests(self.connection, PendingRequestTable::new(), self.task_group)?;
        Ok(Channel::new(Arc::new(inner), self.local_address, self.remote_address))
    }
}
