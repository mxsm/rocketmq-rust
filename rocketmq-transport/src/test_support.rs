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
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use crate::session_view::SessionId;

mod embedded_v2;
pub use embedded_v2::EmbeddedRequestHarnessV2;

pub use crate::client::connect_target_with_config_options_and_telemetry;
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

/// Creates a stable session identity for downstream behavior tests.
///
/// Production code cannot construct [`SessionId`] values; it receives them from the trusted
/// transport boundary. This fixture is available only when the explicit `test-support` feature is
/// enabled and rejects the sentinels excluded by the production allocator.
#[must_use]
pub fn session_id_for_test(owner_id: u64) -> SessionId {
    assert!(
        !matches!(owner_id, 0 | u64::MAX),
        "test session owner must use a production-valid identity"
    );
    SessionId::from_session_owner(owner_id)
}

/// Test-only owner for exercising legacy session-close cleanup through the
/// same opaque context capability used by admitted network requests.
pub struct LegacySessionCleanupHarness {
    owner: crate::dispatch::DeferredSessionCleanupOwner,
}

/// Test-only canonical session owner for claimed legacy execution.
///
/// The harness uses the real session executor, admission scope, cleanup
/// coordinator, and cancellation path while leaving its task-group lifecycle
/// with the caller.
pub struct LegacySessionExecutionHarness {
    owner: crate::dispatch::DeferredSessionCleanupOwner,
    executor: crate::session_executor::SessionExecutor,
    _admission: crate::admission::AdmissionController,
}

impl LegacySessionExecutionHarness {
    #[must_use]
    pub fn new(owner_id: u64, task_group: &TaskGroup) -> Self {
        let admission = crate::admission::AdmissionController::new(crate::admission::AdmissionLimits::default());
        let scope = admission
            .prepare_scope(
                crate::admission::AdmissionScope::new(IpAddr::V4(Ipv4Addr::LOCALHOST)).with_session(owner_id),
            )
            .expect("legacy session execution test admission scope");
        let executor = crate::session_executor::SessionExecutor::try_new(task_group, scope)
            .expect("legacy session execution test owner");
        Self {
            owner: crate::dispatch::DeferredSessionCleanupOwner::new(session_id_for_test(owner_id)),
            executor,
            _admission: admission,
        }
    }

    #[must_use]
    pub fn context(&self, channel: Channel, retained_bytes: usize, request_code: i32) -> ConnectionHandlerContext {
        let seed = crate::dispatch::LegacySessionExecutionSeed::new(
            self.owner.registration(),
            self.executor.deferred_resume_executor(),
            retained_bytes,
            crate::admission::AdmissionClass::for_request_code(request_code),
            crate::request_ordering::RequestOrdering::Concurrent,
        );
        Arc::new(ConnectionHandlerContextWrapper::new_with_legacy_session_execution(
            channel, seed,
        ))
    }

    pub fn set_first_poll_gate(&self, entered: Arc<tokio::sync::Notify>, release: Arc<tokio::sync::Notify>) {
        self.executor.set_legacy_execution_first_poll_gate(entered, release);
    }

    pub fn set_insert_checkpoint(&self, checkpoint: impl Fn(bool) + Send + Sync + 'static) {
        self.owner.set_insert_checkpoint(Arc::new(checkpoint));
    }

    pub fn close(&self) {
        self.executor.begin_close();
        let _ = self.owner.close();
    }
}

impl LegacySessionCleanupHarness {
    #[must_use]
    pub fn new(owner_id: u64) -> Self {
        Self {
            owner: crate::dispatch::DeferredSessionCleanupOwner::new(session_id_for_test(owner_id)),
        }
    }

    #[must_use]
    pub fn context(&self, channel: Channel) -> ConnectionHandlerContext {
        Arc::new(ConnectionHandlerContextWrapper::new_with_legacy_session_cleanup(
            channel,
            self.owner.registration(),
        ))
    }

    /// Installs a deterministic checkpoint immediately before the canonical
    /// cleanup enrollment publishes its caller-owned node. The callback runs
    /// while enrollment still owns the cleanup coordinator gate.
    pub fn set_insert_checkpoint(&self, checkpoint: impl Fn(bool) + Send + Sync + 'static) {
        self.owner.set_insert_checkpoint(Arc::new(checkpoint));
    }

    pub fn close(&self) {
        let _ = self.owner.close();
    }
}

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

    /// Installs a deterministic one-shot barrier immediately before the first
    /// possible socket write.
    #[must_use]
    pub fn write_preflight_barrier(
        mut self,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    ) -> Self {
        self.connection
            .set_write_preflight_barrier(crate::write_strategy::WritePreflightBarrier::new(entered, release));
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
