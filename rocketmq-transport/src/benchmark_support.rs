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

//! Narrow performance-harness adapters enabled only by the `test-support` feature.

use std::hint::black_box;
use std::net::SocketAddr;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::admission::AdmissionScopeHandle;
use crate::base::pending_request_table::PendingRequestLimits;
use crate::base::pending_request_table::PendingRequestOwner;
use crate::base::pending_request_table::PendingRequestTable;
use crate::deadline::RequestDeadline;
use crate::hook_registry::HookRegistry;
use crate::runtime::RPCHook;

pub use crate::client::connect_with_config;
pub use crate::codec::remoting_command_codec::RemotingCommandCodec;
pub use crate::connection::transport_io_snapshot;
pub use crate::connection::Connection;
pub use crate::server::run_connected_session;
pub use crate::server::ConnectionHandler;
pub use crate::server::SessionHandle;
pub use crate::server::SessionProcessor;
pub use crate::server::SessionTransportServer;
pub use crate::server::SessionTransportServerConfig;
pub use crate::write_strategy::FrameWriteMode;
pub use crate::write_strategy::FrameWriter;

/// Same-controller comparison between registry lookup and a prepared session scope.
pub struct AdmissionHotPathHarness {
    controller: AdmissionController,
    scope: AdmissionScope,
    prepared: AdmissionScopeHandle,
}

impl AdmissionHotPathHarness {
    #[must_use]
    pub fn new() -> Self {
        let controller = AdmissionController::new(AdmissionLimits::default());
        let scope = AdmissionScope::new("127.0.0.1".parse().expect("loopback"))
            .with_tenant(7)
            .with_session(11);
        let prepared = controller.prepare_scope(scope).expect("prepare benchmark scope");
        Self {
            controller,
            scope,
            prepared,
        }
    }

    pub fn registry_lookup_acquire_release(&self, bytes: usize) {
        let permit = self
            .controller
            .try_acquire(AdmissionResource::Inflight, self.scope, bytes, AdmissionClass::Data)
            .expect("registry lookup admission");
        black_box(permit);
    }

    pub fn prepared_acquire_release(&self, bytes: usize) {
        let permit = self
            .prepared
            .try_acquire(AdmissionResource::Inflight, bytes, AdmissionClass::Data)
            .expect("prepared admission");
        black_box(permit);
    }
}

impl Default for AdmissionHotPathHarness {
    fn default() -> Self {
        Self::new()
    }
}

struct NoopHook;

impl RPCHook for NoopHook {
    fn do_before_request(
        &self,
        _remote_addr: SocketAddr,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: SocketAddr,
        _request: &RemotingCommand,
        _response: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        Ok(())
    }
}

/// Same-hook-set comparison between per-request `Vec` cloning and `ArcSwap` snapshots.
pub struct HookHotPathHarness {
    legacy: Vec<Arc<dyn RPCHook>>,
    registry: HookRegistry,
}

impl HookHotPathHarness {
    #[must_use]
    pub fn new(hooks: usize) -> Self {
        let legacy = (0..hooks)
            .map(|_| Arc::new(NoopHook) as Arc<dyn RPCHook>)
            .collect::<Vec<_>>();
        Self {
            registry: HookRegistry::new(legacy.clone()),
            legacy,
        }
    }

    pub fn clone_legacy(&self) -> usize {
        let snapshot = self.legacy.clone();
        black_box(snapshot.iter().map(Arc::strong_count).sum())
    }

    pub fn load_snapshot(&self) -> usize {
        let snapshot = self.registry.snapshot();
        black_box(snapshot.as_ref().map_or(0, |snapshot| snapshot.hooks().len()))
    }
}

/// Concrete pending completion compared with the removed boxed/mutex completion shape.
pub struct PendingHotPathHarness {
    table: PendingRequestTable,
    owner: PendingRequestOwner,
    next_opaque: AtomicI32,
}

impl PendingHotPathHarness {
    #[must_use]
    pub fn new() -> Self {
        let table = PendingRequestTable::with_limits(PendingRequestLimits {
            admission_rate_per_second: 1_000_000_000,
            ..PendingRequestLimits::default()
        });
        let owner = table.new_owner();
        Self {
            table,
            owner,
            next_opaque: AtomicI32::new(1),
        }
    }

    pub fn boxed_mutex_completion(&self) {
        let (sender, _receiver) = tokio::sync::oneshot::channel::<rocketmq_error::RocketMQResult<RemotingCommand>>();
        let legacy = Box::new(Mutex::new(Some(sender)));
        if let Some(sender) = legacy.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take() {
            let _ = sender.send(Ok(RemotingCommand::create_response_command()));
        }
        black_box(legacy);
    }

    pub fn concrete_oneshot_completion(&self) {
        let (sender, _receiver) = tokio::sync::oneshot::channel::<rocketmq_error::RocketMQResult<RemotingCommand>>();
        let _ = sender.send(Ok(RemotingCommand::create_response_command()));
    }

    pub fn concrete_register_complete(&self) {
        let opaque = self.next_opaque.fetch_add(1, Ordering::Relaxed);
        let (sender, _receiver) = tokio::sync::oneshot::channel();
        let guard = self
            .table
            .register_for_owner(
                &self.owner,
                opaque,
                RequestDeadline::after(std::time::Duration::from_secs(30)),
                sender,
            )
            .expect("pending registration");
        assert!(self.table.complete_response_for_owner(
            &self.owner,
            opaque,
            RemotingCommand::create_response_command().set_opaque(opaque),
        ));
        black_box(guard);
    }
}

impl Default for PendingHotPathHarness {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "tls")]
pub fn tls_acceptor() -> tokio_rustls::TlsAcceptor {
    let config = crate::config::TlsConfig {
        enable: true,
        test_mode_enable: true,
        ..crate::config::TlsConfig::default()
    };
    crate::tls::build_server_acceptor(&config).expect("benchmark TLS acceptor")
}

#[cfg(feature = "tls")]
pub fn tls_connector() -> tokio_rustls::TlsConnector {
    let config = crate::config::TlsConfig {
        enable: true,
        test_mode_enable: true,
        ..crate::config::TlsConfig::default()
    };
    tokio_rustls::TlsConnector::from(Arc::new(
        crate::tls::build_client_config(&config).expect("benchmark TLS client config"),
    ))
}
