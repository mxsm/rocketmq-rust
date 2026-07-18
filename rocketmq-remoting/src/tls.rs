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
use std::time::Duration;

#[cfg(feature = "tls")]
use rocketmq_runtime::BlockingExecutor;
#[cfg(feature = "tls")]
use rocketmq_runtime::BlockingPoolPolicy;
#[cfg(feature = "tls")]
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::ShutdownReport;
#[cfg(feature = "tls")]
use rocketmq_runtime::TaskGroup;
#[cfg(feature = "tls")]
pub use rocketmq_transport::tls::build_client_config;
#[cfg(feature = "tls")]
pub use rocketmq_transport::tls::build_server_acceptor;
pub use rocketmq_transport::tls::connect_tls_stream;
#[cfg(feature = "tls")]
pub use rocketmq_transport::tls::load_certificates;
pub use rocketmq_transport::tls::tls_disabled_error;
pub use rocketmq_transport::tls::TlsConfig;
pub use rocketmq_transport::tls::TlsMode;
pub use rocketmq_transport::tls::TlsReloadReport;
pub use rocketmq_transport::tls::TLS_DISABLED_ERROR_REASON;
use tokio::net::TcpStream;
#[cfg(feature = "tls")]
use tracing::warn;

use crate::connection::Connection;

/// Compatibility adapter for the transport-owned TLS runtime.
#[derive(Clone)]
pub struct TlsServerRuntime {
    inner: rocketmq_transport::tls::TlsServerRuntime,
}

impl TlsServerRuntime {
    /// Initializes TLS using the lifecycle-owned [`BlockingExecutor`] for certificate I/O.
    ///
    /// The canonical transport initializer owns initial file loading on the injected blocking
    /// executor and parents certificate reload work beneath `service_context`.
    ///
    /// # Errors
    ///
    /// Returns the canonical transport error if initial blocking work cannot be scheduled or
    /// joined.
    ///
    /// [`BlockingExecutor`]: rocketmq_runtime::BlockingExecutor
    pub async fn initialize_with_service_context(
        base_config: TlsConfig,
        service_context: &ServiceContext,
    ) -> rocketmq_error::RocketMQResult<Self> {
        Ok(Self {
            inner: rocketmq_transport::tls::TlsServerRuntime::initialize_with_service_context(
                base_config,
                service_context,
            )
            .await?,
        })
    }

    pub fn new(base_config: TlsConfig) -> Self {
        #[cfg(feature = "tls")]
        {
            let inner = match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    let runtime = RuntimeHandle::new(handle);
                    let task_group = TaskGroup::root("rocketmq-remoting.tls", runtime);
                    match BlockingExecutor::new(
                        BlockingPoolPolicy::default(),
                        task_group.child("rocketmq-remoting.tls.blocking-reaper"),
                    ) {
                        Ok(blocking) => rocketmq_transport::tls::TlsServerRuntime::new_with_task_group_and_blocking(
                            base_config,
                            task_group,
                            blocking,
                        ),
                        Err(error) => {
                            warn!(?error, "failed to create TLS reload BlockingExecutor");
                            rocketmq_transport::tls::TlsServerRuntime::new(base_config)
                        }
                    }
                }
                Err(error) => {
                    warn!(?error, "failed to start TLS reload task outside Tokio runtime");
                    rocketmq_transport::tls::TlsServerRuntime::new(base_config)
                }
            };
            Self { inner }
        }

        #[cfg(not(feature = "tls"))]
        {
            Self {
                inner: rocketmq_transport::tls::TlsServerRuntime::new(base_config),
            }
        }
    }

    pub fn new_with_service_context(base_config: TlsConfig, service_context: &ServiceContext) -> Self {
        Self {
            inner: rocketmq_transport::tls::TlsServerRuntime::new_with_service_context(base_config, service_context),
        }
    }

    pub fn mode(&self) -> TlsMode {
        self.inner.mode()
    }

    pub fn active_generation(&self) -> u64 {
        self.inner.active_generation()
    }

    #[cfg(feature = "tls")]
    pub async fn reload_now_with_report(&self) -> rocketmq_error::RocketMQResult<TlsReloadReport> {
        self.inner.reload_now_with_report().await
    }

    pub(crate) fn transport_runtime(&self) -> rocketmq_transport::tls::TlsServerRuntime {
        self.inner.clone()
    }

    pub async fn into_connection(&self, stream: TcpStream, remote_addr: SocketAddr) -> Option<Connection> {
        self.inner.into_connection(stream, remote_addr).await
    }

    pub fn shutdown(&self) {
        self.inner.shutdown();
    }

    pub async fn shutdown_gracefully(&self, timeout: Duration) -> Option<ShutdownReport> {
        self.inner.shutdown_gracefully(timeout).await
    }
}
