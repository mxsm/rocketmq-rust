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
    pub fn new(base_config: TlsConfig) -> Self {
        #[cfg(feature = "tls")]
        {
            let inner = match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    let runtime = RuntimeHandle::new(handle);
                    let task_group = TaskGroup::root("rocketmq-remoting.tls", runtime);
                    rocketmq_transport::tls::TlsServerRuntime::new_with_task_group(base_config, task_group)
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
