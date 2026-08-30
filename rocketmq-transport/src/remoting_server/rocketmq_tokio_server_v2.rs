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

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use crate::codec::remoting_command_codec::FrameLimits;
use crate::config::ServerConfig;
use crate::file_region::FileTransferMode;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::SharedRocketMQError;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_security_api::Principal;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::admission::ResourceLimit;
use crate::proxy_protocol::ProxyProtocolConfig;
use crate::runtime::RPCHook;
use crate::security::TransportSecurity;
use crate::server::TransportListener;
use crate::telemetry::TransportTelemetry;
use crate::tls::TlsServerRuntime;

/// Default limit the max number of connections.
const DEFAULT_MAX_CONNECTIONS: usize = 1000;

const DEFAULT_TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Typed failure reported before a remoting server can accept connections.
///
/// Compatibility entry points retain their historic `Option` and `()` return
/// values. New composition roots should use the `try_*` server methods so a
/// failed startup remains visible to their lifecycle owner.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ServerStartError {
    /// Server configuration or an input capability was invalid.
    Configuration {
        /// The startup capability whose configuration validation failed.
        stage: &'static str,
        /// A human-readable summary of the validation failure.
        detail: String,
        /// The original typed configuration error preserved for compatibility mapping.
        source: SharedRocketMQError,
    },
    /// TLS runtime initialization failed.
    Tls { stage: &'static str, detail: String },
    /// The configured address could not be bound.
    Bind {
        stage: &'static str,
        address: String,
        detail: String,
    },
    /// The local address of a bound listener could not be read.
    LocalAddress {
        stage: &'static str,
        address: String,
        detail: String,
    },
    /// Connection or request admission could not be initialized.
    Admission { stage: &'static str, detail: String },
    /// The authorized command dispatcher could not be initialized.
    Dispatcher { stage: &'static str, detail: String },
    /// A lifecycle-owned startup task could not be created.
    TaskSpawn { stage: &'static str, detail: String },
}

impl std::fmt::Display for ServerStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration { stage, detail, .. }
            | Self::Tls { stage, detail }
            | Self::Admission { stage, detail }
            | Self::Dispatcher { stage, detail }
            | Self::TaskSpawn { stage, detail } => write!(formatter, "{stage}: {detail}"),
            Self::Bind { stage, address, detail } | Self::LocalAddress { stage, address, detail } => {
                write!(formatter, "{stage} at {address}: {detail}")
            }
        }
    }
}

impl std::error::Error for ServerStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Configuration { source, .. } => Some(source),
            Self::Tls { .. }
            | Self::Bind { .. }
            | Self::LocalAddress { .. }
            | Self::Admission { .. }
            | Self::Dispatcher { .. }
            | Self::TaskSpawn { .. } => None,
        }
    }
}

#[path = "rocketmq_tokio_server/connection_handler_v2_only.rs"]
mod connection_handler;
#[path = "rocketmq_tokio_server/shutdown_v2.rs"]
mod shutdown;
#[path = "rocketmq_tokio_server/v2_server.rs"]
mod v2_server;

pub use v2_server::TransportServerV2;
