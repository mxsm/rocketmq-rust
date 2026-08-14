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

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use arc_swap::ArcSwap;
use parking_lot::RwLock;
use rocketmq_proxy_core::GrpcTlsClientAuth;
use rocketmq_proxy_core::GrpcTlsConfig;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::TaskGroup;
use rocketmq_transport::api::v1::TlsClientAuth;
use rocketmq_transport::api::v1::TlsConfig;
use rocketmq_transport::api::v1::TlsMode;
use rocketmq_transport::api::v1::TlsServerConfig;
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

use crate::ProxyError;
use crate::ProxyResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GrpcTlsReloadHealth {
    pub(crate) active_generation: u64,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TlsFileSnapshot {
    path: String,
    modified: Option<SystemTime>,
    len: Option<u64>,
}

#[derive(Clone)]
pub(crate) struct ReloadableGrpcTlsAcceptor {
    config: Arc<GrpcTlsConfig>,
    active: Arc<ArcSwap<TlsAcceptor>>,
    observed_snapshot: Arc<RwLock<Vec<TlsFileSnapshot>>>,
    health: Arc<RwLock<GrpcTlsReloadHealth>>,
    blocking: BlockingExecutor,
}

impl ReloadableGrpcTlsAcceptor {
    pub(crate) async fn initialize(config: GrpcTlsConfig, blocking: BlockingExecutor) -> ProxyResult<Self> {
        config.validate()?;
        let build_config = config.clone();
        let (acceptor, snapshot) = blocking
            .spawn_io("proxy.grpc.tls.initialize", move || {
                let snapshot = tls_file_snapshot(&build_config);
                let acceptor = build_acceptor(&build_config);
                (acceptor, snapshot)
            })
            .await
            .map_err(|error| ProxyError::Transport {
                message: format!("failed to initialize Proxy gRPC TLS material: {error}"),
            })?;
        let acceptor = acceptor?;
        Ok(Self {
            config: Arc::new(config),
            active: Arc::new(ArcSwap::from_pointee(acceptor)),
            observed_snapshot: Arc::new(RwLock::new(snapshot)),
            health: Arc::new(RwLock::new(GrpcTlsReloadHealth {
                active_generation: 1,
                last_error: None,
            })),
            blocking,
        })
    }

    pub(crate) fn start_reload(&self, task_group: &TaskGroup) -> ProxyResult<()> {
        let runtime = self.clone();
        let interval = Duration::from_millis(self.config.reload_interval_ms);
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.clone());
        let mut schedule = ScheduledTaskConfig::fixed_rate_no_overlap("proxy.grpc.tls-reload", interval);
        schedule.initial_delay = interval;
        scheduled_tasks
            .schedule_fixed_rate_no_overlap(schedule, move || {
                let runtime = runtime.clone();
                async move {
                    runtime.reload_if_changed().await;
                }
            })
            .map_err(|error| ProxyError::Transport {
                message: format!("failed to start Proxy gRPC TLS reload task: {error}"),
            })?;
        Ok(())
    }

    pub(crate) async fn accept(&self, stream: TcpStream) -> std::io::Result<TlsStream<TcpStream>> {
        self.active
            .load_full()
            .accept(stream)
            .await
            .map_err(|error| std::io::Error::other(format!("Proxy gRPC TLS handshake failed: {error}")))
    }

    async fn reload_if_changed(&self) {
        let snapshot_config = Arc::clone(&self.config);
        let next_snapshot = match self
            .blocking
            .spawn_io("proxy.grpc.tls.snapshot", move || tls_file_snapshot(&snapshot_config))
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.record_reload_error(format!("TLS file inspection failed: {error}"));
                return;
            }
        };
        if *self.observed_snapshot.read() == next_snapshot {
            return;
        }
        *self.observed_snapshot.write() = next_snapshot;

        let build_config = Arc::clone(&self.config);
        let candidate = match self
            .blocking
            .spawn_io("proxy.grpc.tls.reload", move || build_acceptor(&build_config))
            .await
        {
            Ok(candidate) => candidate,
            Err(error) => {
                self.record_reload_error(format!("TLS reload work failed: {error}"));
                return;
            }
        };
        match candidate {
            Ok(acceptor) => {
                self.active.store(Arc::new(acceptor));
                let mut health = self.health.write();
                health.active_generation = health.active_generation.saturating_add(1);
                health.last_error = None;
                tracing::info!(
                    generation = health.active_generation,
                    "Proxy gRPC TLS material generation reloaded"
                );
            }
            Err(error) => self.record_reload_error(error.to_string()),
        }
    }

    fn record_reload_error(&self, reason: String) {
        self.health.write().last_error = Some(reason);
        tracing::warn!("Proxy gRPC TLS material reload was rejected; retaining last-known-good generation");
    }
}

fn build_acceptor(config: &GrpcTlsConfig) -> rocketmq_error::RocketMQResult<TlsAcceptor> {
    rocketmq_transport::api::v1::build_server_acceptor_exact_with_alpn(&transport_tls_config(config), &[b"h2".to_vec()])
}

fn transport_tls_config(config: &GrpcTlsConfig) -> TlsConfig {
    let need_client_auth = match config.client_auth {
        GrpcTlsClientAuth::None => TlsClientAuth::None,
        GrpcTlsClientAuth::Optional => TlsClientAuth::Optional,
        GrpcTlsClientAuth::Require => TlsClientAuth::Require,
    };
    TlsConfig {
        enable: true,
        server: TlsServerConfig {
            mode: TlsMode::Enforcing,
            need_client_auth,
            key_path: Some(config.private_key_path.clone()),
            key_password: config.private_key_password.clone(),
            cert_path: Some(config.certificate_path.clone()),
            auth_client: need_client_auth != TlsClientAuth::None,
            trust_cert_path: config.client_ca_path.clone(),
        },
        ..TlsConfig::default()
    }
}

fn tls_file_snapshot(config: &GrpcTlsConfig) -> Vec<TlsFileSnapshot> {
    let mut paths = vec![config.certificate_path.as_str(), config.private_key_path.as_str()];
    if let Some(client_ca_path) = config.client_ca_path.as_deref() {
        paths.push(client_ca_path);
    }
    paths.into_iter().map(file_snapshot).collect()
}

fn file_snapshot(path: &str) -> TlsFileSnapshot {
    let metadata = fs::metadata(Path::new(path));
    TlsFileSnapshot {
        path: path.to_owned(),
        modified: metadata.as_ref().ok().and_then(|metadata| metadata.modified().ok()),
        len: metadata.as_ref().ok().map(fs::Metadata::len),
    }
}
