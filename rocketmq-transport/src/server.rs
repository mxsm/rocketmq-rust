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
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use rocketmq_error::RocketMQResult;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;

use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::config::TlsConfig;
use crate::config::TlsMode;
use crate::tls::TlsServerRuntime;

pub trait RequestProcessor: Send + Sync + 'static {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>>;
}

#[derive(Debug, Clone)]
pub struct TransportServerConfig {
    pub bind_address: SocketAddr,
    pub tls: TlsConfig,
    pub handshake_timeout: Duration,
    pub request_timeout: Duration,
}

impl TransportServerConfig {
    pub fn loopback() -> Self {
        let mut tls = TlsConfig::default();
        tls.server.mode = TlsMode::Disabled;
        Self {
            bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            tls,
            handshake_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
        }
    }
}

pub struct TransportServer {
    local_addr: SocketAddr,
    listener: Mutex<Option<tokio::net::TcpListener>>,
    service_context: ServiceContext,
    config: TransportServerConfig,
    processor: Arc<dyn RequestProcessor>,
    admission: Arc<AdmissionController>,
    tls: TlsServerRuntime,
    started: AtomicBool,
    next_session: AtomicU64,
}

impl TransportServer {
    pub async fn bind(
        service_context: ServiceContext,
        config: TransportServerConfig,
        processor: Arc<dyn RequestProcessor>,
        admission: Arc<AdmissionController>,
    ) -> RocketMQResult<Arc<Self>> {
        let listener = tokio::net::TcpListener::bind(config.bind_address).await?;
        let local_addr = listener.local_addr()?;
        let tls = TlsServerRuntime::new_with_service_context(config.tls.clone(), &service_context);
        Ok(Arc::new(Self {
            local_addr,
            listener: Mutex::new(Some(listener)),
            service_context,
            config,
            processor,
            admission,
            tls,
            started: AtomicBool::new(false),
            next_session: AtomicU64::new(1),
        }))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn start(self: &Arc<Self>) -> RuntimeResult<()> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let listener = self
            .listener
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or(RuntimeError::TaskGroupClosing {
                group_id: self.service_context.task_group().id(),
                group_name: self.service_context.task_group().name().into(),
            })?;
        let server = self.clone();
        let cancellation = self.service_context.task_group().cancellation_token();
        self.service_context.spawn_service("transport.accept", async move {
            loop {
                let accepted = tokio::select! {
                    () = cancellation.cancelled() => break,
                    accepted = listener.accept() => accepted,
                };
                let Ok((stream, remote_addr)) = accepted else {
                    break;
                };
                let session_id = server.next_session.fetch_add(1, Ordering::Relaxed);
                let scope = AdmissionScope::new(remote_addr.ip()).with_session(session_id);
                let Ok(connection_permit) =
                    server
                        .admission
                        .try_acquire(AdmissionResource::Connection, scope, 0, AdmissionClass::Data)
                else {
                    drop(stream);
                    continue;
                };
                let session = server.clone();
                let session_context = server.service_context.child(format!("transport.session.{session_id}"));
                let session_task_context = session_context.clone();
                if session_context
                    .spawn_service("transport.session", async move {
                        let _connection_permit = connection_permit;
                        session
                            .run_session(stream, remote_addr, session_id, session_task_context)
                            .await;
                    })
                    .is_err()
                {
                    break;
                }
            }
        })?;
        Ok(())
    }

    async fn run_session(
        self: Arc<Self>,
        stream: tokio::net::TcpStream,
        remote_addr: SocketAddr,
        session_id: u64,
        session_context: ServiceContext,
    ) {
        let scope = AdmissionScope::new(remote_addr.ip()).with_session(session_id);
        let Ok(_handshake_permit) =
            self.admission
                .try_acquire(AdmissionResource::Handshake, scope, 0, AdmissionClass::Data)
        else {
            return;
        };
        let handshake_deadline = tokio::time::Instant::now() + self.config.handshake_timeout;
        let Ok(Some(mut connection)) =
            tokio::time::timeout_at(handshake_deadline, self.tls.into_connection(stream, remote_addr)).await
        else {
            return;
        };
        drop(_handshake_permit);

        loop {
            let request_deadline = tokio::time::Instant::now() + self.config.request_timeout;
            let request = match tokio::time::timeout_at(request_deadline, connection.receive_command()).await {
                Ok(Some(Ok(request))) => request,
                Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
            };
            let bytes = request.body().map_or(0, bytes::Bytes::len);
            let Ok(_inflight) =
                self.admission
                    .try_acquire(AdmissionResource::Inflight, scope, bytes, AdmissionClass::Data)
            else {
                break;
            };
            let Ok(_queued) = self
                .admission
                .try_acquire(AdmissionResource::Queued, scope, bytes, AdmissionClass::Data)
            else {
                break;
            };
            let Ok(_processor) =
                self.admission
                    .try_acquire(AdmissionResource::Processor, scope, bytes, AdmissionClass::Data)
            else {
                break;
            };
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let processor = self.processor.clone();
            let processor_context = session_context.child("transport.processor");
            let processor_task = match processor_context.spawn_service("transport.processor", async move {
                let _ = sender.send(processor.process(request).await);
            }) {
                Ok(task_id) => task_id,
                Err(_) => break,
            };
            let response = match tokio::time::timeout_at(request_deadline, receiver).await {
                Ok(Ok(Ok(response))) => response,
                Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => {
                    processor_context.task_group().abort_task(processor_task);
                    break;
                }
            };
            if tokio::time::timeout_at(request_deadline, connection.send_command(response))
                .await
                .ok()
                .and_then(Result::ok)
                .is_none()
            {
                break;
            }
        }
        let _ = connection.shutdown().await;
    }

    pub async fn shutdown_until(&self, deadline: ShutdownDeadline) -> ShutdownReport {
        self.tls.shutdown();
        self.service_context.task_group().shutdown_until(deadline).await
    }
}
