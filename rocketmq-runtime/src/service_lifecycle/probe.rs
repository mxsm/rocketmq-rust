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

//! Probe transport and HTTP routing through lifecycle state APIs.

use super::{ServiceLifecycle, ServiceLifecycleState, ShutdownReason};
use crate::{RuntimeError, RuntimeResult};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const PROBE_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_PROBE_REQUEST_BYTES: usize = 2048;

impl ServiceLifecycle {
    pub(super) async fn bind_probe_listener(&self) -> RuntimeResult<Option<(TcpListener, SocketAddr)>> {
        let Some(bind_addr) = self.config().probe_bind_addr else {
            return Ok(None);
        };
        let listener = TcpListener::bind(bind_addr)
            .await
            .map_err(|error| RuntimeError::internal(crate::RuntimeOperation::BindServiceHealthProbe, error))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| RuntimeError::internal(crate::RuntimeOperation::InspectServiceHealthProbe, error))?;
        Ok(Some((listener, local_addr)))
    }

    pub(super) async fn serve_probe_requests(
        &self,
        listener: TcpListener,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => self.handle_probe_connection(stream).await,
                        Err(error) => {
                            tracing::warn!(error = %error, "service lifecycle probe accept failed");
                            self.mark_failed();
                            self.request_shutdown(ShutdownReason::Internal);
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn handle_probe_connection(&self, mut stream: TcpStream) {
        let mut request = vec![0_u8; MAX_PROBE_REQUEST_BYTES];
        let read = tokio::time::timeout(PROBE_REQUEST_TIMEOUT, stream.read(&mut request)).await;
        let response = match read {
            Ok(Ok(0)) => probe_response(400, "bad_request", self.state()),
            Ok(Ok(length)) => self.route_probe_request(&request[..length]),
            Ok(Err(_)) | Err(_) => probe_response(408, "request_timeout", self.state()),
        };
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
    }

    fn route_probe_request(&self, request: &[u8]) -> String {
        let first_line = request.split(|byte| *byte == b'\n').next().unwrap_or_default();
        let first_line = String::from_utf8_lossy(first_line);
        let mut fields = first_line.split_ascii_whitespace();
        let method = fields.next().unwrap_or_default();
        let path = fields.next().unwrap_or_default();
        if method != "GET" && method != "POST" {
            return probe_response(405, "method_not_allowed", self.state());
        }
        match path {
            "/readyz" if self.is_ready() => probe_response(200, "ready", self.state()),
            "/readyz" => probe_response(503, "not_ready", self.state()),
            "/livez" if self.is_live() => probe_response(200, "live", self.state()),
            "/livez" => probe_response(503, "not_live", self.state()),
            "/drainz" => {
                self.request_shutdown(ShutdownReason::PreStop);
                probe_response(200, "draining", self.state())
            }
            _ => probe_response(404, "not_found", self.state()),
        }
    }
}

fn probe_response(status: u16, status_text: &'static str, state: ServiceLifecycleState) -> String {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        _ => "Service Unavailable",
    };
    let body = format!(r#"{{"status":"{status_text}","state":"{}"}}"#, state.as_str());
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: \
         close\r\nCache-Control: no-store\r\n\r\n{body}",
        body.len()
    )
}
