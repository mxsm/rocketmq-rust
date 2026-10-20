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
use crate::{RuntimeError, RuntimeResult, TaskGroup, TaskKind};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

const PROBE_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_PROBE_REQUEST_BYTES: usize = 2048;
/// Probe connections served at once.
///
/// A kubelet opens one connection per probe. The bound keeps idle or slow
/// clients from exhausting the process while leaving room for real probes.
const MAX_CONCURRENT_PROBE_CONNECTIONS: usize = 64;
const ACCEPT_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(10);
const ACCEPT_RETRY_MAX_DELAY: Duration = Duration::from_secs(1);
/// Consecutive resource failures after which the listener counts as broken.
const MAX_CONSECUTIVE_ACCEPT_FAILURES: u32 = 64;

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

    /// Serves probe connections until `tasks` is cancelled.
    ///
    /// Each connection is handled by its own task of `tasks`, so a slow client
    /// cannot delay the probes that follow it.
    pub(super) async fn serve_probe_requests(&self, listener: TcpListener, tasks: TaskGroup) {
        self.serve_probe_connections(|| listener.accept(), tasks).await;
    }

    async fn serve_probe_connections<A, F>(&self, mut accept: A, tasks: TaskGroup)
    where
        A: FnMut() -> F,
        F: Future<Output = io::Result<(TcpStream, SocketAddr)>>,
    {
        let cancellation = tasks.cancellation_token();
        let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_PROBE_CONNECTIONS));
        let mut failures = AcceptFailures::default();
        loop {
            // Waiting for a permit before accepting leaves excess connections
            // in the listen backlog rather than in this process.
            let permit = tokio::select! {
                _ = cancellation.cancelled() => break,
                permit = Arc::clone(&permits).acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_closed) => break,
                },
            };
            let accepted = tokio::select! {
                _ = cancellation.cancelled() => break,
                accepted = accept() => accepted,
            };
            let error = match accepted {
                Ok((stream, _peer)) => {
                    failures.consecutive = 0;
                    let lifecycle = self.clone();
                    let connection_cancellation = cancellation.clone();
                    let spawned = tasks.spawn("service-lifecycle.probe-connection", TaskKind::Worker, async move {
                        let _permit = permit;
                        tokio::select! {
                            _ = connection_cancellation.cancelled() => {}
                            () = lifecycle.handle_probe_connection(stream) => {}
                        }
                    });
                    if spawned.is_err() {
                        // The lifecycle group has closed admission and is shutting down.
                        break;
                    }
                    continue;
                }
                Err(error) => error,
            };
            match classify_accept_error(&error) {
                // The peer went away before the connection was accepted.
                AcceptErrorKind::Connection => {}
                AcceptErrorKind::Listener => {
                    self.fail_probe_listener(&error);
                    break;
                }
                AcceptErrorKind::Resource => {
                    let Some(delay) = failures.record() else {
                        self.fail_probe_listener(&error);
                        break;
                    };
                    if failures.consecutive == 1 {
                        tracing::warn!(error = %error, "service lifecycle probe accept failed; retrying");
                    } else {
                        tracing::debug!(
                            error = %error,
                            consecutive = failures.consecutive,
                            "service lifecycle probe accept failed again"
                        );
                    }
                    tokio::select! {
                        _ = cancellation.cancelled() => break,
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
    }

    fn fail_probe_listener(&self, error: &io::Error) {
        tracing::error!(error = %error, "service lifecycle probe listener is unusable");
        self.mark_failed();
        self.request_shutdown(ShutdownReason::Internal);
    }

    async fn handle_probe_connection(&self, mut stream: TcpStream) {
        let response = match tokio::time::timeout(PROBE_REQUEST_TIMEOUT, read_request_head(&mut stream)).await {
            Ok(Ok(RequestHead::Complete(head) | RequestHead::Closed(head))) => self.route_probe_request(&head),
            Ok(Ok(RequestHead::Empty | RequestHead::TooLarge)) => {
                probe_response(400, "bad_request", self.state(), None)
            }
            Ok(Err(_)) | Err(_) => probe_response(408, "request_timeout", self.state(), None),
        };
        let _ = tokio::time::timeout(PROBE_REQUEST_TIMEOUT, async {
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        })
        .await;
    }

    fn route_probe_request(&self, request: &[u8]) -> String {
        let first_line = request.split(|byte| *byte == b'\n').next().unwrap_or_default();
        let first_line = String::from_utf8_lossy(first_line);
        let mut fields = first_line.split_ascii_whitespace();
        let method = fields.next().unwrap_or_default();
        let path = fields.next().unwrap_or_default();
        if method != "GET" && method != "POST" {
            return probe_response(405, "method_not_allowed", self.state(), Some("GET, POST"));
        }
        match path {
            "/readyz" if self.is_ready() => probe_response(200, "ready", self.state(), None),
            "/readyz" => probe_response(503, "not_ready", self.state(), None),
            "/livez" if self.is_live() => probe_response(200, "live", self.state(), None),
            "/livez" => probe_response(503, "not_live", self.state(), None),
            "/drainz" => {
                let methods = self.config().drain_request_methods;
                if !methods.allows(method) {
                    return probe_response(405, "method_not_allowed", self.state(), Some(methods.allow_header()));
                }
                self.request_shutdown(ShutdownReason::PreStop);
                probe_response(200, "draining", self.state(), None)
            }
            _ => probe_response(404, "not_found", self.state(), None),
        }
    }
}

/// How the probe server reacts to a failed `accept`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcceptErrorKind {
    /// One connection failed; the next accept can follow at once.
    Connection,
    /// The process is short of a resource, such as file descriptors (EMFILE,
    /// ENFILE) or buffer memory; accepting again after a delay can succeed.
    Resource,
    /// The listener itself is unusable.
    Listener,
}

fn classify_accept_error(error: &io::Error) -> AcceptErrorKind {
    match error.kind() {
        io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionRefused
        | io::ErrorKind::Interrupted
        | io::ErrorKind::WouldBlock
        | io::ErrorKind::TimedOut => AcceptErrorKind::Connection,
        io::ErrorKind::InvalidInput | io::ErrorKind::NotConnected | io::ErrorKind::Unsupported => {
            AcceptErrorKind::Listener
        }
        _ => AcceptErrorKind::Resource,
    }
}

/// Consecutive resource failures of `accept` and the backoff between them.
#[derive(Debug, Default)]
struct AcceptFailures {
    consecutive: u32,
}

impl AcceptFailures {
    /// Records one failure and returns the delay before the next accept, or
    /// `None` once the failures have lasted long enough to show that the
    /// listener no longer recovers.
    fn record(&mut self) -> Option<Duration> {
        self.consecutive += 1;
        if self.consecutive > MAX_CONSECUTIVE_ACCEPT_FAILURES {
            return None;
        }
        let doublings = (self.consecutive - 1).min(16);
        Some(
            ACCEPT_RETRY_INITIAL_DELAY
                .saturating_mul(1 << doublings)
                .min(ACCEPT_RETRY_MAX_DELAY),
        )
    }
}

enum RequestHead {
    /// The request line and headers ended with a blank line.
    Complete(Vec<u8>),
    /// The client closed its side after sending part of a request.
    Closed(Vec<u8>),
    /// The client closed its side without sending anything.
    Empty,
    /// The headers did not end within the size limit.
    TooLarge,
}

/// Reads until the end of the request headers, which may arrive in pieces.
async fn read_request_head(stream: &mut TcpStream) -> io::Result<RequestHead> {
    let mut head = Vec::with_capacity(512);
    let mut chunk = [0_u8; 512];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(if head.is_empty() {
                RequestHead::Empty
            } else {
                RequestHead::Closed(head)
            });
        }
        head.extend_from_slice(&chunk[..read]);
        if head_is_complete(&head) {
            return Ok(RequestHead::Complete(head));
        }
        if head.len() >= MAX_PROBE_REQUEST_BYTES {
            return Ok(RequestHead::TooLarge);
        }
    }
}

fn head_is_complete(head: &[u8]) -> bool {
    head.windows(4).any(|window| window == b"\r\n\r\n") || head.windows(2).any(|window| window == b"\n\n")
}

fn probe_response(
    status: u16,
    status_text: &'static str,
    state: ServiceLifecycleState,
    allow: Option<&'static str>,
) -> String {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        _ => "Service Unavailable",
    };
    let allow = allow.map(|methods| format!("Allow: {methods}\r\n")).unwrap_or_default();
    let body = format!(r#"{{"status":"{status_text}","state":"{}"}}"#, state.as_str());
    format!(
        "HTTP/1.1 {status} {reason}\r\n{allow}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: \
         close\r\nCache-Control: no-store\r\n\r\n{body}",
        body.len()
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::service_lifecycle::DrainRequestMethods;
    use crate::service_lifecycle::ServiceLifecycleConfig;
    use crate::RuntimeContext;

    fn lifecycle(drain_request_methods: DrainRequestMethods) -> ServiceLifecycle {
        ServiceLifecycle::new(ServiceLifecycleConfig {
            service_name: Arc::from("probe-test"),
            probe_bind_addr: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            shutdown_timeout: Duration::from_secs(5),
            liveness_stale_after: Duration::from_secs(30),
            drain_request_methods,
        })
    }

    async fn send(addr: SocketAddr, request: &[u8]) -> String {
        let mut stream = TcpStream::connect(addr).await.expect("connect probe");
        stream.write_all(request).await.expect("write probe request");
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.expect("read probe response");
        String::from_utf8(response).expect("probe response is UTF-8")
    }

    fn request(method: &str, path: &str) -> Vec<u8> {
        format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").into_bytes()
    }

    #[test]
    fn drain_methods_parse_only_lists_that_keep_post() {
        assert_eq!(DrainRequestMethods::parse("POST"), Some(DrainRequestMethods::PostOnly));
        assert_eq!(
            DrainRequestMethods::parse(" post "),
            Some(DrainRequestMethods::PostOnly)
        );
        assert_eq!(
            DrainRequestMethods::parse("GET,POST"),
            Some(DrainRequestMethods::GetOrPost)
        );
        assert_eq!(
            DrainRequestMethods::parse("post, get"),
            Some(DrainRequestMethods::GetOrPost)
        );
        for invalid in ["", "GET", "GET,GET", "POST,POST", "PUT", "GET,POST,PUT", "GET;POST"] {
            assert_eq!(DrainRequestMethods::parse(invalid), None, "{invalid:?}");
        }
    }

    #[test]
    fn accept_errors_are_classified_by_what_they_break() {
        assert_eq!(
            classify_accept_error(&io::Error::from(io::ErrorKind::ConnectionAborted)),
            AcceptErrorKind::Connection
        );
        assert_eq!(
            classify_accept_error(&io::Error::from(io::ErrorKind::InvalidInput)),
            AcceptErrorKind::Listener
        );
        assert_eq!(
            classify_accept_error(&io::Error::other("too many open files")),
            AcceptErrorKind::Resource
        );
    }

    #[test]
    fn accept_backoff_grows_to_its_cap_and_then_gives_up() {
        let mut failures = AcceptFailures::default();
        assert_eq!(failures.record(), Some(ACCEPT_RETRY_INITIAL_DELAY));
        assert_eq!(failures.record(), Some(ACCEPT_RETRY_INITIAL_DELAY * 2));
        for _ in 3..=MAX_CONSECUTIVE_ACCEPT_FAILURES {
            assert!(failures.record().is_some_and(|delay| delay <= ACCEPT_RETRY_MAX_DELAY));
        }
        assert_eq!(failures.record(), None);
    }

    #[tokio::test]
    async fn drain_accepts_post_and_rejects_get_by_default() {
        let context = RuntimeContext::from_current("probe-drain-post-only");
        let service = context.service_context("probe-drain-post-only");
        let lifecycle = lifecycle(DrainRequestMethods::PostOnly);
        lifecycle.start(&service).await.unwrap();
        let addr = lifecycle.probe_local_addr().expect("bound probe address");

        let rejected = send(addr, &request("GET", "/drainz")).await;
        assert!(rejected.starts_with("HTTP/1.1 405"), "{rejected}");
        assert!(rejected.contains("\r\nAllow: POST\r\n"), "{rejected}");
        assert!(lifecycle.shutdown_request().is_none());
        assert!(send(addr, &request("GET", "/livez")).await.starts_with("HTTP/1.1 200"));
        assert!(send(addr, &request("POST", "/livez")).await.starts_with("HTTP/1.1 200"));

        let accepted = send(addr, &request("POST", "/drainz")).await;
        assert!(accepted.starts_with("HTTP/1.1 200"), "{accepted}");
        assert_eq!(lifecycle.wait_for_shutdown().await.reason, ShutdownReason::PreStop);

        let report = context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn drain_accepts_get_after_opt_in() {
        let context = RuntimeContext::from_current("probe-drain-get-or-post");
        let service = context.service_context("probe-drain-get-or-post");
        let lifecycle = lifecycle(DrainRequestMethods::GetOrPost);
        lifecycle.start(&service).await.unwrap();
        let addr = lifecycle.probe_local_addr().expect("bound probe address");

        let rejected = send(addr, &request("PUT", "/drainz")).await;
        assert!(rejected.starts_with("HTTP/1.1 405"), "{rejected}");
        assert!(rejected.contains("\r\nAllow: GET, POST\r\n"), "{rejected}");
        let accepted = send(addr, &request("GET", "/drainz")).await;
        assert!(accepted.starts_with("HTTP/1.1 200"), "{accepted}");
        assert_eq!(lifecycle.wait_for_shutdown().await.reason, ShutdownReason::PreStop);

        let report = context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn idle_connections_do_not_delay_readiness_probes() {
        let context = RuntimeContext::from_current("probe-idle-connections");
        let service = context.service_context("probe-idle-connections");
        let lifecycle = lifecycle(DrainRequestMethods::PostOnly);
        lifecycle.start(&service).await.unwrap();
        lifecycle.mark_ready().unwrap();
        let addr = lifecycle.probe_local_addr().expect("bound probe address");

        let mut idle = Vec::new();
        for _ in 0..20 {
            idle.push(TcpStream::connect(addr).await.expect("connect idle client"));
        }
        let response = tokio::time::timeout(Duration::from_millis(100), send(addr, &request("GET", "/readyz")))
            .await
            .expect("an idle client must not delay a readiness probe");
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        drop(idle);

        let report = context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn a_request_that_arrives_in_pieces_is_routed() {
        let context = RuntimeContext::from_current("probe-fragmented-request");
        let service = context.service_context("probe-fragmented-request");
        let lifecycle = lifecycle(DrainRequestMethods::PostOnly);
        lifecycle.start(&service).await.unwrap();
        let addr = lifecycle.probe_local_addr().expect("bound probe address");

        let mut stream = TcpStream::connect(addr).await.expect("connect probe");
        let request = request("GET", "/livez");
        for piece in request.chunks(5) {
            stream.write_all(piece).await.expect("write request piece");
            stream.flush().await.expect("flush request piece");
            tokio::task::yield_now().await;
        }
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.expect("read probe response");
        let response = String::from_utf8(response).expect("probe response is UTF-8");
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");

        let report = context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn resource_exhaustion_on_accept_is_retried_without_failing_the_service() {
        #[cfg(windows)]
        const EMFILE: i32 = 10024;
        #[cfg(not(windows))]
        const EMFILE: i32 = 24;

        let context = RuntimeContext::from_current("probe-accept-emfile");
        let service = context.service_context("probe-accept-emfile");
        let tasks = service.task_group().clone();
        let lifecycle = lifecycle(DrainRequestMethods::PostOnly);
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("bind probe listener");
        let addr = listener.local_addr().expect("probe listener address");
        let injected = Arc::new(AtomicUsize::new(3));

        let server = {
            let lifecycle = lifecycle.clone();
            let injected = Arc::clone(&injected);
            let tasks = tasks.clone();
            tokio::spawn(async move {
                let listener = &listener;
                lifecycle
                    .serve_probe_connections(
                        || {
                            let fail = injected
                                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| left.checked_sub(1))
                                .is_ok();
                            async move {
                                if fail {
                                    Err(io::Error::from_raw_os_error(EMFILE))
                                } else {
                                    listener.accept().await
                                }
                            }
                        },
                        tasks,
                    )
                    .await;
            })
        };

        let response = send(addr, &request("GET", "/livez")).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert_eq!(injected.load(Ordering::Acquire), 0);
        assert_ne!(lifecycle.state(), ServiceLifecycleState::Failed);
        assert!(lifecycle.shutdown_request().is_none());

        tasks.cancel();
        server.await.expect("probe server exits after cancellation");
        let report = context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}
