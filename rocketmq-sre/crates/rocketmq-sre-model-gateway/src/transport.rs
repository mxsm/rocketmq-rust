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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::pin::Pin;

use rocketmq_sre_contracts::CorrelationId;
use serde_json::Value;

use crate::error::ProviderError;
use crate::profile::ProviderDialect;
use crate::secret::SecretMaterial;
use crate::stream::AsyncBoundedModelStream;
use crate::stream::BoundedModelStream;
use crate::stream::CancellationToken;
use crate::stream::StreamBounds;

/// Injected network-boundary request.
#[derive(Clone)]
pub struct TransportRequest {
    pub correlation_id: CorrelationId,
    pub dialect: ProviderDialect,
    pub endpoint: String,
    pub path: String,
    pub body: Value,
    pub credential: Option<SecretMaterial>,
    pub deadline_unix_ms: Option<u64>,
    pub max_response_bytes: usize,
}

impl Debug for TransportRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransportRequest")
            .field("correlation_id", &self.correlation_id)
            .field("dialect", &self.dialect)
            .field("endpoint", &"[ENDPOINT REDACTED]")
            .field("path", &self.path)
            .field("body", &"[MODEL PAYLOAD REDACTED]")
            .field("credential", &self.credential.as_ref().map(|_| "[SECRET REDACTED]"))
            .field("deadline_unix_ms", &self.deadline_unix_ms)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

/// Injected transport response.
#[derive(Clone)]
pub struct TransportResponse {
    pub status: u16,
    pub body: Value,
}

impl Debug for TransportResponse {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransportResponse")
            .field("status", &self.status)
            .field("body", &"[MODEL PAYLOAD REDACTED]")
            .finish()
    }
}

/// Network transport injection boundary.
///
/// This crate supplies protocol translation and never imports a cloud SDK. A
/// service integration owns its HTTP/gRPC client, TLS roots, connection pools,
/// runtime lifecycle, and timeouts behind this trait. Implementations must use
/// only model-provider credentials from [`TransportRequest::credential`]; they
/// must not receive RocketMQ, MCP, or executor credentials.
pub trait ModelTransport: Send + Sync {
    /// Sends one bounded request.
    ///
    /// # Errors
    ///
    /// Returns a stable redacted [`ProviderError`].
    fn invoke(&self, request: TransportRequest) -> Result<TransportResponse, ProviderError>;

    /// Starts an optional bounded stream.
    ///
    /// # Errors
    ///
    /// Returns capability-unsupported unless the transport implements
    /// streaming without unbounded delta accumulation.
    fn invoke_stream(
        &self,
        _request: TransportRequest,
        _bounds: StreamBounds,
        _cancellation: CancellationToken,
    ) -> Result<BoundedModelStream, ProviderError> {
        Err(ProviderError::capability_unsupported(
            "injected transport does not implement streaming",
        ))
    }
}

/// Heap-owned future returned by the object-safe asynchronous transport.
pub type TransportFuture<'a> = Pin<Box<dyn Future<Output = Result<TransportResponse, ProviderError>> + Send + 'a>>;

/// Heap-owned future returned by an object-safe asynchronous stream transport.
pub type TransportStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AsyncBoundedModelStream, ProviderError>> + Send + 'a>>;

/// Non-blocking model-provider transport used by production HTTP integrations.
///
/// This is additive to [`ModelTransport`], whose synchronous shape is retained
/// for compatibility with deterministic fixtures and existing adapters.
/// Implementations must not create nested runtimes, call `block_on`, or perform
/// blocking network or filesystem work on the async caller's thread.
pub trait AsyncModelTransport: Send + Sync {
    /// Sends one bounded request without blocking the async runtime.
    fn invoke(&self, request: TransportRequest) -> TransportFuture<'_>;

    /// Starts a bounded, cancellation-aware stream without blocking the async
    /// runtime or spawning an unowned producer task.
    fn invoke_stream(
        &self,
        _request: TransportRequest,
        _bounds: StreamBounds,
        _cancellation: CancellationToken,
    ) -> TransportStreamFuture<'_> {
        Box::pin(async {
            Err(ProviderError::capability_unsupported(
                "asynchronous transport does not implement streaming",
            ))
        })
    }
}
