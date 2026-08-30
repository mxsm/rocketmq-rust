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

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_sre_contracts::CorrelationId;

use crate::error::ProviderError;
use crate::error::ProviderErrorCode;
use crate::ir::CanonicalEmbeddingRequest;
use crate::ir::CanonicalEmbeddingResponse;
use crate::ir::CanonicalModelRequest;
use crate::ir::CanonicalModelResponse;
use crate::ir::CanonicalRerankRequest;
use crate::ir::CanonicalRerankResponse;
use crate::profile::ProviderCapabilities;
use crate::profile::ProviderHealth;
use crate::stream::BoundedModelStream;
use crate::stream::CancellationToken;
use crate::stream::StreamBounds;

/// Invocation controls shared by provider and transport boundaries.
#[derive(Clone, Debug)]
pub struct InvocationContext {
    pub correlation_id: CorrelationId,
    pub deadline_unix_ms: Option<u64>,
    pub cancellation: CancellationToken,
    pub stream_bounds: StreamBounds,
    pub max_response_bytes: usize,
}

impl InvocationContext {
    /// Creates a context with finite response and stream bounds.
    #[must_use]
    pub fn new(correlation_id: CorrelationId) -> Self {
        Self {
            correlation_id,
            deadline_unix_ms: None,
            cancellation: CancellationToken::default(),
            stream_bounds: StreamBounds::default(),
            max_response_bytes: 4 * 1024 * 1024,
        }
    }

    /// Validates cancellation and deadline before an external call.
    ///
    /// # Errors
    ///
    /// Returns cancellation or timeout if the invocation must not start.
    pub fn ensure_active(&self) -> Result<(), ProviderError> {
        if self.cancellation.is_cancelled() {
            return Err(ProviderError::new(
                ProviderErrorCode::Cancelled,
                "model invocation was cancelled",
            ));
        }
        if let Some(deadline) = self.deadline_unix_ms
            && current_unix_ms() >= deadline
        {
            return Err(ProviderError::timeout("model invocation deadline has expired"));
        }
        Ok(())
    }
}

/// Provider-neutral chat boundary.
pub trait ChatModelProvider: Send + Sync {
    /// Stable profile identifier implemented by this provider instance.
    fn profile_id(&self) -> &str;

    /// Current capability declaration.
    fn capabilities(&self) -> ProviderCapabilities;

    /// Current health used for routing.
    fn health(&self) -> ProviderHealth;

    /// Executes one bounded canonical chat invocation.
    ///
    /// # Errors
    ///
    /// Returns a stable redacted [`ProviderError`].
    fn invoke(
        &self,
        context: &InvocationContext,
        request: &CanonicalModelRequest,
    ) -> Result<CanonicalModelResponse, ProviderError>;

    /// Starts a bounded canonical stream.
    ///
    /// # Errors
    ///
    /// Returns capability-unsupported by default or a stable provider error.
    fn invoke_stream(
        &self,
        _context: &InvocationContext,
        _request: &CanonicalModelRequest,
    ) -> Result<BoundedModelStream, ProviderError> {
        Err(ProviderError::capability_unsupported(
            "provider does not implement streaming",
        ))
    }
}

/// Provider-neutral embedding boundary.
pub trait EmbeddingProvider: Send + Sync {
    /// Stable profile identifier.
    fn profile_id(&self) -> &str;

    /// Executes one bounded embedding request.
    ///
    /// # Errors
    ///
    /// Returns a stable redacted [`ProviderError`].
    fn embed(
        &self,
        context: &InvocationContext,
        request: &CanonicalEmbeddingRequest,
    ) -> Result<CanonicalEmbeddingResponse, ProviderError>;
}

/// Provider-neutral reranking boundary.
pub trait RerankProvider: Send + Sync {
    /// Stable profile identifier.
    fn profile_id(&self) -> &str;

    /// Executes one bounded reranking request.
    ///
    /// # Errors
    ///
    /// Returns a stable redacted [`ProviderError`].
    fn rerank(
        &self,
        context: &InvocationContext,
        request: &CanonicalRerankRequest,
    ) -> Result<CanonicalRerankResponse, ProviderError>;
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}
