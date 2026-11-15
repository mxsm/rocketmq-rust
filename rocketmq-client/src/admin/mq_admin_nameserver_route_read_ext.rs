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

//! Exact-target, uncached NameServer reads for trusted read-only adapters.

use cheetah_string::CheetahString;

use super::default_mq_admin_ext::DefaultMQAdminExt;
use super::mq_admin_infrastructure_observation_read_ext::{sanitized_error, InfrastructureObservationReadError};

/// The only native operations admitted by the observation primitive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NameserverReadOperation {
    ViewConfiguration,
    TopicRoute { topic: String },
}

/// Closed response classification; remote remarks are never retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NameserverReplyStatus {
    Success,
    TopicAbsent,
    PermissionDenied,
    Unsupported,
    Unavailable,
    Invalid,
    TooLarge,
}

/// Bounded private wire body. It has no Serde implementation and its debug
/// representation never includes the config/route body or target endpoint.
pub struct NameserverReadReply {
    pub status: NameserverReplyStatus,
    pub received_body_bytes: usize,
    body: Vec<u8>,
}

impl NameserverReadReply {
    pub(crate) fn new(status: NameserverReplyStatus, received_body_bytes: usize, body: Vec<u8>) -> Self {
        Self {
            status,
            received_body_bytes,
            body,
        }
    }

    /// Private adapter material, not a public evidence/logging representation.
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

impl std::fmt::Debug for NameserverReadReply {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NameserverReadReply")
            .field("status", &self.status)
            .field("received_body_bytes", &self.received_body_bytes)
            .finish_non_exhaustive()
    }
}

/// Pure reads to a trusted configured endpoint. No pooled selection, retry,
/// default-Topic fallback, Broker heartbeat or QueryDataVersion is performed.
#[allow(async_fn_in_trait)]
pub trait MQAdminNameserverRouteReadExt: Send {
    /// Reads one bounded response using the caller's remaining time budget.
    ///
    /// # Errors
    /// Returns an address-free typed failure on invalid input, session,
    /// transport or deadline failure. Response-body bounds apply before decode,
    /// after the existing transport frame bound, not at the socket byte level.
    async fn nameserver_observation_at(
        &self,
        endpoint: CheetahString,
        operation: NameserverReadOperation,
        timeout_millis: u64,
        max_body_bytes: usize,
    ) -> Result<NameserverReadReply, InfrastructureObservationReadError>;
}

impl MQAdminNameserverRouteReadExt for DefaultMQAdminExt {
    async fn nameserver_observation_at(
        &self,
        endpoint: CheetahString,
        operation: NameserverReadOperation,
        timeout_millis: u64,
        max_body_bytes: usize,
    ) -> Result<NameserverReadReply, InfrastructureObservationReadError> {
        self.inner()
            .mq_client_api()
            .map_err(sanitized_error)?
            .read_nameserver_observation_at(endpoint, operation, timeout_millis, max_body_bytes)
            .await
            .map_err(sanitized_error)
    }
}
