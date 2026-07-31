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

use rocketmq_security_api::PeerInfo;
use rocketmq_security_api::Principal;

use crate::deadline::RequestDeadline;

/// Trusted entry adapter that materialized a dispatch request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestTransport {
    /// A command decoded from an accepted network session.
    Network,
    /// A command submitted by the Broker-owned embedded Proxy adapter.
    EmbeddedProxy,
}

/// Error returned before an entry adapter can create a trusted request context.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RequestContextError {
    /// Embedded dispatch is fail-closed when no trusted identity was injected.
    #[error("embedded proxy dispatch requires an authenticated principal")]
    MissingEmbeddedPrincipal,
}

/// Security and lifecycle metadata established by a trusted entry adapter.
///
/// Fields are private so command headers and bodies cannot self-assert an
/// embedded identity. Network authentication and embedded composition are the
/// only supported constructors.
#[derive(Debug, Clone)]
pub struct RequestContext {
    transport: RequestTransport,
    peer: Option<PeerInfo>,
    principal: Option<Principal>,
    deadline: Option<RequestDeadline>,
}

impl RequestContext {
    /// Creates a context for a decoded network command.
    #[must_use]
    pub fn network(peer: PeerInfo, principal: Option<Principal>, deadline: Option<RequestDeadline>) -> Self {
        Self {
            transport: RequestTransport::Network,
            peer: Some(peer),
            principal,
            deadline,
        }
    }

    /// Creates a fail-closed context for a Broker-owned embedded adapter.
    ///
    /// # Errors
    ///
    /// Returns [`RequestContextError::MissingEmbeddedPrincipal`] when the
    /// composition root did not inject a trusted principal.
    pub fn try_embedded(
        principal: Option<Principal>,
        deadline: Option<RequestDeadline>,
    ) -> Result<Self, RequestContextError> {
        let principal = principal.ok_or(RequestContextError::MissingEmbeddedPrincipal)?;
        Ok(Self {
            transport: RequestTransport::EmbeddedProxy,
            peer: None,
            principal: Some(principal),
            deadline,
        })
    }

    /// Returns the trusted entry type.
    #[must_use]
    pub const fn transport(&self) -> RequestTransport {
        self.transport
    }

    /// Returns read-only peer metadata established by the network adapter.
    #[must_use]
    pub const fn peer(&self) -> Option<&PeerInfo> {
        self.peer.as_ref()
    }

    /// Returns the authenticated identity established by the adapter.
    #[must_use]
    pub const fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }

    /// Returns the immutable end-to-end deadline, when supplied.
    #[must_use]
    pub const fn deadline(&self) -> Option<RequestDeadline> {
        self.deadline
    }
}
