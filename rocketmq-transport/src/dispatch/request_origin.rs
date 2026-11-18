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

use std::net::SocketAddr;

use rocketmq_security_api::PeerInfo;
use rocketmq_security_api::Principal;
use rocketmq_security_api::SecurityBootstrapProfile;

/// A trusted in-process caller that submitted an embedded request.
///
/// This value is supplied by the transport composition root. It is not derived
/// from command headers, extensions, or processor-provided data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EmbeddedCaller {
    /// The Broker-owned embedded Proxy adapter.
    BrokerProxy,
}

/// The trusted ingress origin of a request.
///
/// Network requests carry the peer observed by the transport. Embedded
/// requests identify their in-process caller and intentionally have no
/// network peer; they may carry the end-client address that the caller
/// observed at its own network ingress.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RequestOrigin {
    /// A request decoded from a network session.
    Network {
        /// Peer metadata captured by the transport ingress.
        peer: PeerInfo,
    },
    /// A request submitted by a trusted in-process adapter.
    Embedded {
        /// The trusted adapter that submitted the request.
        caller: EmbeddedCaller,
        /// The address of the end client on whose behalf `caller` submitted
        /// the request, as observed by `caller`'s own network ingress.
        ///
        /// Only the embedded dispatch entry point records it, from an argument
        /// of the trusted composition root; command headers, extensions, and
        /// processors cannot set it. It is not a transport peer: request
        /// authentication and authorization never read it. `None` means the
        /// caller supplied no client, for example for a request the adapter
        /// issues on its own behalf.
        client_addr: Option<SocketAddr>,
    },
}

/// Authentication facts established at transport ingress.
///
/// Callers outside this crate can inspect an authenticated principal, but
/// cannot construct an authenticated state from processor or command data. Use
/// [`Self::principal`] to distinguish an authenticated state when matching the
/// forward-compatible enum.
///
/// ```compile_fail
/// use rocketmq_security_api::Principal;
/// use rocketmq_transport::api::AuthenticationState;
///
/// let _ = AuthenticationState::Authenticated(Principal::new("forged"), ());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuthenticationState {
    /// A principal supplied by a trusted network or embedded ingress adapter.
    #[allow(
        private_interfaces,
        reason = "the private proof prevents untrusted construction while patterns can inspect the principal"
    )]
    Authenticated(Principal, AuthenticationProof),
    /// Security remains enabled, but ingress did not establish a principal.
    Anonymous,
    /// The explicitly configured development-insecure loopback profile.
    SecurityDisabled,
}

impl AuthenticationState {
    pub(crate) fn from_network_principal(principal: Option<Principal>, profile: SecurityBootstrapProfile) -> Self {
        match (principal, profile) {
            (Some(principal), _) => Self::Authenticated(principal, AuthenticationProof),
            (None, SecurityBootstrapProfile::DevelopmentInsecureLoopback) => Self::SecurityDisabled,
            (None, SecurityBootstrapProfile::SecureEnforced) => Self::Anonymous,
        }
    }

    pub(crate) fn authenticated(principal: Principal) -> Self {
        Self::Authenticated(principal, AuthenticationProof)
    }

    /// Returns the trusted principal when ingress authenticated the request.
    #[must_use]
    pub const fn principal(&self) -> Option<&Principal> {
        match self {
            Self::Authenticated(principal, ..) => Some(principal),
            Self::Anonymous | Self::SecurityDisabled => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AuthenticationProof;
