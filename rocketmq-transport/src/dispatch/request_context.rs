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
use rocketmq_security_api::SecurityBootstrapProfile;

use crate::contract::TransportContractViolation;
use crate::deadline::RequestDeadline;
use crate::dispatch::AuthenticationState;
use crate::dispatch::EmbeddedCaller;
use crate::dispatch::RequestOrigin;

/// Trusted entry adapter that materialized a dispatch request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestTransport {
    /// A command decoded from an accepted network session.
    Network,
    /// A command submitted by the Broker-owned embedded Proxy adapter.
    EmbeddedProxy,
}

/// Security and lifecycle metadata established by a trusted entry adapter.
///
/// Fields are private so command headers and bodies cannot self-assert an
/// embedded identity. Network authentication and embedded composition are the
/// only supported constructors.
#[derive(Debug, Clone)]
pub(crate) struct RequestContext {
    deadline: Option<RequestDeadline>,
    origin: RequestOrigin,
    authentication: AuthenticationState,
}

/// Trusted request facts transferred from ingress into the request builder.
pub(crate) struct RequestContextParts {
    pub(crate) deadline: Option<RequestDeadline>,
    pub(crate) origin: RequestOrigin,
    pub(crate) authentication: AuthenticationState,
}

impl RequestContext {
    /// Creates a context for a decoded network command using the security
    /// profile selected by the trusted transport bootstrap.
    #[must_use]
    pub(crate) fn network_with_security_profile(
        peer: PeerInfo,
        principal: Option<Principal>,
        deadline: Option<RequestDeadline>,
        profile: SecurityBootstrapProfile,
    ) -> Self {
        let authentication = AuthenticationState::from_network_principal(principal, profile);
        Self {
            deadline,
            origin: RequestOrigin::Network { peer },
            authentication,
        }
    }

    /// Creates a fail-closed context for an explicitly identified embedded caller.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the composition root did not inject a
    /// trusted principal.
    pub(crate) fn try_embedded_with_caller(
        caller: EmbeddedCaller,
        principal: Option<Principal>,
        deadline: Option<RequestDeadline>,
    ) -> Result<Self, TransportContractViolation> {
        let principal = principal.ok_or(TransportContractViolation::RequestContextMissingEmbeddedAuthentication)?;
        Ok(Self {
            origin: RequestOrigin::Embedded { caller },
            deadline,
            authentication: AuthenticationState::authenticated(principal),
        })
    }

    /// Returns the trusted entry type.
    #[must_use]
    pub const fn transport(&self) -> RequestTransport {
        match &self.origin {
            RequestOrigin::Network { .. } => RequestTransport::Network,
            RequestOrigin::Embedded { .. } => RequestTransport::EmbeddedProxy,
        }
    }

    /// Returns read-only peer metadata established by the network adapter.
    #[must_use]
    pub const fn peer(&self) -> Option<&PeerInfo> {
        match &self.origin {
            RequestOrigin::Network { peer } => Some(peer),
            RequestOrigin::Embedded { .. } => None,
        }
    }

    /// Returns the authenticated identity established by the adapter.
    #[must_use]
    pub const fn principal(&self) -> Option<&Principal> {
        self.authentication.principal()
    }

    /// Returns the immutable end-to-end deadline, when supplied.
    #[must_use]
    pub const fn deadline(&self) -> Option<RequestDeadline> {
        self.deadline
    }

    /// Returns the immutable origin captured by a trusted ingress adapter.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn origin(&self) -> &RequestOrigin {
        &self.origin
    }

    /// Returns the immutable authentication facts established at ingress.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn authentication(&self) -> &AuthenticationState {
        &self.authentication
    }

    /// Consumes the staging context after trusted ingress has finished
    /// materializing its request facts.
    pub(crate) fn into_parts(self) -> RequestContextParts {
        RequestContextParts {
            deadline: self.deadline,
            origin: self.origin,
            authentication: self.authentication,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_parts(parts: RequestContextParts) -> Self {
        Self {
            deadline: parts.deadline,
            origin: parts.origin,
            authentication: parts.authentication,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

    use super::*;

    fn peer(address: &str) -> PeerInfo {
        PeerInfo::new(
            address.parse::<SocketAddr>().expect("test peer address must parse"),
            true,
        )
    }

    #[test]
    fn network_context_retains_the_trusted_peer_and_principal() {
        let peer = peer("192.0.2.44:10911");
        let principal = Principal::new("network-user");

        let context = RequestContext::network_with_security_profile(
            peer.clone(),
            Some(principal.clone()),
            None,
            SecurityBootstrapProfile::SecureEnforced,
        );

        assert_eq!(context.peer(), Some(&peer));
        assert_eq!(context.principal(), Some(&principal));
        assert_eq!(context.origin(), &RequestOrigin::Network { peer: peer.clone() });
        assert_eq!(context.authentication(), &AuthenticationState::authenticated(principal));
    }

    #[test]
    fn network_authentication_matrix_prioritizes_the_trusted_principal() {
        for (profile, unauthenticated) in [
            (SecurityBootstrapProfile::SecureEnforced, AuthenticationState::Anonymous),
            (
                SecurityBootstrapProfile::DevelopmentInsecureLoopback,
                AuthenticationState::SecurityDisabled,
            ),
        ] {
            let without_principal =
                RequestContext::network_with_security_profile(peer("192.0.2.45:10911"), None, None, profile);
            assert_eq!(without_principal.authentication(), &unauthenticated);

            let principal = Principal::new("network-user");
            let with_principal = RequestContext::network_with_security_profile(
                peer("192.0.2.46:10911"),
                Some(principal.clone()),
                None,
                profile,
            );
            assert!(matches!(
                with_principal.authentication(),
                AuthenticationState::Authenticated(actual, ..) if actual == &principal
            ));
        }
    }

    #[test]
    fn embedded_context_has_an_explicit_caller_without_a_network_peer() {
        let principal = Principal::new("embedded-user");
        let context =
            RequestContext::try_embedded_with_caller(EmbeddedCaller::BrokerProxy, Some(principal.clone()), None)
                .expect("trusted embedded principal must construct context");

        assert_eq!(context.transport(), RequestTransport::EmbeddedProxy);
        assert_eq!(context.peer(), None);
        assert_eq!(
            context.origin(),
            &RequestOrigin::Embedded {
                caller: EmbeddedCaller::BrokerProxy
            }
        );
        assert_eq!(context.authentication(), &AuthenticationState::authenticated(principal));
    }

    #[test]
    fn embedded_context_without_a_principal_fails_closed() {
        assert_eq!(
            RequestContext::try_embedded_with_caller(EmbeddedCaller::BrokerProxy, None, None)
                .expect_err("embedded context must require a trusted principal"),
            TransportContractViolation::RequestContextMissingEmbeddedAuthentication
        );
    }

    #[test]
    fn command_extensions_cannot_claim_origin_or_authentication_facts() {
        let mut command = RemotingCommand::create_remoting_command(17);
        command.add_ext_field("principal", "forged-user");
        command.add_ext_field("origin", "embedded");
        let peer = peer("198.51.100.7:10911");
        let context = RequestContext::network_with_security_profile(
            peer.clone(),
            None,
            None,
            SecurityBootstrapProfile::SecureEnforced,
        );

        assert!(command.ext_fields().is_some());
        assert_eq!(context.origin(), &RequestOrigin::Network { peer: peer.clone() });
        assert_eq!(context.authentication(), &AuthenticationState::Anonymous);
        assert_eq!(context.principal(), None);
    }
}
