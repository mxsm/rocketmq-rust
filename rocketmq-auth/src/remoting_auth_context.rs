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

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_transport::api::v2::EmbeddedCaller;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestOrigin;
use rocketmq_transport::api::v2::SessionView;

/// Typed, read-only transport facts used by remoting authentication and authorization.
///
/// This value carries no connection, writer, cancellation, or lifecycle authority.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RemotingAuthContext {
    source_ip: Option<String>,
    channel_id: Option<String>,
    embedded: bool,
}

impl RemotingAuthContext {
    /// Creates a compatibility authentication context from ingress facts.
    ///
    /// New network adapters should use [`Self::network`]. Embedded trust can
    /// only be projected by [`Self::from_request`] from a transport-owned
    /// request; missing network metadata never implies embedded trust.
    #[must_use]
    pub fn new(source_ip: Option<String>, channel_id: Option<String>) -> Self {
        Self {
            source_ip,
            channel_id,
            embedded: false,
        }
    }

    /// Creates a context for a network request from trusted ingress metadata.
    #[must_use]
    pub fn network(source_ip: impl Into<String>, channel_id: impl Into<String>) -> Self {
        Self {
            source_ip: Some(source_ip.into()),
            channel_id: Some(channel_id.into()),
            embedded: false,
        }
    }

    /// Creates a context for a trusted in-process request.
    #[must_use]
    pub(crate) fn embedded(channel_id: impl Into<String>) -> Self {
        Self {
            source_ip: None,
            channel_id: Some(channel_id.into()),
            embedded: true,
        }
    }

    /// Projects authentication facts from a request assembled by the trusted
    /// V2 transport boundary.
    ///
    /// # Errors
    ///
    /// Returns an authentication error if origin and session kinds disagree or
    /// required network/session metadata is missing.
    pub fn from_request(request: &RemotingRequest) -> RocketMQResult<Self> {
        let channel_id = format!(
            "transport-session-{}",
            request.original_identity().request_id().owner_id()
        );
        let context = match (request.origin(), request.session()) {
            (RequestOrigin::Network { peer }, SessionView::Network { remote_addr, .. }) => {
                validate_network_origin(peer.address(), *remote_addr)?;
                Self::network(remote_addr.ip().to_string(), channel_id)
            }
            (RequestOrigin::Embedded { caller }, SessionView::Embedded { .. }) => {
                validate_embedded_caller(*caller)?;
                Self::embedded(channel_id)
            }
            _ => {
                return Err(RocketMQError::authentication_failed(
                    "remoting request origin does not match its session",
                ));
            }
        };
        context.validate()?;
        Ok(context)
    }

    /// Returns the trusted source IP, when the ingress has a network peer.
    #[must_use]
    pub fn source_ip(&self) -> Option<&str> {
        self.source_ip.as_deref()
    }

    /// Returns the stable transport session identity used for request signing.
    #[must_use]
    pub fn channel_id(&self) -> Option<&str> {
        self.channel_id.as_deref()
    }

    /// Returns whether the request came from a trusted in-process adapter.
    #[must_use]
    pub const fn is_embedded(&self) -> bool {
        self.embedded
    }

    /// Verifies that the context carries a complete, internally consistent
    /// trusted-ingress projection.
    ///
    /// # Errors
    ///
    /// Returns an authentication error for missing session identity, missing
    /// network source, or mixed embedded/network facts.
    pub fn validate(&self) -> RocketMQResult<()> {
        let channel_id = self.channel_id().filter(|value| !value.trim().is_empty());
        if channel_id.is_none() {
            return Err(RocketMQError::authentication_failed(
                "remoting authentication context is missing a session identity",
            ));
        }
        if self.embedded {
            if self.source_ip.is_some() {
                return Err(RocketMQError::authentication_failed(
                    "embedded remoting authentication context cannot carry a network source",
                ));
            }
            return Ok(());
        }
        if self.source_ip().filter(|value| !value.trim().is_empty()).is_none() {
            return Err(RocketMQError::authentication_failed(
                "network remoting authentication context is missing a source address",
            ));
        }
        Ok(())
    }
}

fn validate_network_origin(peer: SocketAddr, remote_addr: SocketAddr) -> RocketMQResult<()> {
    if peer != remote_addr {
        return Err(RocketMQError::authentication_failed(
            "remoting request peer does not match its session source",
        ));
    }
    Ok(())
}

fn validate_embedded_caller(caller: EmbeddedCaller) -> RocketMQResult<()> {
    match caller {
        EmbeddedCaller::BrokerProxy => Ok(()),
        _ => Err(RocketMQError::authentication_failed(
            "embedded remoting caller is not trusted by Broker authentication",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use rocketmq_transport::api::v2::EmbeddedCaller;

    use super::validate_embedded_caller;
    use super::validate_network_origin;
    use super::RemotingAuthContext;

    #[test]
    fn exposes_only_owned_authentication_facts() {
        let context = RemotingAuthContext::network("192.0.2.10", "transport-session-17");

        assert_eq!(context.source_ip(), Some("192.0.2.10"));
        assert_eq!(context.channel_id(), Some("transport-session-17"));
    }

    #[test]
    fn embedded_context_can_omit_network_facts() {
        let context = RemotingAuthContext::embedded("embedded-session-1");

        assert_eq!(context.source_ip(), None);
        assert_eq!(context.channel_id(), Some("embedded-session-1"));
        assert!(context.is_embedded());
        assert!(context.validate().is_ok());
    }

    #[test]
    fn missing_network_facts_fail_closed() {
        assert!(RemotingAuthContext::default().validate().is_err());
        assert!(RemotingAuthContext::new(Some("192.0.2.10".to_owned()), None)
            .validate()
            .is_err());
    }

    #[test]
    fn mismatched_network_peer_and_session_source_fail_closed() {
        let peer: SocketAddr = "192.0.2.10:10911".parse().unwrap();
        let session_source: SocketAddr = "192.0.2.11:10911".parse().unwrap();

        assert!(validate_network_origin(peer, session_source).is_err());
        assert!(validate_network_origin(peer, peer).is_ok());
    }

    #[test]
    fn broker_proxy_is_an_explicitly_trusted_embedded_caller() {
        assert!(validate_embedded_caller(EmbeddedCaller::BrokerProxy).is_ok());
    }
}
