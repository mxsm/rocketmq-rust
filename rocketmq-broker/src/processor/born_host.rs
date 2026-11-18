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

//! Trusted born-host resolution for the processors that write client messages.

use std::net::SocketAddr;

use rocketmq_transport::api::RequestOrigin;

/// Resolves the born host to persist for a message that a request from
/// `origin` writes on behalf of a client (send, reply, and recall).
///
/// The address always comes from a trusted ingress fact, never from command
/// headers, extensions, or the body:
///
/// - a network request uses the peer address its transport session observed;
/// - an embedded request uses the end-client address that its trusted
///   in-process caller (the local-mode Proxy) observed at its own network
///   ingress and passed to the embedded dispatch boundary. Java local mode
///   writes the same address, through the channel it builds from the client's
///   Proxy context.
///
/// # Errors
///
/// An embedded request without a client address fails closed with an invalid
/// argument error: every Proxy network ingress records the client address, so
/// a missing one means the write was not issued for a network client, and the
/// Broker does not substitute a host the client never had. An origin this
/// Broker does not recognize is an invariant violation.
pub(crate) fn trusted_born_host(origin: &RequestOrigin) -> crate::broker_error::BrokerResult<SocketAddr> {
    match origin {
        RequestOrigin::Network { peer } => Ok(peer.address()),
        RequestOrigin::Embedded {
            client_addr: Some(client_addr),
            ..
        } => Ok(*client_addr),
        RequestOrigin::Embedded { client_addr: None, .. } => Err(crate::broker_error::invalid_argument(
            "an embedded message write requires the client address observed at Proxy ingress for the persisted born \
             host",
        )),
        _ => Err(crate::broker_error::invariant_violated(
            "a message write received an unrecognized request origin",
        )),
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_security_api::PeerInfo;
    use rocketmq_transport::api::EmbeddedCaller;

    use super::*;

    fn address(value: &str) -> SocketAddr {
        value.parse().expect("test address parses")
    }

    #[test]
    fn network_writes_use_the_transport_peer() {
        let peer = address("192.0.2.10:40001");
        let origin = RequestOrigin::Network {
            peer: PeerInfo::new(peer, false),
        };

        assert_eq!(trusted_born_host(&origin).expect("network peer is trusted"), peer);
    }

    #[test]
    fn embedded_writes_use_the_client_address_from_the_trusted_caller() {
        let client = address("198.51.100.20:50001");
        let origin = RequestOrigin::Embedded {
            caller: EmbeddedCaller::BrokerProxy,
            client_addr: Some(client),
        };

        assert_eq!(
            trusted_born_host(&origin).expect("embedded client address is trusted"),
            client
        );
    }

    #[test]
    fn embedded_writes_without_a_client_address_fail_closed() {
        let origin = RequestOrigin::Embedded {
            caller: EmbeddedCaller::BrokerProxy,
            client_addr: None,
        };

        let error = trusted_born_host(&origin).expect_err("an embedded write needs a client address");
        assert_eq!(error.descriptor(), &rocketmq_error::CORE_ARGUMENT_INVALID);
    }
}
