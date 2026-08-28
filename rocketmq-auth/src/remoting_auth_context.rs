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

/// Typed, read-only transport facts used by remoting authentication and authorization.
///
/// This value carries no connection, writer, cancellation, or lifecycle authority.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RemotingAuthContext {
    source_ip: Option<String>,
    channel_id: Option<String>,
}

impl RemotingAuthContext {
    /// Creates an authentication context from trusted ingress facts.
    #[must_use]
    pub fn new(source_ip: Option<String>, channel_id: Option<String>) -> Self {
        Self { source_ip, channel_id }
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
}

#[cfg(test)]
mod tests {
    use super::RemotingAuthContext;

    #[test]
    fn exposes_only_owned_authentication_facts() {
        let context = RemotingAuthContext::new(Some("192.0.2.10".to_owned()), Some("transport-session-17".to_owned()));

        assert_eq!(context.source_ip(), Some("192.0.2.10"));
        assert_eq!(context.channel_id(), Some("transport-session-17"));
    }

    #[test]
    fn embedded_context_can_omit_network_facts() {
        let context = RemotingAuthContext::new(None, None);

        assert_eq!(context.source_ip(), None);
        assert_eq!(context.channel_id(), None);
    }
}
