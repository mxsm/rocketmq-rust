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

use std::net::SocketAddr;

use rocketmq_common::common::mix_all;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::RPCHook;

use crate::base::client_config::ClientConfig;

/// RPC hook that injects namespace metadata into outgoing requests.
///
/// When the client configuration contains a `namespace_v2` value, this hook
/// automatically adds namespace-related extension fields to all RPC requests.
/// This enables multi-tenancy and namespace isolation in RocketMQ clusters.
///
/// Two extension fields are added to requests when a namespace is configured:
/// - `nsd`: Marker field set to `"true"` indicating namespace presence
/// - `ns`: The actual namespace identifier
///
/// # Examples
///
/// ```no_run
/// use cheetah_string::CheetahString;
/// use rocketmq_client::base::client_config::ClientConfig;
/// use rocketmq_client::hook::namespace_rpc_hook::NamespaceRpcHook;
///
/// let mut config = ClientConfig::default();
/// config.set_namespace_v2(CheetahString::from_static_str("my-namespace"));
///
/// let hook = NamespaceRpcHook::new(config);
/// ```
pub struct NamespaceRpcHook {
    client_config: ClientConfig,
}

impl NamespaceRpcHook {
    /// Creates a new namespace RPC hook.
    ///
    /// The hook uses the provided client configuration to determine whether
    /// namespace fields should be injected into requests.
    pub fn new(client_config: ClientConfig) -> Self {
        Self { client_config }
    }

    /// Returns a reference to the underlying client configuration.
    pub fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }
}

impl RPCHook for NamespaceRpcHook {
    /// Injects namespace extension fields into the request before transmission.
    ///
    /// If the client configuration contains a `namespace_v2` value, this method
    /// initializes the request's extension fields map (if not already present)
    /// and adds the namespace marker and identifier fields.
    fn do_before_request(&self, _remote_addr: SocketAddr, request: &mut RemotingCommand) -> RocketMQResult<()> {
        if let Some(namespace_v2) = self.client_config.get_namespace_v2() {
            request.ensure_ext_fields_initialized();

            request.add_ext_field(mix_all::RPC_REQUEST_HEADER_NAMESPACED_FIELD, "true");
            request.add_ext_field(mix_all::RPC_REQUEST_HEADER_NAMESPACE_FIELD, namespace_v2.as_str());
        }
        Ok(())
    }

    /// Processes the response after reception.
    ///
    /// This implementation performs no modifications and unconditionally returns `Ok(())`.
    fn do_after_response(
        &self,
        _remote_addr: SocketAddr,
        _request: &RemotingCommand,
        _response: &mut RemotingCommand,
    ) -> RocketMQResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use cheetah_string::CheetahString;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::runtime::RPCHook;

    use super::*;

    #[test]
    fn test_namespace_rpc_hook_adds_headers_when_namespace_configured() {
        let mut config = ClientConfig::default();
        config.set_namespace_v2(CheetahString::from_static_str("test-namespace"));

        let hook = NamespaceRpcHook::new(config);
        let mut request = RemotingCommand::create_remoting_command(1);
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        hook.do_before_request(addr, &mut request).unwrap();

        assert_eq!(
            request
                .ext_fields()
                .unwrap()
                .get(mix_all::RPC_REQUEST_HEADER_NAMESPACED_FIELD),
            Some(&CheetahString::from_static_str("true"))
        );
        assert_eq!(
            request
                .ext_fields()
                .unwrap()
                .get(mix_all::RPC_REQUEST_HEADER_NAMESPACE_FIELD),
            Some(&CheetahString::from_static_str("test-namespace"))
        );
    }

    #[test]
    fn test_namespace_rpc_hook_no_headers_when_namespace_not_configured() {
        let config = ClientConfig::default();
        let hook = NamespaceRpcHook::new(config);
        let mut request = RemotingCommand::create_remoting_command(1);
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        hook.do_before_request(addr, &mut request).unwrap();

        assert_eq!(request.ext_fields(), None);
    }

    #[test]
    fn test_do_after_response_does_nothing() {
        let config = ClientConfig::default();
        let hook = NamespaceRpcHook::new(config);
        let request = RemotingCommand::create_remoting_command(1);
        let mut response = RemotingCommand::create_response_command();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        let result = hook.do_after_response(addr, &request, &mut response);
        assert!(result.is_ok());
    }

    #[test]
    fn test_client_config_accessor() {
        let mut config = ClientConfig::default();
        config.set_namespace_v2(CheetahString::from_static_str("test-namespace"));

        let hook = NamespaceRpcHook::new(config.clone());

        assert_eq!(
            hook.client_config().get_namespace_v2(),
            Some(&CheetahString::from_static_str("test-namespace"))
        );
    }

    #[test]
    fn test_ensure_ext_fields_preserves_existing_data() {
        let mut config = ClientConfig::default();
        config.set_namespace_v2(CheetahString::from_static_str("test-ns"));

        let hook = NamespaceRpcHook::new(config);
        let mut request = RemotingCommand::create_remoting_command(100);

        // Store original opaque (request ID)
        let original_opaque = request.opaque();

        // Add an existing ext field
        request.ensure_ext_fields_initialized();
        request.add_ext_field("existing-key", "existing-value");

        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Apply namespace hook
        hook.do_before_request(addr, &mut request).unwrap();

        // Verify original opaque is preserved
        assert_eq!(request.opaque(), original_opaque);

        // Verify existing field is preserved
        assert_eq!(
            request
                .ext_fields()
                .unwrap()
                .get(&CheetahString::from_static_str("existing-key")),
            Some(&CheetahString::from_static_str("existing-value"))
        );

        // Verify namespace fields are added
        assert_eq!(
            request
                .ext_fields()
                .unwrap()
                .get(mix_all::RPC_REQUEST_HEADER_NAMESPACED_FIELD),
            Some(&CheetahString::from_static_str("true"))
        );
        assert_eq!(
            request
                .ext_fields()
                .unwrap()
                .get(mix_all::RPC_REQUEST_HEADER_NAMESPACE_FIELD),
            Some(&CheetahString::from_static_str("test-ns"))
        );
    }
}
