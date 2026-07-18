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

use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::update_global_white_addrs_config_request_header::UpdateGlobalWhiteAddrsConfigRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_store::base::message_store::MessageStore;

use crate::auth::auth_admin_service::AuthAdminService;
use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub struct UpdateGlobalWhiteAddrsConfigRequestHandler<MS: MessageStore> {
    _broker_runtime_inner: rocketmq_rust::ArcMut<BrokerRuntimeInner<MS>>,
    auth_admin_service: Arc<AuthAdminService>,
}

impl<MS: MessageStore> UpdateGlobalWhiteAddrsConfigRequestHandler<MS> {
    pub fn new(
        broker_runtime_inner: rocketmq_rust::ArcMut<BrokerRuntimeInner<MS>>,
        auth_admin_service: Arc<AuthAdminService>,
    ) -> Self {
        Self {
            _broker_runtime_inner: broker_runtime_inner,
            auth_admin_service,
        }
    }

    pub async fn update_global_white_addrs_config(
        &mut self,
        _channel: Channel,
        _ctx: rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<UpdateGlobalWhiteAddrsConfigRequestHeader>()?;
        let response = RemotingCommand::create_response_command();
        let global_white_addrs = parse_global_white_addrs(request_header.global_white_addrs.as_str());

        if global_white_addrs.is_empty() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("The globalWhiteAddrs is blank"),
            ));
        }

        if self.is_not_super_user_login(request).await? {
            return Ok(Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_remark("Global white addresses can only be updated by super user"),
            ));
        }

        match self
            .auth_admin_service
            .update_global_white_remote_addresses(global_white_addrs)
            .await
        {
            Ok(_) => Ok(Some(response.set_code(ResponseCode::Success))),
            Err(error) => Ok(Some(map_error_response(response, error))),
        }
    }

    async fn is_not_super_user_login(&self, request: &RemotingCommand) -> rocketmq_error::RocketMQResult<bool> {
        let Some(access_key) = request.ext_fields().and_then(|fields| fields.get("AccessKey")) else {
            return Ok(false);
        };

        Ok(!self.auth_admin_service.is_super_user(access_key.as_str()).await?)
    }
}

fn parse_global_white_addrs(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|address| !address.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn map_error_response(response: RemotingCommand, error: RocketMQError) -> RemotingCommand {
    super::map_auth_admin_error_response(response, error)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    use cheetah_string::CheetahString;
    use rocketmq_auth::config::AuthConfig;
    use rocketmq_auth::ProviderRegistry;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_remoting::base::response_future::ResponseFuture;
    use rocketmq_remoting::connection::Connection;
    use rocketmq_remoting::net::channel::ChannelInner;
    use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::*;
    use crate::broker_runtime::BrokerRuntime;

    fn temp_test_root(label: &str) -> std::path::PathBuf {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time should move forward")
            .as_millis();
        std::env::temp_dir().join(format!("rocketmq-rust-admin-global-white-{label}-{millis}"))
    }

    async fn new_test_runtime(label: &str) -> BrokerRuntime {
        let temp_root = temp_test_root(label);
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        assert!(runtime.initialize().await);
        runtime
    }

    async fn create_test_channel() -> Channel {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let local_addr = listener.local_addr().expect("local listener addr");
        let std_stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
        std_stream.set_nonblocking(true).expect("set nonblocking");
        drop(listener);
        let tcp_stream = tokio::net::TcpStream::from_std(std_stream).expect("convert tcp stream");
        let connection = Connection::new(tcp_stream);
        let response_table = std::sync::Arc::new(parking_lot::Mutex::new(HashMap::<i32, ResponseFuture>::new()));
        let inner = std::sync::Arc::new(ChannelInner::new(connection, response_table));
        Channel::new(inner, local_addr, local_addr)
    }

    #[test]
    fn parse_global_white_addrs_splits_comma_values() {
        assert_eq!(
            parse_global_white_addrs("10.10.*.*, 192.168.0.* ,,172.16.1.1"),
            vec!["10.10.*.*", "192.168.0.*", "172.16.1.1"]
        );
    }

    #[tokio::test]
    async fn update_global_white_addrs_config_updates_runtime_snapshot() {
        let mut runtime = new_test_runtime("update").await;
        let inner = runtime.inner_for_test().clone();
        let provider_registry = ProviderRegistry::local(&AuthConfig {
            auth_config_path: inner.broker_config().auth_config_path.clone(),
            ..AuthConfig::default()
        })
        .expect("create provider registry");
        let auth_admin_service = Arc::new(AuthAdminService::with_provider_registry(provider_registry.clone()));
        let mut handler = UpdateGlobalWhiteAddrsConfigRequestHandler::new(inner.clone(), auth_admin_service);
        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));

        let mut request = RemotingCommand::create_request_command(
            RequestCode::UpdateGlobalWhiteAddrsConfig,
            UpdateGlobalWhiteAddrsConfigRequestHeader {
                global_white_addrs: CheetahString::from_static_str("10.10.*.*,192.168.0.*"),
            },
        );
        request.make_custom_header_to_net();

        let response = handler
            .update_global_white_addrs_config(channel, ctx, RequestCode::UpdateGlobalWhiteAddrsConfig, &mut request)
            .await
            .expect("request should be handled")
            .expect("response should exist");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(provider_registry
            .is_acl_white_remote_address(None, Some("10.10.1.2"))
            .unwrap());
        assert!(provider_registry
            .is_acl_white_remote_address(None, Some("192.168.0.7"))
            .unwrap());

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
