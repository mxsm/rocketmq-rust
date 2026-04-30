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

use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub struct GetBrokerHaStatusHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> GetBrokerHaStatusHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }

    pub async fn get_broker_ha_status(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let broker_runtime_inner = self.broker_runtime_inner.as_mut();
        let response = RemotingCommand::create_response_command();

        let message_store = match broker_runtime_inner.message_store() {
            Some(store) => store,
            None => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("message store is not available"),
                ));
            }
        };

        let ha_runtime_info = match message_store.get_ha_runtime_info() {
            Some(runtime_info) => runtime_info,
            None => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("HA service is not available"),
                ));
            }
        };

        match serde_json::to_vec(&ha_runtime_info) {
            Ok(body) => Ok(Some(response.set_body(body).set_code(ResponseCode::Success))),
            Err(e) => Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(format!("Failed to serialize HARuntimeInfo: {}", e)),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_remoting::base::response_future::ResponseFuture;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::connection::Connection;
    use rocketmq_remoting::net::channel::ChannelInner;
    use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use crate::broker_runtime::BrokerRuntime;

    use super::*;

    fn temp_test_root(label: &str) -> std::path::PathBuf {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time should move forward")
            .as_millis();
        std::env::temp_dir().join(format!("rocketmq-rust-ha-status-{label}-{millis}"))
    }

    async fn create_test_channel() -> Channel {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let local_addr = listener.local_addr().expect("local listener addr");
        let std_stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
        std_stream.set_nonblocking(true).expect("set nonblocking");
        drop(listener);
        let tcp_stream = tokio::net::TcpStream::from_std(std_stream).expect("convert tcp stream");
        let connection = Connection::new(tcp_stream);
        let response_table = ArcMut::new(HashMap::<i32, ResponseFuture>::new());
        let inner = ArcMut::new(ChannelInner::new(connection, response_table));
        Channel::new(inner, local_addr, local_addr)
    }

    #[tokio::test]
    async fn get_broker_ha_status_serializes_store_runtime_info() {
        let temp_root = temp_test_root("success");
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

        let inner = runtime.inner_for_test().clone();
        let mut handler = GetBrokerHaStatusHandler::new(inner);
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerHaStatus);

        let response = handler
            .get_broker_ha_status(channel, ctx, RequestCode::GetBrokerHaStatus, &mut request)
            .await
            .expect("HA status should return broker response")
            .expect("HA status should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = response.body().expect("HA status body");
        let runtime_info: HARuntimeInfo = serde_json::from_slice(body.as_ref()).expect("decode HA runtime info");
        assert!(runtime_info.master);

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn get_broker_ha_status_returns_error_when_store_is_missing() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        let inner = runtime.inner_for_test().clone();
        let mut handler = GetBrokerHaStatusHandler::new(inner);
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerHaStatus);

        let response = handler
            .get_broker_ha_status(channel, ctx, RequestCode::GetBrokerHaStatus, &mut request)
            .await
            .expect("HA status should return broker response")
            .expect("HA status should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.contains("message store is not available")));
    }
}
