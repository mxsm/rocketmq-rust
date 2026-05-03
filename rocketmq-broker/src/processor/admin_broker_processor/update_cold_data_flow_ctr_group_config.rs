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

use crate::broker_runtime::BrokerRuntimeInner;
use rocketmq_common::common::mix_all;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;

#[derive(Clone)]
pub struct UpdateColdDataFlowCtrGroupConfigRequestHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> UpdateColdDataFlowCtrGroupConfigRequestHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }

    pub async fn update_cold_data_flow_ctr_group_config(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command();

        let Some(body) = request.get_body() else {
            return Ok(Some(response.set_code(ResponseCode::Success)));
        };

        let body = mix_all::string_to_properties(&String::from_utf8_lossy(body));
        match body {
            Some(body) => {
                let Some(service) = self.broker_runtime_inner.cold_data_cg_ctr_service() else {
                    return Ok(Some(
                        response
                            .set_code(ResponseCode::SystemError)
                            .set_remark("ColdDataCgCtrService is not configured"),
                    ));
                };

                for (consumer_group, threshold) in body {
                    if let Ok(threshold) = threshold.as_str().parse::<i64>() {
                        service.add_or_update_group_config(consumer_group.as_str(), threshold);
                    }
                }

                Ok(Some(response.set_code(ResponseCode::Success)))
            }
            None => Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("string2Properties error"),
            )),
        }
    }

    pub async fn remove_cold_data_flow_ctr_group_config(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command();
        let Some(body) = request.get_body() else {
            return Ok(Some(response.set_code(ResponseCode::Success)));
        };
        let Some(service) = self.broker_runtime_inner.cold_data_cg_ctr_service() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("ColdDataCgCtrService is not configured"),
            ));
        };

        let consumer_group = String::from_utf8_lossy(body);
        if !consumer_group.is_empty() {
            service.remove_group_config(&consumer_group);
        }
        Ok(Some(response.set_code(ResponseCode::Success)))
    }

    pub async fn get_cold_data_flow_ctr_info(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let Some(service) = self.broker_runtime_inner.cold_data_cg_ctr_service() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("ColdDataCgCtrService is not configured"),
            ));
        };

        response.set_body_mut_ref(service.get_cold_data_flow_ctr_info());
        Ok(Some(response.set_code(ResponseCode::Success)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_remoting::base::response_future::ResponseFuture;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::connection::Connection;
    use rocketmq_remoting::net::channel::ChannelInner;
    use rocketmq_remoting::protocol::header::empty_header::EmptyHeader;
    use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
    use rocketmq_rust::ArcMut;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::*;
    use crate::broker_runtime::BrokerRuntime;

    fn temp_test_root(label: &str) -> std::path::PathBuf {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time should move forward")
            .as_millis();
        std::env::temp_dir().join(format!("rocketmq-rust-admin-cold-data-{label}-{millis}"))
    }

    async fn new_test_runtime(label: &str, cold_data_flow_control_enable: bool) -> BrokerRuntime {
        let temp_root = temp_test_root(label);
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            cold_data_flow_control_enable,
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
        let response_table = ArcMut::new(HashMap::<i32, ResponseFuture>::new());
        let inner = ArcMut::new(ChannelInner::new(connection, response_table));
        Channel::new(inner, local_addr, local_addr)
    }

    #[tokio::test]
    async fn update_and_get_cold_data_flow_ctr_info_round_trips_config() {
        let mut runtime = new_test_runtime("update-get", true).await;
        let inner = runtime.inner_for_test().clone();
        let mut handler = UpdateColdDataFlowCtrGroupConfigRequestHandler::new(inner.clone());
        let mut request =
            RemotingCommand::create_request_command(RequestCode::UpdateColdDataFlowCtrConfig, EmptyHeader {})
                .set_body("group-a=128\ngroup-b=256");

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let response = handler
            .update_cold_data_flow_ctr_group_config(
                channel.clone(),
                ctx.clone(),
                RequestCode::UpdateColdDataFlowCtrConfig,
                &mut request,
            )
            .await
            .expect("update cold data flow ctr should succeed")
            .expect("update cold data flow ctr should return response");
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(response.is_response_type());
        assert!(!inner
            .cold_data_cg_ctr_service()
            .expect("cold data service")
            .is_cg_need_cold_data_flow_ctr("group-a"));
        inner
            .cold_data_cg_ctr_service()
            .expect("cold data service")
            .cold_acc("group-a", 128);
        assert!(inner
            .cold_data_cg_ctr_service()
            .expect("cold data service")
            .is_cg_need_cold_data_flow_ctr("group-a"));

        let mut info_request =
            RemotingCommand::create_request_command(RequestCode::GetColdDataFlowCtrInfo, EmptyHeader {});
        let mut info_response = handler
            .get_cold_data_flow_ctr_info(channel, ctx, RequestCode::GetColdDataFlowCtrInfo, &mut info_request)
            .await
            .expect("get cold data flow ctr info should succeed")
            .expect("get cold data flow ctr info should return response");
        assert_eq!(ResponseCode::from(info_response.code()), ResponseCode::Success);
        assert!(info_response.is_response_type());

        let value: serde_json::Value = serde_json::from_slice(
            info_response
                .take_body()
                .expect("cold data flow ctr info body should exist")
                .as_ref(),
        )
        .expect("decode cold data flow ctr info body");
        assert_eq!(value["configTable"]["group-a"], 128);
        assert_eq!(value["configTable"]["group-b"], 256);
        assert_eq!(value["runtimeTable"]["group-a"]["coldAcc"], 128);
        assert!(
            value["runtimeTable"]["group-a"]["createTimeMills"]
                .as_i64()
                .unwrap_or(0)
                > 0
        );
        assert!(
            value["runtimeTable"]["group-a"]["lastColdReadTimeMills"]
                .as_i64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(value["coldDataFlowControlEnable"], true);
        assert_eq!(value["cgColdReadThreshold"], 3 * 1024 * 1024);
        assert_eq!(value["globalColdReadThreshold"], 100 * 1024 * 1024);
        assert_eq!(value["globalAcc"], 128);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn update_and_remove_cold_data_flow_ctr_empty_body_match_java_noop_success() {
        let mut runtime = new_test_runtime("empty-body", true).await;
        let inner = runtime.inner_for_test().clone();
        let mut handler = UpdateColdDataFlowCtrGroupConfigRequestHandler::new(inner);

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));

        let mut update_request =
            RemotingCommand::create_request_command(RequestCode::UpdateColdDataFlowCtrConfig, EmptyHeader {});
        let update_response = handler
            .update_cold_data_flow_ctr_group_config(
                channel.clone(),
                ctx.clone(),
                RequestCode::UpdateColdDataFlowCtrConfig,
                &mut update_request,
            )
            .await
            .expect("empty update should succeed")
            .expect("empty update should return response");
        assert_eq!(ResponseCode::from(update_response.code()), ResponseCode::Success);
        assert!(update_response.is_response_type());

        let mut remove_request =
            RemotingCommand::create_request_command(RequestCode::RemoveColdDataFlowCtrConfig, EmptyHeader {});
        let remove_response = handler
            .remove_cold_data_flow_ctr_group_config(
                channel,
                ctx,
                RequestCode::RemoveColdDataFlowCtrConfig,
                &mut remove_request,
            )
            .await
            .expect("empty remove should succeed")
            .expect("empty remove should return response");
        assert_eq!(ResponseCode::from(remove_response.code()), ResponseCode::Success);
        assert!(remove_response.is_response_type());

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn remove_cold_data_flow_ctr_config_removes_group_threshold() {
        let mut runtime = new_test_runtime("remove", true).await;
        let inner = runtime.inner_for_test().clone();
        inner
            .cold_data_cg_ctr_service()
            .expect("cold data service")
            .add_or_update_group_config("group-a", 128);
        let mut handler = UpdateColdDataFlowCtrGroupConfigRequestHandler::new(inner.clone());
        let mut request =
            RemotingCommand::create_request_command(RequestCode::RemoveColdDataFlowCtrConfig, EmptyHeader {})
                .set_body("group-a");

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let response = handler
            .remove_cold_data_flow_ctr_group_config(
                channel,
                ctx,
                RequestCode::RemoveColdDataFlowCtrConfig,
                &mut request,
            )
            .await
            .expect("remove cold data flow ctr should succeed")
            .expect("remove cold data flow ctr should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(response.is_response_type());
        assert!(!inner
            .cold_data_cg_ctr_service()
            .expect("cold data service")
            .is_cg_need_cold_data_flow_ctr("group-a"));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
