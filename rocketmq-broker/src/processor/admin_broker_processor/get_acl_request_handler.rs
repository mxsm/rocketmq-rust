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
use rocketmq_remoting::protocol::header::get_acl_request_header::GetAclRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_store::base::message_store::MessageStore;

use crate::auth::auth_admin_service::AuthAdminService;
use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub struct GetAclRequestHandler<MS: MessageStore> {
    _broker_runtime_inner: rocketmq_rust::ArcMut<BrokerRuntimeInner<MS>>,
    auth_admin_service: Arc<AuthAdminService>,
}

impl<MS: MessageStore> GetAclRequestHandler<MS> {
    pub fn new(
        broker_runtime_inner: rocketmq_rust::ArcMut<BrokerRuntimeInner<MS>>,
        auth_admin_service: Arc<AuthAdminService>,
    ) -> Self {
        Self {
            _broker_runtime_inner: broker_runtime_inner,
            auth_admin_service,
        }
    }

    pub async fn get_acl(
        &mut self,
        _channel: Channel,
        _ctx: rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetAclRequestHeader>()?;
        let mut response = RemotingCommand::create_response_command();

        if request_header.subject.is_empty() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("The subject is blank"),
            ));
        }

        match self.auth_admin_service.get_acl(request_header.subject.as_str()).await {
            Ok(Some(acl_info)) => {
                response.set_body_mut_ref(acl_info.encode()?);
                Ok(Some(response.set_code(ResponseCode::Success)))
            }
            Ok(None) => Ok(Some(response.set_code(ResponseCode::Success))),
            Err(error) => Ok(Some(map_error_response(response, error))),
        }
    }
}

fn map_error_response(response: RemotingCommand, error: RocketMQError) -> RemotingCommand {
    match error {
        RocketMQError::IllegalArgument(message) => {
            response.set_code(ResponseCode::InvalidParameter).set_remark(message)
        }
        RocketMQError::BrokerPermissionDenied { operation } => {
            response.set_code(ResponseCode::NoPermission).set_remark(operation)
        }
        other => response
            .set_code(ResponseCode::SystemError)
            .set_remark(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    use cheetah_string::CheetahString;
    use rocketmq_auth::authentication::enums::user_status::UserStatus;
    use rocketmq_auth::authentication::enums::user_type::UserType;
    use rocketmq_auth::authentication::model::user::User;
    use rocketmq_auth::config::AuthConfig;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_remoting::base::response_future::ResponseFuture;
    use rocketmq_remoting::connection::Connection;
    use rocketmq_remoting::net::channel::Channel;
    use rocketmq_remoting::net::channel::ChannelInner;
    use rocketmq_remoting::protocol::body::acl_info::AclInfo;
    use rocketmq_remoting::protocol::body::acl_info::PolicyEntryInfo;
    use rocketmq_remoting::protocol::body::acl_info::PolicyInfo;
    use rocketmq_remoting::protocol::header::create_acl_request_header::CreateAclRequestHeader;
    use rocketmq_remoting::protocol::header::update_acl_request_header::UpdateAclRequestHeader;
    use rocketmq_remoting::protocol::RemotingDeserializable;
    use rocketmq_remoting::protocol::RemotingSerializable;
    use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
    use rocketmq_rust::ArcMut;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::*;
    use crate::auth::auth_admin_service::AuthAdminService;
    use crate::broker_runtime::BrokerRuntime;
    use crate::processor::admin_broker_processor::create_acl_request_handler::CreateAclRequestHandler;
    use crate::processor::admin_broker_processor::update_acl_request_handler::UpdateAclRequestHandler;

    fn temp_test_root(label: &str) -> std::path::PathBuf {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time should move forward")
            .as_millis();
        std::env::temp_dir().join(format!("rocketmq-rust-admin-acl-{label}-{millis}"))
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
        let response_table = ArcMut::new(HashMap::<i32, ResponseFuture>::new());
        let inner = ArcMut::new(ChannelInner::new(connection, response_table));
        Channel::new(inner, local_addr, local_addr)
    }

    fn build_acl_info(resource: &str, actions: &str) -> AclInfo {
        AclInfo {
            subject: None,
            policies: Some(vec![PolicyInfo {
                policy_type: Some(CheetahString::from_static_str("Custom")),
                entries: Some(vec![PolicyEntryInfo {
                    resource: Some(CheetahString::from_string(resource.to_string())),
                    actions: Some(CheetahString::from_string(actions.to_string())),
                    source_ips: None,
                    decision: Some(CheetahString::from_static_str("Allow")),
                }]),
            }]),
        }
    }

    #[tokio::test]
    async fn auth_acl_handlers_round_trip() {
        let mut runtime = new_test_runtime("round-trip").await;
        let inner = runtime.inner_for_test().clone();
        let auth_admin_service = Arc::new(
            AuthAdminService::new(AuthConfig {
                auth_config_path: inner.broker_config().auth_config_path.clone(),
                ..AuthConfig::default()
            })
            .expect("create auth admin service"),
        );

        let mut admin = User::of_with_type("admin", "secret", UserType::Super);
        admin.set_user_status(UserStatus::Enable);
        auth_admin_service.create_user(admin).await.expect("create admin user");
        let mut alice = User::of_with_type("alice", "secret", UserType::Normal);
        alice.set_user_status(UserStatus::Enable);
        auth_admin_service.create_user(alice).await.expect("create alice user");

        let mut create_handler = CreateAclRequestHandler::new(inner.clone(), auth_admin_service.clone());
        let mut update_handler = UpdateAclRequestHandler::new(inner.clone(), auth_admin_service.clone());
        let mut get_handler = GetAclRequestHandler::new(inner.clone(), auth_admin_service.clone());
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));

        let mut create_request = RemotingCommand::create_request_command(
            RequestCode::AuthCreateAcl,
            CreateAclRequestHeader {
                subject: CheetahString::from_static_str("User:alice"),
            },
        )
        .set_body(
            build_acl_info("Topic:topic-a", "Pub")
                .encode()
                .expect("encode create acl body"),
        );
        create_request.ensure_ext_fields_initialized();
        create_request.add_ext_field("AccessKey", "admin");
        create_request.make_custom_header_to_net();
        let create_response = create_handler
            .create_acl(
                channel.clone(),
                ctx.clone(),
                RequestCode::AuthCreateAcl,
                &mut create_request,
            )
            .await
            .expect("create acl should succeed")
            .expect("create acl should return response");
        assert_eq!(ResponseCode::from(create_response.code()), ResponseCode::Success);

        let mut update_request = RemotingCommand::create_request_command(
            RequestCode::AuthUpdateAcl,
            UpdateAclRequestHeader {
                subject: CheetahString::from_static_str("User:alice"),
            },
        )
        .set_body(
            build_acl_info("Topic:topic-b", "Sub")
                .encode()
                .expect("encode update acl body"),
        );
        update_request.ensure_ext_fields_initialized();
        update_request.add_ext_field("AccessKey", "admin");
        update_request.make_custom_header_to_net();
        let update_response = update_handler
            .update_acl(
                channel.clone(),
                ctx.clone(),
                RequestCode::AuthUpdateAcl,
                &mut update_request,
            )
            .await
            .expect("update acl should succeed")
            .expect("update acl should return response");
        assert_eq!(ResponseCode::from(update_response.code()), ResponseCode::Success);

        let mut get_request = RemotingCommand::create_request_command(
            RequestCode::AuthGetAcl,
            GetAclRequestHeader {
                subject: CheetahString::from_static_str("User:alice"),
            },
        );
        get_request.make_custom_header_to_net();
        let mut get_response = get_handler
            .get_acl(channel, ctx, RequestCode::AuthGetAcl, &mut get_request)
            .await
            .expect("get acl should succeed")
            .expect("get acl should return response");
        assert_eq!(ResponseCode::from(get_response.code()), ResponseCode::Success);
        let body = AclInfo::decode(
            get_response
                .take_body()
                .expect("get acl response should contain body")
                .as_ref(),
        )
        .expect("decode acl body");
        let entries = body
            .policies
            .as_ref()
            .and_then(|policies| policies.first())
            .and_then(|policy| policy.entries.as_ref())
            .expect("entries should exist");
        assert_eq!(entries.len(), 2);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
