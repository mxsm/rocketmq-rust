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

use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::mix_all::is_sys_consumer_group;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::delete_subscription_group_list_request_body::DeleteSubscriptionGroupListRequestBody;
use rocketmq_protocol::protocol::body::subscription_group_list::SubscriptionGroupList;
use rocketmq_protocol::protocol::header::delete_subscription_group_request_header::DeleteSubscriptionGroupRequestHeader;
use rocketmq_protocol::protocol::header::get_subscription_group_config_request_header::GetSubscriptionGroupConfigRequestHeader;
use rocketmq_protocol::protocol::header::update_group_forbidden_request_header::UpdateGroupForbiddenRequestHeader;
use rocketmq_protocol::protocol::header::update_subscription_group_config_cas_request_header::UpdateSubscriptionGroupConfigCasRequestHeader;
use rocketmq_protocol::protocol::header::update_subscription_group_config_cas_response_header::UpdateSubscriptionGroupConfigCasResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::subscription::group_forbidden::GroupForbidden;
use rocketmq_protocol::protocol::subscription::subscription_group_config::validate_subscription_group_configs;
use rocketmq_protocol::protocol::subscription::subscription_group_config::validate_subscription_group_name;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::BrokerAdminStore;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use std::collections::HashSet;
use tracing::info;

use crate::broker::broker_admin_runtime::BrokerAdminRuntime;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigCasError;

pub(super) struct SubscriptionGroupHandler;

impl SubscriptionGroupHandler {
    pub(super) const fn new() -> Self {
        Self
    }

    pub async fn update_and_create_subscription_group<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let start_time = current_millis() as i64;

        let response = RemotingCommand::create_java_default_error_response_command();

        info!(
            "AdminBrokerProcessor#updateAndCreateSubscriptionGroup called by {}",
            _channel.remote_address()
        );
        let Some(body) = request.get_body() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("subscription group body is required"),
            ));
        };
        let mut config = match SubscriptionGroupConfig::decode(body) {
            Ok(config) => config,
            Err(_) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark("subscription group body is malformed"),
                ));
            }
        };
        if let Err(error) = validate_subscription_group_name(config.group_name().as_str()) {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark(error.to_string()),
            ));
        }
        if !broker_runtime_inner
            .subscription_group_manager()
            .update_subscription_group_config(&mut config)
        {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("subscription group configuration was rejected"),
            ));
        }
        let execution_time = current_millis() as i64 - start_time;
        info!(
            "executionTime of create subscriptionGroup:{} is {} ms",
            config.group_name(),
            execution_time
        );

        // todo
        // InvocationStatus status =
        // response.getCode() == ResponseCode.SUCCESS ? InvocationStatus.SUCCESS :
        // InvocationStatus.FAILURE; Attributes attributes =
        // BrokerMetricsManager.newAttributesBuilder()     .put(LABEL_INVOCATION_STATUS,
        // status.getName())     .build();
        // BrokerMetricsManager.consumerGroupCreateExecuteTime.record(executionTime, attributes);
        Ok(Some(RemotingCommand::create_success_response_command()))
    }

    pub async fn update_subscription_group_config_cas<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_java_default_error_response_command().set_opaque(request.opaque());
        let request_header =
            match request.decode_command_custom_header::<UpdateSubscriptionGroupConfigCasRequestHeader>() {
                Ok(header) => header,
                Err(_) => {
                    return Ok(Some(response.set_code(ResponseCode::InvalidParameter).set_remark(
                        "group, expectedVersion, and a valid allowlisted patch are required",
                    )));
                }
            };
        info!(
            "Broker receive version-checked Subscription Group patch for group={}, caller address={}",
            request_header.group,
            channel.remote_address()
        );

        if let Err(error) = validate_subscription_group_name(request_header.group.as_str()) {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark(error.to_string()),
            ));
        }
        if is_sys_consumer_group(request_header.group.as_str()) {
            return Ok(Some(response.set_code(ResponseCode::InvalidParameter).set_remark(
                "system Subscription Group configuration is not eligible for supervised patching",
            )));
        }
        if request_header.retry_max_times.is_none()
            && request_header.retry_queue_nums.is_none()
            && request_header.consume_timeout_minutes.is_none()
        {
            return Ok(Some(response.set_code(ResponseCode::InvalidParameter).set_remark(
                "Subscription Group configuration patch must contain at least one allowlisted field",
            )));
        }

        let retry_max_times = match request_header.retry_max_times {
            Some(value) if (1..=16).contains(&value) => Some(value as u32),
            Some(_) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark("retryMaxTimes must be between 1 and 16"),
                ));
            }
            None => None,
        };
        let retry_queue_nums = match request_header.retry_queue_nums {
            Some(value) if (1..=8).contains(&value) => Some(value as u32),
            Some(_) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark("retryQueueNums must be between 1 and 8"),
                ));
            }
            None => None,
        };
        let consume_timeout_minutes = match request_header.consume_timeout_minutes {
            Some(value) if (1..=1_440).contains(&value) => Some(value as u32),
            Some(_) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark("consumeTimeoutMinutes must be between 1 and 1440"),
                ));
            }
            None => None,
        };

        let update = match broker_runtime_inner
            .subscription_group_manager()
            .update_subscription_group_config_if_version(
                &request_header.group,
                request_header.expected_version,
                retry_max_times,
                retry_queue_nums,
                consume_timeout_minutes,
            ) {
            Ok(update) => update,
            Err(SubscriptionGroupConfigCasError::InvalidGroupName) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark("The specified group is invalid."),
                ));
            }
            Err(SubscriptionGroupConfigCasError::GroupNotFound) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SubscriptionGroupNotExist)
                        .set_remark("Subscription Group configuration does not exist on this Broker"),
                ));
            }
            Err(SubscriptionGroupConfigCasError::VersionConflict {
                expected_version,
                actual_version,
            }) => {
                return Ok(Some(
                    response
                        .set_command_custom_header(UpdateSubscriptionGroupConfigCasResponseHeader {
                            subscription_group_version: actual_version,
                        })
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark(format!(
                            "Subscription Group configuration version conflict: expected={expected_version}, \
                             actual={actual_version}"
                        )),
                ));
            }
            Err(SubscriptionGroupConfigCasError::NoChange) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark("Subscription Group configuration patch has no effect"),
                ));
            }
            Err(SubscriptionGroupConfigCasError::ValueOutOfRange) => {
                return Ok(Some(response.set_code(ResponseCode::InvalidParameter).set_remark(
                    "Subscription Group configuration patch contains an out-of-range value",
                )));
            }
            Err(SubscriptionGroupConfigCasError::VersionUnavailable) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("Subscription Group configuration version is unavailable"),
                ));
            }
            Err(SubscriptionGroupConfigCasError::VersionExhausted) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("Subscription Group configuration version is exhausted"),
                ));
            }
        };
        let subscription_group_version = match u64::try_from(update.data_version.counter()) {
            Ok(version) => version,
            Err(_) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("Subscription Group configuration version is unavailable"),
                ));
            }
        };

        Ok(Some(
            RemotingCommand::create_success_response_command_with_header(
                UpdateSubscriptionGroupConfigCasResponseHeader {
                    subscription_group_version,
                },
            )
            .set_opaque(request.opaque())
            .set_remark(format!(
                "Subscription Group configuration patch committed, version={subscription_group_version}"
            )),
        ))
    }

    pub async fn get_subscription_group_config<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_java_default_error_response_command();
        let request_header = request.decode_command_custom_header::<GetSubscriptionGroupConfigRequestHeader>()?;
        let group = &request_header.group;
        let group_config = broker_runtime_inner
            .subscription_group_manager()
            .select_subscription_group_config_with_version(group);

        match group_config {
            Ok((config, subscription_group_version)) => Ok(Some(
                RemotingCommand::create_success_response_command_with_header(
                    UpdateSubscriptionGroupConfigCasResponseHeader {
                        subscription_group_version,
                    },
                )
                .set_body(config.encode()?),
            )),
            Err(SubscriptionGroupConfigCasError::GroupNotFound) => Ok(Some(
                response
                    .set_code(ResponseCode::SubscriptionGroupNotExist)
                    .set_remark(format!("No group in this broker. group: {}", group)),
            )),
            Err(SubscriptionGroupConfigCasError::InvalidGroupName) => Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("The specified group is invalid."),
            )),
            Err(
                SubscriptionGroupConfigCasError::VersionUnavailable | SubscriptionGroupConfigCasError::VersionExhausted,
            ) => Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("Subscription Group configuration version is unavailable"),
            )),
            Err(
                SubscriptionGroupConfigCasError::VersionConflict { .. }
                | SubscriptionGroupConfigCasError::ValueOutOfRange
                | SubscriptionGroupConfigCasError::NoChange,
            ) => {
                Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(
                    "Subscription Group configuration snapshot is unavailable",
                )))
            }
        }
    }

    pub async fn update_and_create_subscription_group_list<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        info!(
            "AdminBrokerProcessor#updateAndCreateSubscriptionGroupList called by {}",
            channel.remote_address()
        );

        let response = RemotingCommand::create_java_default_error_response_command();
        let Some(body) = request.get_body() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("subscription group list body is required"),
            ));
        };
        let subscription_group_list = match SubscriptionGroupList::decode(body) {
            Ok(list) => list,
            Err(_) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark("subscription group list body is malformed"),
                ));
            }
        };
        if let Err(error) = validate_subscription_group_configs(&subscription_group_list.group_config_list) {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark(error.to_string()),
            ));
        }

        if !broker_runtime_inner
            .subscription_group_manager()
            .update_subscription_group_config_list(subscription_group_list.group_config_list)
        {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("subscription group configuration list was rejected"),
            ));
        }
        Ok(Some(RemotingCommand::create_success_response_command()))
    }

    pub async fn delete_subscription_group<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<DeleteSubscriptionGroupRequestHeader>()?;
        info!(
            "AdminBrokerProcessor#deleteSubscriptionGroup called by {}",
            channel.remote_address()
        );

        let should_clean_offset = request_header.clean_offset
            || broker_runtime_inner
                .subscription_group_manager()
                .find_subscription_group_config(&request_header.group_name)
                .and_then(|config| config.lite_bind_topic().cloned())
                .is_some();

        broker_runtime_inner
            .subscription_group_manager()
            .delete_subscription_group_config(request_header.group_name.as_str());

        if let Some(processor) = broker_runtime_inner.pop_message_processor() {
            if let Err(error) = processor
                .remove_consumer_profile(request_header.group_name.clone())
                .await
            {
                return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::ServiceNotAvailable,
                    format!("failed to remove POP consumer profile: {error}"),
                )));
            }
        }

        if should_clean_offset {
            broker_runtime_inner
                .consumer_offset_manager()
                .clean_offset_by_group(&request_header.group_name);
            broker_runtime_inner
                .pop_inflight_message_counter()
                .clear_in_flight_message_num_by_group_name(&request_header.group_name);
        }

        Ok(Some(RemotingCommand::create_success_response_command()))
    }

    pub async fn delete_subscription_group_list<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_java_default_error_response_command();
        let Some(encoded) = request.body() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("The specified group name list is blank."),
            ));
        };
        let request_body = match DeleteSubscriptionGroupListRequestBody::decode(encoded.as_ref()) {
            Ok(body) => body,
            Err(_) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark("The specified group name list body is invalid."),
                ));
            }
        };
        if request_body.group_name_list.is_empty() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("The specified group name list is blank."),
            ));
        }

        let mut seen = HashSet::new();
        let mut groups = Vec::with_capacity(request_body.group_name_list.len());
        for group in request_body.group_name_list {
            if let Err(error) = validate_subscription_group_name(group.as_str()) {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark(error.to_string()),
                ));
            }
            if seen.insert(group.clone()) {
                groups.push(group);
            }
        }
        info!(
            "AdminBrokerProcessor#deleteSubscriptionGroupList: groupNames={:?}, caller={}",
            groups,
            channel.remote_address()
        );

        let clean_offsets = groups
            .iter()
            .filter(|group| {
                request_body.clean_offset
                    || broker_runtime_inner
                        .subscription_group_manager()
                        .find_subscription_group_config(group)
                        .and_then(|config| config.lite_bind_topic().cloned())
                        .is_some()
            })
            .cloned()
            .collect::<Vec<_>>();
        broker_runtime_inner
            .subscription_group_manager()
            .delete_subscription_group_config_list(&groups);
        if let Some(processor) = broker_runtime_inner.pop_message_processor() {
            for group in &groups {
                if let Err(error) = processor.remove_consumer_profile(group.clone()).await {
                    return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::ServiceNotAvailable,
                        format!("failed to remove POP consumer profile for {group}: {error}"),
                    )));
                }
            }
        }
        for group in clean_offsets {
            broker_runtime_inner
                .consumer_offset_manager()
                .clean_offset_by_group(&group);
            broker_runtime_inner
                .pop_inflight_message_counter()
                .clear_in_flight_message_num_by_group_name(&group);
        }
        Ok(Some(RemotingCommand::create_success_response_command()))
    }

    pub async fn update_and_get_group_forbidden<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<UpdateGroupForbiddenRequestHeader>()?;
        info!(
            "AdminBrokerProcessor#updateAndGetGroupForbidden called by {} for object {}@{} readable={:?}",
            channel.remote_address(),
            request_header.group,
            request_header.topic,
            request_header.readable
        );

        if let Some(readable) = request_header.readable {
            if readable {
                broker_runtime_inner.subscription_group_manager().clear_forbidden(
                    &request_header.group,
                    &request_header.topic,
                    PermName::INDEX_PERM_READ as i32,
                );
            } else {
                broker_runtime_inner.subscription_group_manager().set_forbidden(
                    &request_header.group,
                    &request_header.topic,
                    PermName::INDEX_PERM_READ as i32,
                );
            }
        }

        let readable = !broker_runtime_inner.subscription_group_manager().get_forbidden(
            &request_header.group,
            &request_header.topic,
            PermName::INDEX_PERM_READ as i32,
        );
        let body = GroupForbidden::new(request_header.topic, request_header.group, Some(readable));
        Ok(Some(
            RemotingCommand::create_success_response_command().set_body(body.encode()?),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::SystemTime;

    use crate::config::broker_config::BrokerConfig;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::body::subscription_group_list::SubscriptionGroupList;
    use rocketmq_protocol::protocol::header::delete_subscription_group_request_header::DeleteSubscriptionGroupRequestHeader;
    use rocketmq_protocol::protocol::header::empty_header::EmptyHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::subscription::group_forbidden::GroupForbidden;
    use rocketmq_protocol::protocol::RemotingSerializable;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_transport::api::v1::Channel;
    use rocketmq_transport::api::v1::ConnectionHandlerContextWrapper;
    use rocketmq_transport::test_support::Connection;

    use super::*;
    use crate::broker_runtime::BrokerRuntime;

    fn temp_test_root(label: &str) -> std::path::PathBuf {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time should move forward")
            .as_millis();
        std::env::temp_dir().join(format!("rocketmq-rust-admin-sub-group-{label}-{millis}"))
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
        assert!(runtime.initialize().await.is_ok());
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
        rocketmq_transport::test_support::TestChannelBuilder::new(connection, crate::test_task_group("channel"))
            .addresses(local_addr, local_addr)
            .build()
            .expect("build test channel")
    }

    #[tokio::test]
    async fn update_and_create_subscription_group_list_persists_multiple_groups() {
        let runtime = new_test_runtime("update-list").await;
        let admin = runtime.admin_runtime_for_test();
        let handler = SubscriptionGroupHandler::new();

        let body = SubscriptionGroupList {
            group_config_list: vec![
                SubscriptionGroupConfig::new(CheetahString::from_static_str("group-a")),
                SubscriptionGroupConfig::new(CheetahString::from_static_str("group-b")),
            ],
        };
        let mut request =
            RemotingCommand::create_request_command(RequestCode::UpdateAndCreateSubscriptionGroupList, EmptyHeader {})
                .set_body(body.encode().expect("encode subscription group list"));

        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let response = handler
            .update_and_create_subscription_group_list(
                &admin,
                channel,
                ctx,
                RequestCode::UpdateAndCreateSubscriptionGroupList,
                &mut request,
            )
            .await
            .expect("batch update request should succeed")
            .expect("batch update request should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(admin
            .subscription_group_manager()
            .find_subscription_group_config(&CheetahString::from_static_str("group-a"))
            .is_some());
        assert!(admin
            .subscription_group_manager()
            .find_subscription_group_config(&CheetahString::from_static_str("group-b"))
            .is_some());

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn subscription_group_updates_fail_closed_without_mutating_state() {
        let runtime = new_test_runtime("invalid-update").await;
        let admin = runtime.admin_runtime_for_test();
        let handler = SubscriptionGroupHandler::new();
        let before_version = admin.subscription_group_manager().data_version().read().clone();

        let mut missing = RemotingCommand::create_remoting_command(RequestCode::UpdateAndCreateSubscriptionGroup);
        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let response = handler
            .update_and_create_subscription_group(
                &admin,
                channel,
                ctx,
                RequestCode::UpdateAndCreateSubscriptionGroup,
                &mut missing,
            )
            .await
            .expect("missing body should return normally")
            .expect("missing body should return a response");
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);

        let mut malformed = RemotingCommand::create_remoting_command(RequestCode::UpdateAndCreateSubscriptionGroup)
            .set_body(Bytes::from_static(b"{not-json"));
        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let response = handler
            .update_and_create_subscription_group(
                &admin,
                channel,
                ctx,
                RequestCode::UpdateAndCreateSubscriptionGroup,
                &mut malformed,
            )
            .await
            .expect("malformed body should return normally")
            .expect("malformed body should return a response");
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);

        let invalid_group = CheetahString::from_static_str("invalid.group");
        let invalid = SubscriptionGroupConfig::new(invalid_group.clone());
        let mut single = RemotingCommand::create_remoting_command(RequestCode::UpdateAndCreateSubscriptionGroup)
            .set_body(invalid.encode().expect("invalid config should still encode"));
        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let response = handler
            .update_and_create_subscription_group(
                &admin,
                channel,
                ctx,
                RequestCode::UpdateAndCreateSubscriptionGroup,
                &mut single,
            )
            .await
            .expect("invalid group should return normally")
            .expect("invalid group should return a response");
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);
        assert!(!admin
            .subscription_group_manager()
            .contains_subscription_group(&invalid_group));

        let valid_group = CheetahString::from_static_str("valid-group");
        let body = SubscriptionGroupList {
            group_config_list: vec![
                SubscriptionGroupConfig::new(valid_group.clone()),
                SubscriptionGroupConfig::new(invalid_group.clone()),
            ],
        };
        let mut list =
            RemotingCommand::create_request_command(RequestCode::UpdateAndCreateSubscriptionGroupList, EmptyHeader {})
                .set_body(body.encode().expect("subscription group list should encode"));
        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let response = handler
            .update_and_create_subscription_group_list(
                &admin,
                channel,
                ctx,
                RequestCode::UpdateAndCreateSubscriptionGroupList,
                &mut list,
            )
            .await
            .expect("invalid list should return normally")
            .expect("invalid list should return a response");
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);
        assert!(!admin
            .subscription_group_manager()
            .contains_subscription_group(&valid_group));
        assert_eq!(
            *admin.subscription_group_manager().data_version().read(),
            before_version
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn delete_subscription_group_cleans_offsets_for_lite_group_even_without_flag() {
        let runtime = new_test_runtime("delete-group").await;
        let admin = runtime.admin_runtime_for_test();
        let mut config = SubscriptionGroupConfig::new(CheetahString::from_static_str("group-a"));
        config.set_lite_bind_topic(Some(CheetahString::from_static_str("parent-topic")));
        admin
            .subscription_group_manager()
            .subscription_group_table()
            .insert(CheetahString::from_static_str("group-a"), Arc::new(config));
        admin.consumer_offset_manager().commit_offset(
            CheetahString::from_static_str("127.0.0.1"),
            &CheetahString::from_static_str("group-a"),
            &CheetahString::from_static_str("topic-a"),
            0,
            12,
        );
        admin.consumer_offset_manager().assign_reset_offset(
            &CheetahString::from_static_str("topic-a"),
            &CheetahString::from_static_str("group-a"),
            0,
            8,
        );

        let handler = SubscriptionGroupHandler::new();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::DeleteSubscriptionGroup,
            DeleteSubscriptionGroupRequestHeader {
                group_name: CheetahString::from_static_str("group-a"),
                clean_offset: false,
                rpc_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let response = handler
            .delete_subscription_group(&admin, channel, ctx, RequestCode::DeleteSubscriptionGroup, &mut request)
            .await
            .expect("delete group request should succeed")
            .expect("delete group request should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(!admin
            .subscription_group_manager()
            .contains_subscription_group(&CheetahString::from_static_str("group-a")));
        assert_eq!(
            admin.consumer_offset_manager().query_offset(
                &CheetahString::from_static_str("group-a"),
                &CheetahString::from_static_str("topic-a"),
                0,
            ),
            -1
        );
        assert!(!admin
            .consumer_offset_manager()
            .has_offset_reset("group-a", "topic-a", 0));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn update_and_get_group_forbidden_updates_readable_flag() {
        let runtime = new_test_runtime("group-forbidden").await;
        let admin = runtime.admin_runtime_for_test();
        let handler = SubscriptionGroupHandler::new();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::UpdateAndGetGroupForbidden,
            UpdateGroupForbiddenRequestHeader {
                group: CheetahString::from_static_str("group-a"),
                topic: CheetahString::from_static_str("topic-a"),
                readable: Some(false),
                topic_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut response = handler
            .update_and_get_group_forbidden(
                &admin,
                channel,
                ctx,
                RequestCode::UpdateAndGetGroupForbidden,
                &mut request,
            )
            .await
            .expect("update and get group forbidden should succeed")
            .expect("update and get group forbidden should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = GroupForbidden::decode(
            response
                .take_body()
                .expect("group forbidden response should contain body")
                .as_ref(),
        )
        .expect("decode group forbidden body");
        assert_eq!(body.group(), &CheetahString::from_static_str("group-a"));
        assert_eq!(body.topic(), &CheetahString::from_static_str("topic-a"));
        assert_eq!(body.readable(), Some(false));
        assert!(admin.subscription_group_manager().get_forbidden(
            &CheetahString::from_static_str("group-a"),
            &CheetahString::from_static_str("topic-a"),
            PermName::INDEX_PERM_READ as i32,
        ));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn subscription_group_cas_commits_once_rejects_stale_and_reports_version() {
        let runtime = new_test_runtime("subscription-group-cas").await;
        let admin = runtime.admin_runtime_for_test();
        let handler = SubscriptionGroupHandler::new();
        let group = CheetahString::from_static_str("sre-cas-group");
        let mut initial = SubscriptionGroupConfig::new(group.clone());
        initial.set_consume_enable(false);
        initial.set_consume_broadcast_enable(false);
        initial.set_group_sys_flag(17);
        admin
            .subscription_group_manager()
            .update_subscription_group_config(&mut initial);
        let (_, initial_version) = admin
            .subscription_group_manager()
            .select_subscription_group_config_with_version(&group)
            .expect("versioned Subscription Group should exist");

        let mut request = RemotingCommand::create_request_command(
            RequestCode::UpdateSubscriptionGroupConfigCas,
            UpdateSubscriptionGroupConfigCasRequestHeader {
                group: group.clone(),
                expected_version: initial_version,
                retry_max_times: Some(8),
                retry_queue_nums: Some(4),
                consume_timeout_minutes: Some(30),
            },
        );
        request.make_custom_header_to_net();
        let request_opaque = request.opaque();
        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let response = handler
            .update_subscription_group_config_cas(
                &admin,
                channel,
                ctx,
                RequestCode::UpdateSubscriptionGroupConfigCas,
                &mut request,
            )
            .await
            .expect("Subscription Group CAS should run")
            .expect("Subscription Group CAS should return a response");
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert_eq!(response.opaque(), request_opaque);
        let expected_remark = format!(
            "Subscription Group configuration patch committed, version={}",
            initial_version + 1
        );
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some(expected_remark.as_str())
        );
        let response_header = response
            .read_custom_header_ref::<UpdateSubscriptionGroupConfigCasResponseHeader>()
            .expect("success response should carry the committed version");
        assert_eq!(response_header.subscription_group_version, initial_version + 1);
        let current = admin
            .subscription_group_manager()
            .find_subscription_group_config(&group)
            .expect("Subscription Group should remain available");
        assert_eq!(current.retry_max_times(), 8);
        assert_eq!(current.retry_queue_nums(), 4);
        assert_eq!(current.consume_timeout_minute(), 30);
        assert!(!current.consume_enable());
        assert!(!current.consume_broadcast_enable());
        assert_eq!(current.group_sys_flag(), 17);

        let mut stale_request = RemotingCommand::create_request_command(
            RequestCode::UpdateSubscriptionGroupConfigCas,
            UpdateSubscriptionGroupConfigCasRequestHeader {
                group: group.clone(),
                expected_version: initial_version,
                retry_max_times: Some(7),
                retry_queue_nums: None,
                consume_timeout_minutes: None,
            },
        );
        stale_request.make_custom_header_to_net();
        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let stale_response = handler
            .update_subscription_group_config_cas(
                &admin,
                channel,
                ctx,
                RequestCode::UpdateSubscriptionGroupConfigCas,
                &mut stale_request,
            )
            .await
            .expect("stale Subscription Group CAS should return normally")
            .expect("stale Subscription Group CAS should return a response");
        assert_eq!(
            ResponseCode::from(stale_response.code()),
            ResponseCode::InvalidParameter
        );
        assert_eq!(
            stale_response
                .read_custom_header_ref::<UpdateSubscriptionGroupConfigCasResponseHeader>()
                .expect("conflict should carry current version")
                .subscription_group_version,
            initial_version + 1
        );

        let mut get_request = RemotingCommand::create_request_command(
            RequestCode::GetSubscriptionGroupConfig,
            GetSubscriptionGroupConfigRequestHeader {
                group: group.clone(),
                rpc_request_header: None,
            },
        );
        get_request.make_custom_header_to_net();
        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let get_response = handler
            .get_subscription_group_config(
                &admin,
                channel,
                ctx,
                RequestCode::GetSubscriptionGroupConfig,
                &mut get_request,
            )
            .await
            .expect("get Subscription Group config should run")
            .expect("get Subscription Group config should return a response");
        assert_eq!(
            get_response
                .read_custom_header_ref::<UpdateSubscriptionGroupConfigCasResponseHeader>()
                .expect("Subscription Group query should carry the current version")
                .subscription_group_version,
            initial_version + 1
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn batch_delete_validates_all_groups_before_mutation_and_deduplicates() {
        let runtime = new_test_runtime("batch-delete-groups").await;
        let admin = runtime.admin_runtime_for_test();
        let handler = SubscriptionGroupHandler::new();
        let group_a = CheetahString::from_static_str("BatchDeleteGroupA");
        let group_b = CheetahString::from_static_str("BatchDeleteGroupB");
        for group in [&group_a, &group_b] {
            let mut config = SubscriptionGroupConfig::new(group.clone());
            admin
                .subscription_group_manager()
                .update_subscription_group_config(&mut config);
        }

        let empty_body = rocketmq_protocol::protocol::body::delete_subscription_group_list_request_body::DeleteSubscriptionGroupListRequestBody::default();
        let mut empty = RemotingCommand::create_remoting_command(RequestCode::DeleteSubscriptionGroupList.to_i32())
            .set_body(empty_body.encode().unwrap());
        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let response = handler
            .delete_subscription_group_list(
                &admin,
                channel,
                ctx,
                RequestCode::DeleteSubscriptionGroupList,
                &mut empty,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ResponseCode::InvalidParameter, ResponseCode::from(response.code()));
        assert!(admin.subscription_group_manager().group_exists(group_a.as_str()));
        assert!(admin.subscription_group_manager().group_exists(group_b.as_str()));

        let invalid_body = rocketmq_protocol::protocol::body::delete_subscription_group_list_request_body::DeleteSubscriptionGroupListRequestBody {
            group_name_list: vec![group_a.clone(), CheetahString::from_static_str("invalid group")],
            clean_offset: true,
        };
        let mut invalid = RemotingCommand::create_remoting_command(RequestCode::DeleteSubscriptionGroupList.to_i32())
            .set_body(invalid_body.encode().unwrap());
        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let response = handler
            .delete_subscription_group_list(
                &admin,
                channel,
                ctx,
                RequestCode::DeleteSubscriptionGroupList,
                &mut invalid,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ResponseCode::InvalidParameter, ResponseCode::from(response.code()));
        assert!(admin.subscription_group_manager().group_exists(group_a.as_str()));
        assert!(admin.subscription_group_manager().group_exists(group_b.as_str()));

        let body = rocketmq_protocol::protocol::body::delete_subscription_group_list_request_body::DeleteSubscriptionGroupListRequestBody {
            group_name_list: vec![group_a.clone(), group_b.clone(), group_a.clone()],
            clean_offset: true,
        };
        let mut request = RemotingCommand::create_remoting_command(RequestCode::DeleteSubscriptionGroupList.to_i32())
            .set_body(body.encode().unwrap());
        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let response = handler
            .delete_subscription_group_list(
                &admin,
                channel,
                ctx,
                RequestCode::DeleteSubscriptionGroupList,
                &mut request,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ResponseCode::Success, ResponseCode::from(response.code()));
        assert!(!admin.subscription_group_manager().group_exists(group_a.as_str()));
        assert!(!admin.subscription_group_manager().group_exists(group_b.as_str()));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
