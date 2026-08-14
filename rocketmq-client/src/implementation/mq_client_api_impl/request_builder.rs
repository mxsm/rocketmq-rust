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

use super::*;
use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;

impl MQClientAPIImpl {
    #[inline]
    pub(super) fn create_remoting_command(&self, code: impl Into<i32>) -> RemotingCommand {
        self.command_factory.create_remoting_command(code)
    }

    #[inline]
    pub(super) fn create_request(&self, code: impl Into<i32>, body: impl Into<bytes::Bytes>) -> RemotingCommand {
        self.command_factory.create_request(code, body)
    }

    #[inline]
    pub(super) fn create_request_command<T>(&self, code: impl Into<i32>, header: T) -> RemotingCommand
    where
        T: CommandCustomHeader + Send + Sync + 'static,
    {
        self.command_factory.create_request_command(code, header)
    }
}

#[cfg(feature = "admin-mutation")]
pub(super) fn lite_subscription_ctl_request(
    command_factory: &RemotingCommandFactory,
    lite_subscription_dto: LiteSubscriptionDTO,
) -> RocketMQResult<RemotingCommand> {
    let mut request_body = LiteSubscriptionCtlRequestBody::new();
    request_body.set_subscription_set(vec![lite_subscription_dto]);
    let mut request = command_factory.create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {});
    request.set_body_mut_ref(request_body.encode()?);
    Ok(request)
}

pub(super) fn notification_request(
    command_factory: &RemotingCommandFactory,
    request_header: NotificationRequestHeader,
) -> RemotingCommand {
    command_factory.create_request_command(RequestCode::Notification, request_header)
}

#[cfg(feature = "admin-mutation")]
pub(super) fn create_and_update_plain_access_config_request(
    command_factory: &RemotingCommandFactory,
    plain_access_config: &PlainAccessConfig,
) -> RocketMQResult<RemotingCommand> {
    let mut request = command_factory.create_request_command(RequestCode::UpdateAndCreateAclConfig, EmptyHeader {});
    request.set_body_mut_ref(plain_access_config.encode()?);
    Ok(request)
}

pub(super) fn get_acl_request(command_factory: &RemotingCommandFactory, subject: CheetahString) -> RemotingCommand {
    command_factory.create_request_command(RequestCode::AuthGetAcl, GetAclRequestHeader { subject })
}

#[cfg(feature = "admin-mutation")]
pub(super) fn delete_plain_access_config_request(
    command_factory: &RemotingCommandFactory,
    access_key: &CheetahString,
) -> RemotingCommand {
    command_factory
        .create_request_command(RequestCode::DeleteAclConfig, EmptyHeader {})
        .set_body(access_key.as_str().as_bytes().to_vec())
}

pub(super) fn heartbeat_request(
    command_factory: &RemotingCommandFactory,
    heartbeat_data: &HeartbeatData,
    language: LanguageCode,
) -> RocketMQResult<RemotingCommand> {
    Ok(command_factory
        .create_request_command(RequestCode::HeartBeat, HeartbeatRequestHeader::default())
        .set_language(language)
        .set_body(heartbeat_data.encode()?))
}

pub(super) fn get_all_consumer_offset_request(command_factory: &RemotingCommandFactory) -> RemotingCommand {
    command_factory.create_remoting_command(RequestCode::GetAllConsumerOffset)
}

#[cfg(feature = "admin-mutation")]
pub(super) fn create_topic_list_request(
    command_factory: &RemotingCommandFactory,
    topic_config_list: Vec<TopicConfig>,
) -> RocketMQResult<RemotingCommand> {
    let body = CreateTopicListRequestBody { topic_config_list };
    Ok(command_factory
        .create_request_command(
            RequestCode::UpdateAndCreateTopicList,
            CreateTopicListRequestHeader::default(),
        )
        .set_body(body.encode()?))
}

#[cfg(feature = "admin-mutation")]
pub(super) fn create_subscription_group_list_request(
    command_factory: &RemotingCommandFactory,
    configs: Vec<SubscriptionGroupConfig>,
) -> RocketMQResult<RemotingCommand> {
    let body = SubscriptionGroupList {
        group_config_list: configs,
    };
    Ok(command_factory
        .create_request_command(RequestCode::UpdateAndCreateSubscriptionGroupList, EmptyHeader {})
        .set_body(body.encode()?))
}

#[cfg(feature = "admin-mutation")]
pub(super) fn delete_topic_list_request(
    command_factory: &RemotingCommandFactory,
    topic_list: Vec<CheetahString>,
) -> RocketMQResult<RemotingCommand> {
    let body = DeleteTopicListRequestBody { topic_list };
    Ok(command_factory
        .create_request_command(RequestCode::DeleteTopicInBrokerList, EmptyHeader {})
        .set_body(body.encode()?))
}

#[cfg(feature = "admin-mutation")]
pub(super) fn delete_subscription_group_list_request(
    command_factory: &RemotingCommandFactory,
    group_name_list: Vec<CheetahString>,
    clean_offset: bool,
) -> RocketMQResult<RemotingCommand> {
    let body = DeleteSubscriptionGroupListRequestBody {
        group_name_list,
        clean_offset,
    };
    Ok(command_factory
        .create_request_command(RequestCode::DeleteSubscriptionGroupList, EmptyHeader {})
        .set_body(body.encode()?))
}
