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

pub(super) fn lite_subscription_ctl_request(
    lite_subscription_dto: LiteSubscriptionDTO,
) -> RocketMQResult<RemotingCommand> {
    let mut request_body = LiteSubscriptionCtlRequestBody::new();
    request_body.set_subscription_set(vec![lite_subscription_dto]);
    let mut request = RemotingCommand::create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {});
    request.set_body_mut_ref(request_body.encode()?);
    Ok(request)
}

pub(super) fn notification_request(request_header: NotificationRequestHeader) -> RemotingCommand {
    RemotingCommand::create_request_command(RequestCode::Notification, request_header)
}

pub(super) fn create_and_update_plain_access_config_request(
    plain_access_config: &PlainAccessConfig,
) -> RocketMQResult<RemotingCommand> {
    let mut request = RemotingCommand::create_request_command(RequestCode::UpdateAndCreateAclConfig, EmptyHeader {});
    request.set_body_mut_ref(plain_access_config.encode()?);
    Ok(request)
}

pub(super) fn get_acl_request(subject: CheetahString) -> RemotingCommand {
    RemotingCommand::create_request_command(RequestCode::AuthGetAcl, GetAclRequestHeader { subject })
}

pub(super) fn delete_plain_access_config_request(access_key: &CheetahString) -> RemotingCommand {
    RemotingCommand::create_request_command(RequestCode::DeleteAclConfig, EmptyHeader {})
        .set_body(access_key.as_str().as_bytes().to_vec())
}

pub(super) fn heartbeat_request(
    heartbeat_data: &HeartbeatData,
    language: LanguageCode,
) -> RocketMQResult<RemotingCommand> {
    Ok(
        RemotingCommand::create_request_command(RequestCode::HeartBeat, HeartbeatRequestHeader::default())
            .set_language(language)
            .set_body(heartbeat_data.encode()?),
    )
}

pub(super) fn get_all_consumer_offset_request() -> RemotingCommand {
    RemotingCommand::create_remoting_command(RequestCode::GetAllConsumerOffset)
}

pub(super) fn create_topic_list_request(topic_config_list: Vec<TopicConfig>) -> RocketMQResult<RemotingCommand> {
    let body = CreateTopicListRequestBody { topic_config_list };
    Ok(RemotingCommand::create_request_command(
        RequestCode::UpdateAndCreateTopicList,
        CreateTopicListRequestHeader::default(),
    )
    .set_body(body.encode()?))
}

pub(super) fn create_subscription_group_list_request(
    configs: Vec<SubscriptionGroupConfig>,
) -> RocketMQResult<RemotingCommand> {
    let body = SubscriptionGroupList {
        group_config_list: configs,
    };
    Ok(
        RemotingCommand::create_request_command(RequestCode::UpdateAndCreateSubscriptionGroupList, EmptyHeader {})
            .set_body(body.encode()?),
    )
}
