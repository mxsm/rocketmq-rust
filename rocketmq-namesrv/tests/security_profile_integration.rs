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

use std::sync::Arc;

use rocketmq_namesrv::security::classify_namesrv_request;
use rocketmq_namesrv::security::NameServerTransportPolicy;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_security_api::Action;
use rocketmq_security_api::Decision;
use rocketmq_security_api::Principal;
use rocketmq_security_api::Resource;
use rocketmq_security_api::ResourceKind;
use rocketmq_transport::api::TransportSecurity;

const NAMESRV_REQUESTS: [RequestCode; 23] = [
    RequestCode::GetRouteinfoByTopic,
    RequestCode::RegisterBroker,
    RequestCode::UnregisterBroker,
    RequestCode::BrokerHeartbeat,
    RequestCode::GetKvConfig,
    RequestCode::QueryDataVersion,
    RequestCode::GetBrokerMemberGroup,
    RequestCode::GetBrokerClusterInfo,
    RequestCode::GetAllTopicListFromNameserver,
    RequestCode::GetKvlistByNamespace,
    RequestCode::GetTopicsByCluster,
    RequestCode::GetSystemTopicListFromNs,
    RequestCode::GetUnitTopicList,
    RequestCode::GetHasUnitSubTopicList,
    RequestCode::GetHasUnitSubUnunitTopicList,
    RequestCode::GetNamesrvConfig,
    RequestCode::PutKvConfig,
    RequestCode::DeleteKvConfig,
    RequestCode::WipeWritePermOfBroker,
    RequestCode::AddWritePermOfBroker,
    RequestCode::DeleteTopicInNamesrv,
    RequestCode::RegisterTopicInNamesrv,
    RequestCode::UpdateNamesrvConfig,
];

#[test]
fn secure_transport_policy_covers_all_nameserver_codes_and_denies_unknown_codes() {
    let security = TransportSecurity::secure_enforced(Some(Arc::new(NameServerTransportPolicy)), None);
    let principal = Principal::new("namesrv.protocol-authorization");
    let resource = Resource::new(ResourceKind::Other, "namesrv");

    for request_code in NAMESRV_REQUESTS {
        assert!(classify_namesrv_request(request_code).is_some());
        let command = RemotingCommand::create_remoting_command(request_code.to_i32());
        assert_eq!(
            security.authorize(&command, None, Some(&principal), resource.clone(), Action::Manage),
            Decision::Allow
        );
    }

    let command = RemotingCommand::create_remoting_command(RequestCode::SendMessage.to_i32());
    assert!(matches!(
        security.authorize(&command, None, Some(&principal), resource, Action::Manage),
        Decision::Deny { .. }
    ));
}
