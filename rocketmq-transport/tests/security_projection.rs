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

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_security_api::Action;
use rocketmq_security_api::AuthenticatedRequestContext;
use rocketmq_security_api::Decision;
use rocketmq_security_api::OutboundSigner;
use rocketmq_security_api::PeerInfo;
use rocketmq_security_api::Principal;
use rocketmq_security_api::RequestPolicy;
use rocketmq_security_api::Resource;
use rocketmq_security_api::Secret;
use rocketmq_security_api::SecurityRequestView;
use rocketmq_security_api::Signature;
use rocketmq_security_api::SigningError;
use rocketmq_transport::security::TransportSecurity;

struct AllowPolicy;

impl RequestPolicy for AllowPolicy {
    fn evaluate_authenticated(&self, context: AuthenticatedRequestContext<'_>) -> Decision {
        assert_eq!(context.request().code(), 105);
        assert_eq!(context.resource().name(), "TopicA");
        Decision::Allow
    }
}

struct TestSigner;

impl OutboundSigner for TestSigner {
    fn sign(&self, request: SecurityRequestView<'_>) -> Result<Signature, SigningError> {
        assert_eq!(request.body().unwrap(), b"payload");
        Ok(Signature::new(vec![(
            CheetahString::from_static_str("Signature"),
            Secret::new(CheetahString::from_static_str("secret-value")),
        )]))
    }
}

#[test]
fn command_projection_borrows_canonical_values_and_injected_ports() {
    let peer = PeerInfo::new("127.0.0.1:10911".parse().unwrap(), true);
    let principal = Principal::new("tenant-user");
    let mut command = RemotingCommand::create_remoting_command(105)
        .set_version(403)
        .set_body("payload");
    command.add_ext_field("topic", "TopicA");
    let security = TransportSecurity::new(Some(Arc::new(AllowPolicy)), Some(Arc::new(TestSigner)));

    assert_eq!(
        security.authorize(
            &command,
            Some(&peer),
            Some(&principal),
            Resource::topic("TopicA"),
            Action::Publish,
        ),
        Decision::Allow
    );
    security.sign(&mut command, Some(&peer)).unwrap();
    assert_eq!(command.code(), 105);
    assert_eq!(command.version(), 403);
    assert_eq!(command.ext_fields().unwrap().get("Signature").unwrap(), "secret-value");
}

#[test]
fn missing_principal_is_fail_closed_without_an_auth_provider_dependency() {
    let command = RemotingCommand::create_remoting_command(105);
    let security = TransportSecurity::new(Some(Arc::new(AllowPolicy)), None);
    assert!(matches!(
        security.authorize(&command, None, None, Resource::topic("TopicA"), Action::Publish,),
        Decision::Deny { .. }
    ));
}
