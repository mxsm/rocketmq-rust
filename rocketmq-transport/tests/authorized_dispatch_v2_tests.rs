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

#![cfg(feature = "test-support")]

use bytes::Bytes;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RejectRequestDecision;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::ResponseBodyKind;
use rocketmq_transport::api::v2::ResponsePlan;

#[derive(Clone)]
struct IntegrationProcessor;

impl RequestProcessorV2 for IntegrationProcessor {
    async fn process(&mut self, _request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        Ok(HandlerOutcome::Reply(
            ResponsePlan::bytes(
                RemotingCommand::create_response_command_with_code(0),
                Bytes::from_static(b"owned integration response"),
            )
            .expect("valid integration response plan"),
        ))
    }

    fn reject_request(&self, _code: i32) -> RejectRequestDecision {
        RejectRequestDecision::Reject(
            ResponsePlan::command(RemotingCommand::create_response_command_with_code(2))
                .expect("valid structured rejection"),
        )
    }
}

fn assert_production_v2_processor<P: RequestProcessorV2 + Clone + Sync + 'static>() {}

#[test]
fn v2_processor_and_affine_response_contracts_remain_available_without_exporting_the_dispatcher() {
    assert_production_v2_processor::<IntegrationProcessor>();

    let decision = IntegrationProcessor.reject_request(91);
    let RejectRequestDecision::Reject(plan) = decision else {
        panic!("integration processor should own its structured rejection")
    };
    assert_eq!(plan.body_kind(), ResponseBodyKind::Empty);
    assert_eq!(plan.response_code(), 2);
}
