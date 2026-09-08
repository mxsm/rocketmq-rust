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

use cheetah_string::CheetahString;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::controller_rollout::RolloutQuorumStatus;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::api::OutboundRequestOutcome;

use super::admin_request_error;
use crate::common::retry_policy::RetryInput;
use crate::implementation::mq_client_api_impl::MQClientAPIImpl;
use crate::{ClientError, ClientResult};

impl MQClientAPIImpl {
    /// Requests fresh replication evidence from the current Controller leader.
    ///
    /// # Errors
    ///
    /// Rejects a missing, malformed, denied, or unsupported response, including an
    /// older Java peer that ignores the opt-in field and returns ordinary metadata.
    pub async fn check_controller_rollout(
        &self,
        controller_address: CheetahString,
        target_node_id: u64,
        timeout_millis: u64,
    ) -> ClientResult<RolloutQuorumStatus> {
        if target_node_id == 0 || controller_address.trim().is_empty() {
            return Err(ClientError::illegal_argument(
                "Controller address and nonzero rollout target are required",
            ));
        }
        let mut request = self.create_remoting_command(RequestCode::ControllerGetMetadataInfo);
        request.add_ext_field("checkQuorumForNode", target_node_id.to_string());
        let response = match self
            .remoting_client
            .invoke_request(Some(&controller_address), request, timeout_millis)
            .await
        {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "check_controller_rollout",
                    RetryInput::Rejected(rejection),
                ))
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "check_controller_rollout",
                    RetryInput::Contract(contract),
                ))
            }
            Err(error) => return Err(ClientError::from_shared(error.into_shared_error())),
        };
        decode_rollout_response(&response, target_node_id)
    }
}

fn decode_rollout_response(response: &RemotingCommand, target: u64) -> ClientResult<RolloutQuorumStatus> {
    if response.code() != ResponseCode::Success as i32 {
        return Err(ClientError::response_process_failed(
            "check Controller rollout",
            "Controller did not confirm quorum",
        ));
    }
    let body = response.body().ok_or_else(|| {
        ClientError::response_process_failed(
            "check Controller rollout",
            "Controller does not support rollout observations",
        )
    })?;
    let status: RolloutQuorumStatus = serde_json::from_slice(body)
        .map_err(|source| ClientError::response_process_source("decode Controller rollout observation", source))?;
    if !status.permits(target) {
        return Err(ClientError::response_process_failed(
            "check Controller rollout",
            "remaining Controller quorum is not verified",
        ));
    }
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollout_requires_explicit_supported_quorum_body() {
        let factory = rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory();
        let legacy = factory.create_success_response_command();
        assert!(decode_rollout_response(&legacy, 2).is_err());
        let malformed = legacy.clone().set_body(b"{}".to_vec());
        assert!(decode_rollout_response(&malformed, 2).is_err());
        let status = RolloutQuorumStatus {
            schema_version: 1,
            target_node_id: 2,
            leader_id: 1,
            voters: vec![1, 2, 3],
            remaining_ready_voters: vec![1, 3],
            allowed: true,
            peer_endpoints: (1..=3).map(|id| (id, format!("node-{id}:9879"))).collect(),
        };
        let supported = legacy.set_body(serde_json::to_vec(&status).unwrap());
        assert_eq!(decode_rollout_response(&supported, 2).unwrap(), status);
        assert!(decode_rollout_response(&supported, 3).is_err());
        let denied = factory.create_response_command_with_code_remark(ResponseCode::NoPermission, "denied");
        assert!(decode_rollout_response(&denied, 2).is_err());
    }
}
