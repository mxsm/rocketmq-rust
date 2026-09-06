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

use crate::manager::ControllerManager;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use std::collections::HashMap;
use std::collections::HashSet;

use super::ControllerRequestProcessor;

impl ControllerRequestProcessor {
    pub(super) fn init_config_blacklist(controller_manager: &ControllerManager) -> HashSet<String> {
        let mut blacklist = HashSet::from([
            "configBlackList".to_string(),
            "configStorePath".to_string(),
            "rocketmqHome".to_string(),
        ]);

        for item in controller_manager.controller_config().config_black_list.split(';') {
            let trimmed = item.trim();
            if !trimmed.is_empty() {
                blacklist.insert(trimmed.to_string());
            }
        }

        blacklist
    }

    pub(super) async fn handle_update_controller_config(
        &self,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let body = request
            .body()
            .ok_or_else(|| RocketMQError::request_body_invalid("UPDATE_CONTROLLER_CONFIG", "request body not exist"))?;
        let properties = Self::parse_properties_from_string(body)?;
        if properties.is_empty() {
            return Err(RocketMQError::request_body_invalid(
                "UPDATE_CONTROLLER_CONFIG",
                "update config found empty config",
            ));
        }
        if self.validate_blacklist_config_exist(&properties) {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::NoPermission,
                "Cannot update blacklisted configuration".to_string(),
            )));
        }

        self.controller_manager()?.update_config(properties).await?;
        Ok(Some(self.command_factory.create_success_response_command()))
    }

    fn parse_properties_from_string(body: &[u8]) -> RocketMQResult<HashMap<String, String>> {
        let content = String::from_utf8(body.to_vec())
            .map_err(|error| RocketMQError::request_body_source("UPDATE_CONTROLLER_CONFIG", error))?;
        let mut properties = HashMap::new();

        for line in content.lines() {
            if let Some((key, value)) = line.split_once('=') {
                properties.insert(key.trim().to_string(), value.trim().to_string());
            }
        }

        Ok(properties)
    }

    pub(super) fn handle_get_controller_config(&self) -> RocketMQResult<Option<RemotingCommand>> {
        let config_string = self.controller_manager()?.controller_config().to_properties_string();
        Ok(Some(
            self.command_factory
                .create_success_response_command()
                .set_body(config_string.into_bytes()),
        ))
    }

    fn validate_blacklist_config_exist(&self, properties: &HashMap<String, String>) -> bool {
        self.config_blacklist
            .iter()
            .any(|black_config| properties.contains_key(black_config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_properties_from_string_rejects_invalid_utf8_as_request_body() {
        let error = ControllerRequestProcessor::parse_properties_from_string(&[0xff])
            .expect_err("invalid utf8 should be rejected");

        assert_eq!(error.descriptor(), &rocketmq_error::PROTOCOL_BODY_INVALID);
        assert!(error.to_string().contains("UPDATE_CONTROLLER_CONFIG"));
    }
}
