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

use std::str::FromStr;

use rocketmq_error::RocketMQError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BrokerRequestCode {
    RegisterBroker = 103,
    BrokerHeartbeat = 904,
    GetBrokerClusterInfo = 106,
}

impl BrokerRequestCode {
    pub fn value_of(code: i32) -> Option<Self> {
        match code {
            103 => Some(BrokerRequestCode::RegisterBroker),
            106 => Some(BrokerRequestCode::GetBrokerClusterInfo),
            _ => None,
        }
    }

    pub fn get_code(&self) -> i32 {
        *self as i32
    }

    pub fn get_type_from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "REGISTERBROKER" => Some(BrokerRequestCode::RegisterBroker),
            "BROKERHEARTBEAT" => Some(BrokerRequestCode::BrokerHeartbeat),
            "GETBROKERCLUSTERINFO" => Some(BrokerRequestCode::GetBrokerClusterInfo),
            _ => None,
        }
    }
}

impl FromStr for BrokerRequestCode {
    type Err = RocketMQError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "REGISTERBROKER" => Ok(BrokerRequestCode::RegisterBroker),
            "BROKERHEARTBEAT" => Ok(BrokerRequestCode::BrokerHeartbeat),
            "GETBROKERCLUSTERINFO" => Ok(BrokerRequestCode::GetBrokerClusterInfo),
            _ => Err(RocketMQError::illegal_argument(format!(
                "Parse from string error,Invalid BrokerRequestCode: {s}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_of() {
        let code: i32 = 103;
        assert_eq!(
            BrokerRequestCode::value_of(code),
            Some(BrokerRequestCode::RegisterBroker)
        );

        let code: i32 = 1;
        assert_eq!(BrokerRequestCode::value_of(code), None);
    }

    #[test]
    fn test_get_type_from_name() {
        let name = "RegisterBroker";
        assert_eq!(
            BrokerRequestCode::get_type_from_name(name),
            Some(BrokerRequestCode::RegisterBroker)
        );

        let name = "UNKNOW";
        assert_eq!(BrokerRequestCode::get_type_from_name(name), None);
    }
}
