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

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminToolsResultCodeEnum {
    Success = 200,
    RemotingError = -1001,
    MQBrokerError = -1002,
    MQClientError = -1003,
    InterruptError = -1004,
    TopicRouteInfoNotExist = -2001,
    ConsumerNotOnline = -2002,
    BroadcastConsumption = -2003,
}

#[allow(dead_code)]
impl AdminToolsResultCodeEnum {
    pub fn get_code(&self) -> i32 {
        *self as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_code_returns_correct_value_for_success() {
        let code = AdminToolsResultCodeEnum::Success;
        assert_eq!(code.get_code(), 200);
    }

    #[test]
    fn get_code_returns_correct_value_for_remoting_error() {
        let code = AdminToolsResultCodeEnum::RemotingError;
        assert_eq!(code.get_code(), -1001);
    }

    #[test]
    fn get_code_returns_correct_value_for_mq_broker_error() {
        let code = AdminToolsResultCodeEnum::MQBrokerError;
        assert_eq!(code.get_code(), -1002);
    }

    #[test]
    fn get_code_returns_correct_value_for_mq_client_error() {
        let code = AdminToolsResultCodeEnum::MQClientError;
        assert_eq!(code.get_code(), -1003);
    }

    #[test]
    fn get_code_returns_correct_value_for_interrupt_error() {
        let code = AdminToolsResultCodeEnum::InterruptError;
        assert_eq!(code.get_code(), -1004);
    }

    #[test]
    fn get_code_returns_correct_value_for_topic_route_info_not_exist() {
        let code = AdminToolsResultCodeEnum::TopicRouteInfoNotExist;
        assert_eq!(code.get_code(), -2001);
    }

    #[test]
    fn get_code_returns_correct_value_for_consumer_not_online() {
        let code = AdminToolsResultCodeEnum::ConsumerNotOnline;
        assert_eq!(code.get_code(), -2002);
    }

    #[test]
    fn get_code_returns_correct_value_for_broadcast_consumption() {
        let code = AdminToolsResultCodeEnum::BroadcastConsumption;
        assert_eq!(code.get_code(), -2003);
    }
}
