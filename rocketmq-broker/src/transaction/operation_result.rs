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

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_remoting::code::response_code::ResponseCode;

#[derive(Debug, Clone)]
pub(crate) struct OperationResult {
    pub(crate) prepare_message: Option<MessageExt>,
    pub(crate) response_remark: Option<String>,
    pub(crate) response_code: ResponseCode,
}

impl Default for OperationResult {
    fn default() -> Self {
        Self {
            prepare_message: None,
            response_remark: None,
            response_code: ResponseCode::Success,
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::message::message_ext::MessageExt;
    use rocketmq_remoting::code::response_code::ResponseCode;

    use super::*;

    #[test]
    fn default_operation_result_has_none_fields() {
        let result = OperationResult::default();
        assert!(result.prepare_message.is_none());
        assert!(result.response_remark.is_none());
        assert_eq!(result.response_code, ResponseCode::Success);
    }

    #[test]
    fn operation_result_with_some_fields() {
        let message = MessageExt::default();
        let remark = Some(String::from("Test remark"));
        let response_code = ResponseCode::SystemError;

        let result = OperationResult {
            prepare_message: Some(message.clone()),
            response_remark: remark.clone(),
            response_code,
        };

        assert_eq!(result.response_remark, remark);
        assert_eq!(result.response_code, response_code);
    }

    #[test]
    fn operation_result_with_none_fields() {
        let result = OperationResult {
            prepare_message: None,
            response_remark: None,
            response_code: ResponseCode::Success,
        };

        assert!(result.prepare_message.is_none());
        assert!(result.response_remark.is_none());
        assert_eq!(result.response_code, ResponseCode::Success);
    }
}
