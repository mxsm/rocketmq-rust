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

use rocketmq_error::ProtocolError;
use rocketmq_error::RocketMQError;

use crate::protocol::remoting_command::RemotingCommand;

/// Convert a typed RocketMQ error into a remoting response command.
pub fn command_from_error(error: &RocketMQError) -> RemotingCommand {
    command_from_error_with_remark(error, error.to_string())
}

/// Convert a typed RocketMQ error into a remoting response command and preserve
/// the request opaque.
pub fn command_from_error_with_opaque(error: &RocketMQError, opaque: i32) -> RemotingCommand {
    command_from_error(error).set_opaque(opaque)
}

/// Convert a typed RocketMQ error into a remoting response command with an
/// explicit wire remark.
pub fn command_from_error_with_remark(error: &RocketMQError, remark: impl Into<String>) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code_remark(error.spec().remoting.code.as_i32(), remark.into())
}

/// Convert a typed RocketMQ error into a remoting response command with an
/// explicit wire remark and request opaque.
pub fn command_from_error_with_remark_and_opaque(
    error: &RocketMQError,
    remark: impl Into<String>,
    opaque: i32,
) -> RemotingCommand {
    command_from_error_with_remark(error, remark).set_opaque(opaque)
}

/// Build the standard unsupported-request-code response from the central
/// remoting boundary mapping.
pub fn request_code_not_supported(request_code: i32) -> RemotingCommand {
    request_code_not_supported_with_remark(
        request_code,
        format!("The request code {request_code} is not supported."),
    )
}

/// Build an unsupported-request-code response with a caller-specific remark.
pub fn request_code_not_supported_with_remark(request_code: i32, remark: impl Into<String>) -> RemotingCommand {
    let error = RocketMQError::Protocol(ProtocolError::invalid_command(request_code));
    command_from_error_with_remark(&error, remark)
}

/// Build the standard unsupported-request-code response and preserve request
/// opaque.
pub fn request_code_not_supported_with_opaque(request_code: i32, opaque: i32) -> RemotingCommand {
    request_code_not_supported(request_code).set_opaque(opaque)
}

/// Build an unsupported-request-code response with a caller-specific remark and
/// request opaque.
pub fn request_code_not_supported_with_remark_and_opaque(
    request_code: i32,
    remark: impl Into<String>,
    opaque: i32,
) -> RemotingCommand {
    request_code_not_supported_with_remark(request_code, remark).set_opaque(opaque)
}

/// Build a generic internal-error response from the central remoting boundary
/// mapping.
pub fn internal_error_with_opaque(opaque: i32, remark: impl Into<String>) -> RemotingCommand {
    let remark = remark.into();
    let error = RocketMQError::Internal(remark.clone());
    command_from_error_with_remark_and_opaque(&error, remark, opaque)
}

#[cfg(test)]
mod tests {
    use rocketmq_error::RemotingResponseCode;

    use super::*;
    use crate::code::response_code::ResponseCode;

    #[test]
    fn command_from_error_uses_central_remoting_spec() {
        let response = command_from_error(&RocketMQError::response_process_failed("decode", "bad header"));

        assert_eq!(
            ResponseCode::from(response.code()),
            ResponseCode::from(RemotingResponseCode::InvalidParameter.as_i32())
        );
        assert!(response
            .remark()
            .expect("remark should be set")
            .contains("Response decode failed"));
    }

    #[test]
    fn request_code_not_supported_uses_protocol_spec_mapping() {
        let response = request_code_not_supported_with_opaque(999, 7);

        assert_eq!(
            ResponseCode::from(response.code()),
            ResponseCode::RequestCodeNotSupported
        );
        assert_eq!(response.opaque(), 7);
        assert_eq!(
            response.remark().map(|remark| remark.as_str()),
            Some("The request code 999 is not supported.")
        );
    }

    #[test]
    fn internal_error_uses_system_error_mapping() {
        let response = internal_error_with_opaque(9, "worker failed");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
        assert_eq!(response.opaque(), 9);
        assert_eq!(response.remark().map(|remark| remark.as_str()), Some("worker failed"));
    }
}
