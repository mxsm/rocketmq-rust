/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RemotingError {
    #[error("{0}")]
    RemoteError(String),

    #[error("{0}")]
    RemotingCommandError(String),

    #[error("connect to {0} failed")]
    RemotingConnectError(String),

    #[error("send request to < {0} > failed")]
    RemotingSendRequestError(String),

    #[error("wait response on the channel < {0}  >, timeout: {1}(ms)")]
    RemotingTimeoutError(String, u64),

    #[error("RemotingTooMuchRequestException: {0}")]
    RemotingTooMuchRequestError(String),

    #[error("RpcException: code: {0}, message: {1}")]
    RpcError(i32, String),

    #[error("{0}")]
    FromStrErr(String),

    #[error("{0:?}")]
    Io(#[from] io::Error),

    #[error("RocketMQ protocol decoding failed, extFields length: {0}, but header length: {1}")]
    DecodingError(usize, usize),

    #[error("UTF-8 decoding error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error("RemotingCommandDecoderError:{0}")]
    RemotingCommandDecoderError(String),

    #[error("RemotingCommandEncoderError:{0}")]
    RemotingCommandEncoderError(String),

    #[error("Not support serialize type: {0}")]
    NotSupportSerializeType(u8),

    #[error("ConnectionInvalid: {0}")]
    ConnectionInvalid(String),

    #[error("AbortProcessError: {0}-{1}")]
    AbortProcessError(i32, String),

    #[error("Channel Send Request failed: {0}")]
    ChannelSendRequestFailed(String),

    #[error("Channel recv Request failed: {0}")]
    ChannelRecvRequestFailed(String),

    #[error("CommonError: {0}")]
    CommonError(#[from] rocketmq_common::error::Error),
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn remote_error_displays_correctly() {
        let error = RemotingError::RemoteError("remote error".to_string());
        assert_eq!(format!("{}", error), "remote error");
    }

    #[test]
    fn remoting_command_error_displays_correctly() {
        let error = RemotingError::RemotingCommandError("command error".to_string());
        assert_eq!(format!("{}", error), "command error");
    }

    #[test]
    fn remoting_connect_error_displays_correctly() {
        let error = RemotingError::RemotingConnectError("localhost:8080".to_string());
        assert_eq!(format!("{}", error), "connect to localhost:8080 failed");
    }

    #[test]
    fn remoting_send_request_error_displays_correctly() {
        let error = RemotingError::RemotingSendRequestError("localhost:8080".to_string());
        assert_eq!(
            format!("{}", error),
            "send request to < localhost:8080 > failed"
        );
    }

    #[test]
    fn remoting_timeout_error_displays_correctly() {
        let error = RemotingError::RemotingTimeoutError("localhost:8080".to_string(), 5000);
        assert_eq!(
            format!("{}", error),
            "wait response on the channel < localhost:8080  >, timeout: 5000(ms)"
        );
    }

    #[test]
    fn remoting_too_much_request_error_displays_correctly() {
        let error = RemotingError::RemotingTooMuchRequestError("too many requests".to_string());
        assert_eq!(
            format!("{}", error),
            "RemotingTooMuchRequestException: too many requests"
        );
    }

    #[test]
    fn rpc_error_displays_correctly() {
        let error = RemotingError::RpcError(404, "not found".to_string());
        assert_eq!(
            format!("{}", error),
            "RpcException: code: 404, message: not found"
        );
    }

    #[test]
    fn from_str_err_displays_correctly() {
        let error = RemotingError::FromStrErr("parse error".to_string());
        assert_eq!(format!("{}", error), "parse error");
    }

    #[test]
    fn io_error_displays_correctly() {
        let error = RemotingError::Io(io::Error::new(io::ErrorKind::Other, "io error"));
        assert_eq!(
            format!("{}", error),
            "Custom { kind: Other, error: \"io error\" }"
        );
    }

    #[test]
    fn decoding_error_displays_correctly() {
        let error = RemotingError::DecodingError(10, 5);
        assert_eq!(
            format!("{}", error),
            "RocketMQ protocol decoding failed, extFields length: 10, but header length: 5"
        );
    }

    #[test]
    fn remoting_command_decoder_error_displays_correctly() {
        let error = RemotingError::RemotingCommandDecoderError("decoder error".to_string());
        assert_eq!(
            format!("{}", error),
            "RemotingCommandDecoderError:decoder error"
        );
    }

    #[test]
    fn remoting_command_encoder_error_displays_correctly() {
        let error = RemotingError::RemotingCommandEncoderError("encoder error".to_string());
        assert_eq!(
            format!("{}", error),
            "RemotingCommandEncoderError:encoder error"
        );
    }

    #[test]
    fn not_support_serialize_type_displays_correctly() {
        let error = RemotingError::NotSupportSerializeType(1);
        assert_eq!(format!("{}", error), "Not support serialize type: 1");
    }

    #[test]
    fn connection_invalid_displays_correctly() {
        let error = RemotingError::ConnectionInvalid("invalid connection".to_string());
        assert_eq!(
            format!("{}", error),
            "ConnectionInvalid: invalid connection"
        );
    }

    #[test]
    fn abort_process_error_displays_correctly() {
        let error = RemotingError::AbortProcessError(1, "abort".to_string());
        assert_eq!(format!("{}", error), "AbortProcessError: 1-abort");
    }

    #[test]
    fn channel_send_request_failed_displays_correctly() {
        let error = RemotingError::ChannelSendRequestFailed("send failed".to_string());
        assert_eq!(
            format!("{}", error),
            "Channel Send Request failed: send failed"
        );
    }

    #[test]
    fn channel_recv_request_failed_displays_correctly() {
        let error = RemotingError::ChannelRecvRequestFailed("recv failed".to_string());
        assert_eq!(
            format!("{}", error),
            "Channel recv Request failed: recv failed"
        );
    }
}
