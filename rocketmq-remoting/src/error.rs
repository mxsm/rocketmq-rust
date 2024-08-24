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
pub enum Error {
    #[error("{0}")]
    RemotingCommandException(String),

    #[error("{0}")]
    FromStrError(String),

    #[error("{0:?}")]
    Io(#[from] io::Error),

    #[error("{0}")]
    RemoteException(String),

    #[error("RpcException: code: {0}, message: {1}")]
    RpcException(i32, String),

    #[error("RocketMQ protocol decoding failed, extFields length: {0}, but header length: {1}")]
    DecodingError(usize, usize),

    #[error("UTF-8 decoding error")]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error("RemotingCommandDecoderError:{0}")]
    RemotingCommandDecoderError(String),

    #[error("RemotingCommandEncoderError:{0}")]
    RemotingCommandEncoderError(String),

    #[error("Not support serialize type: {0}")]
    NotSupportSerializeType(u8),

    #[error("{0}")]
    ConnectionInvalid(String),

    /*    #[error("{0}")]
    Elapsed(#[from] tokio::time::error::Elapsed),*/
    #[error("{0}-{1}")]
    AbortProcessException(i32, String),
}

#[cfg(test)]
mod error_tests {
    use std::io;

    use super::*;

    #[test]
    fn remoting_command_exception_contains_correct_message() {
        let error = Error::RemotingCommandException("Error message".into());
        assert_eq!(error.to_string(), "Error message");
    }

    #[test]
    fn from_str_error_contains_correct_message() {
        let error = Error::FromStrError("Parse error".into());
        assert_eq!(error.to_string(), "Parse error");
    }

    #[test]
    fn io_error_is_correctly_mapped() {
        let io_error = io::Error::new(io::ErrorKind::Other, "IO error");
        let error: Error = io_error.into();
        assert!(matches!(error, Error::Io(_)));
        assert!(error.to_string().contains("IO error"));
    }

    #[test]
    fn remote_exception_contains_correct_message() {
        let error = Error::RemoteException("Remote error".into());
        assert_eq!(error.to_string(), "Remote error");
    }

    #[test]
    fn rpc_exception_contains_correct_code_and_message() {
        let error = Error::RpcException(404, "Not found".into());
        assert_eq!(
            error.to_string(),
            "RpcException: code: 404, message: Not found"
        );
    }

    #[test]
    fn decoding_error_contains_correct_lengths() {
        let error = Error::DecodingError(10, 20);
        assert_eq!(
            error.to_string(),
            "RocketMQ protocol decoding failed, extFields length: 10, but header length: 20"
        );
    }

    #[test]
    #[allow(invalid_from_utf8)]
    fn utf8_error_is_correctly_mapped() {
        let utf8_error = std::str::from_utf8(&[0, 159, 146, 150]).unwrap_err();
        let error: Error = utf8_error.into();
        assert!(matches!(error, Error::Utf8Error(_)));
        assert!(error.to_string().contains("UTF-8 decoding error"));
    }

    #[test]
    fn remoting_command_decoder_error_contains_correct_message() {
        let error = Error::RemotingCommandDecoderError("Decoder error".into());
        assert_eq!(
            error.to_string(),
            "RemotingCommandDecoderError:Decoder error"
        );
    }

    #[test]
    fn remoting_command_encoder_error_contains_correct_message() {
        let error = Error::RemotingCommandEncoderError("Encoder error".into());
        assert_eq!(
            error.to_string(),
            "RemotingCommandEncoderError:Encoder error"
        );
    }
}
