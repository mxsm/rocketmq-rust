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
    RemotingCommandException(String),
    #[error("{0}")]
    FromStrError(String),
    #[error("{0:?}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    RemoteException(String),
}

#[derive(Debug, Error)]
#[error("RpcException: code: {0}, message: {1}")]
pub struct RpcException(pub i32, pub String);

#[derive(Debug, Error)]
#[error("{0}")]
pub struct RemotingCommandDecoderError(pub String);
impl From<io::Error> for RemotingCommandDecoderError {
    fn from(err: io::Error) -> Self {
        RemotingCommandDecoderError(err.to_string())
    }
}
#[derive(Debug, Error)]
#[error("{0}")]
pub struct RemotingCommandEncoderError(pub String);
impl From<io::Error> for RemotingCommandEncoderError {
    fn from(err: io::Error) -> Self {
        RemotingCommandEncoderError(err.to_string())
    }
}

#[derive(Debug, Error)]
pub enum RemotingCommandError {
    #[error("RocketMQ protocol decoding failed, extFields length: {0}, but header length: {1}")]
    DecodingError(usize, usize),
    #[error("UTF-8 decoding error")]
    Utf8Error(#[from] std::str::Utf8Error),
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn remoting_command_decoder_error_from_io_error() {
        let io_error = io::Error::new(io::ErrorKind::Other, "test error");
        let decoder_error = RemotingCommandDecoderError::from(io_error);
        assert_eq!(decoder_error.0, "test error");
    }

    #[test]
    fn remoting_command_encoder_error_from_io_error() {
        let io_error = io::Error::new(io::ErrorKind::Other, "test error");
        let encoder_error = RemotingCommandEncoderError::from(io_error);
        assert_eq!(encoder_error.0, "test error");
    }

    #[test]
    fn remoting_error_from_io_error() {
        let io_error = io::Error::new(io::ErrorKind::Other, "test error");
        let remoting_error = RemotingError::from(io_error);
        match remoting_error {
            RemotingError::Io(error) => assert_eq!(error.to_string(), "test error"),
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn remoting_error_from_string() {
        let remoting_error = RemotingError::RemotingCommandException("test error".to_string());
        match remoting_error {
            RemotingError::RemotingCommandException(error) => assert_eq!(error, "test error"),
            _ => panic!("Expected RemotingCommandException variant"),
        }
    }
}
