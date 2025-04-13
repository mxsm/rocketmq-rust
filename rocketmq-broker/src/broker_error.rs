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
use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("broker client error: {0}")]
    BrokerRemotingError(#[from] rocketmq_remoting::remoting_error::RemotingError),

    #[error("Common error: {0}")]
    BrokerCommonError(#[from] rocketmq_common::error::Error),

    #[error("Client exception occurred: CODE:{0}, broker address:{2}, Message:{1}")]
    MQBrokerError(i32, String, String),

    #[error("{0}")]
    IllegalArgumentError(String),

    #[error("Client error: {0}")]
    ClientError(#[from] rocketmq_client_rust::client_error::MQClientError),
}

impl From<BrokerError> for rocketmq_remoting::remoting_error::RemotingError {
    fn from(value: BrokerError) -> Self {
        match value {
            BrokerError::BrokerRemotingError(e) => e,
            BrokerError::BrokerCommonError(e) => {
                rocketmq_remoting::remoting_error::RemotingError::RemoteError(format!("{}", e))
            }
            BrokerError::MQBrokerError(code, message, _) => {
                rocketmq_remoting::remoting_error::RemotingError::RemoteError(format!(
                    "CODE:{}, Message:{}",
                    code, message
                ))
            }
            BrokerError::IllegalArgumentError(e) => {
                rocketmq_remoting::remoting_error::RemotingError::RemoteError(e)
            }
            BrokerError::ClientError(e) => {
                rocketmq_remoting::remoting_error::RocketmqError::DeserializeHeaderError(format!(
                    "{}",
                    e
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_remoting::remoting_error::RemotingError;

    use super::*;

    #[test]
    fn broker_remoting_error_displays_correctly() {
        let error = BrokerError::BrokerRemotingError(RemotingError::RemoteError(
            "remote error".to_string(),
        ));
        assert_eq!(format!("{}", error), "broker client error: remote error");
    }

    #[test]
    fn mq_broker_error_displays_correctly() {
        let error =
            BrokerError::MQBrokerError(404, "not found".to_string(), "127.0.0.1".to_string());
        assert_eq!(
            format!("{}", error),
            "Client exception occurred: CODE:404, broker address:127.0.0.1, Message:not found"
        );
    }

    #[test]
    fn illegal_argument_error_displays_correctly() {
        let error = BrokerError::IllegalArgumentError("illegal argument".to_string());
        assert_eq!(format!("{}", error), "illegal argument");
    }
}
