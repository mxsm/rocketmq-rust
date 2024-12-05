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
pub enum NamesrvError {
    #[error("Namesrv error: {0}")]
    NamesrvRemotingError(#[from] rocketmq_remoting::remoting_error::RemotingError),

    #[error("Common error: {0}")]
    NamesrvCommonError(#[from] rocketmq_common::error::Error),

    #[error("{0}")]
    MQNamesrvError(String),
}

impl From<NamesrvError> for rocketmq_remoting::remoting_error::RemotingError {
    #[inline]
    fn from(value: NamesrvError) -> Self {
        match value {
            NamesrvError::NamesrvRemotingError(e) => e,
            NamesrvError::NamesrvCommonError(e) => {
                rocketmq_remoting::remoting_error::RemotingError::RemoteError(format!("{}", e))
            }
            NamesrvError::MQNamesrvError(e) => {
                rocketmq_remoting::remoting_error::RemotingError::RemoteError(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::error::Error;
    use rocketmq_remoting::remoting_error::RemotingError;

    use super::*;

    #[test]
    fn namesrv_remoting_error_conversion() {
        let remoting_error = RemotingError::RemoteError("Remoting error".to_string());
        let remoting_error1 = RemotingError::RemoteError("Remoting error".to_string());
        let namesrv_error = NamesrvError::NamesrvRemotingError(remoting_error);
        let converted_error: RemotingError = namesrv_error.into();
        assert_eq!(converted_error.to_string(), remoting_error1.to_string());
    }

    #[test]
    fn namesrv_common_error_conversion() {
        let common_error = Error::UnsupportedOperationException("Common error".to_string());
        let common_error1 = Error::UnsupportedOperationException("Common error".to_string());
        let namesrv_error = NamesrvError::NamesrvCommonError(common_error);
        let converted_error: RemotingError = namesrv_error.into();
        assert_eq!(converted_error.to_string(), format!("{}", common_error1));
    }

    #[test]
    fn mq_namesrv_error_conversion() {
        let mq_error = "MQ Namesrv error".to_string();
        let namesrv_error = NamesrvError::MQNamesrvError(mq_error.clone());
        let converted_error: RemotingError = namesrv_error.into();
        assert_eq!(converted_error.to_string(), format!("{}", mq_error));
    }

    #[test]
    fn namesrv_error_debug_format() {
        let remoting_error = RemotingError::RemoteError("Remoting error".to_string());
        let namesrv_error = NamesrvError::NamesrvRemotingError(remoting_error);
        assert_eq!(
            format!("{:?}", namesrv_error),
            "NamesrvRemotingError(RemoteError(\"Remoting error\"))"
        );
    }
}
