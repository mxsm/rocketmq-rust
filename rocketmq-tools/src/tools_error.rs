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
pub enum ToolsError {
    #[error("MQ client error occurred. {0}")]
    MQClientError(#[from] rocketmq_client_rust::client_error::MQClientError),
    #[error("MQ broker error occurred.")]
    MQBrokerError,
    #[error("Remoting timeout.")]
    RemotingTimeoutError,
    #[error("Remoting send request failed.")]
    RemotingSendRequestError,
    #[error("Remoting connect failed.")]
    RemotingConnectError,
    #[error("Unsupported encoding.")]
    UnsupportedEncodingError,
    #[error("Operation interrupted.")]
    InterruptedError,
}
