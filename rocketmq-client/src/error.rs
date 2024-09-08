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
use rocketmq_remoting::error::Error as RemotingError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MQClientError {
    #[error("Client exception occurred: CODE:{0}, Message:{1}")]
    MQClientErr(i32, String),

    #[error("{0}")]
    RemotingTooMuchRequestError(String),

    #[error("Client exception occurred: CODE:{0}, broker address:{1}, Message:{2}")]
    MQBrokerError(i32, String, String),

    #[error("Client exception occurred: CODE:{0}, Message:{1}")]
    RequestTimeoutError(i32, String),

    #[error("Client exception occurred: CODE:{0}, broker address:{1}, Message:{2}")]
    OffsetNotFoundError(i32, String, String),

    #[error("{0}")]
    RemotingError(#[from] RemotingError),

    #[error("{0}")]
    IllegalArgumentError(String),
}
