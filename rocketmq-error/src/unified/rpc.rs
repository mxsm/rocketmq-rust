//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

//! RPC client errors with full context preservation

use thiserror::Error;

/// RPC client specific errors with full context preservation
#[derive(Error, Debug)]
pub enum RpcClientError {
    /// Broker address not found in client metadata
    #[error("Broker '{broker_name}' address not found in client metadata")]
    BrokerNotFound { broker_name: String },

    /// RPC request failed
    #[error(
        "RPC request failed: addr={addr}, request_code={request_code}, timeout={timeout_ms}ms"
    )]
    RequestFailed {
        addr: String,
        request_code: i32,
        timeout_ms: u64,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Unexpected response code received
    #[error("Unexpected response code: {code} ({code_name})")]
    UnexpectedResponseCode { code: i32, code_name: String },

    /// Request code not supported by the handler
    #[error("Request code not supported: {code}")]
    UnsupportedRequestCode { code: i32 },

    /// RPC error from remote server
    #[error("RPC error from remote: code={0}, message={1}")]
    RemoteError(i32, String),
}
