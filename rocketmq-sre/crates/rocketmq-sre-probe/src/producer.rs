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

//! Body-safe producer request types shared by real and fixture drivers.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Send behavior selected by a bounded scenario.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeSendMode {
    Standard,
    ProxyPath,
    TransactionCommit,
    DelayedTimer,
    PopSeed,
}

/// Synthetic message batch without any business payload field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeMessageBatch {
    pub count: u16,
    pub payload_bytes: u32,
    pub minimum_interval_millis: u64,
    pub tag: &'static str,
    pub key_prefix: String,
}

/// Bounded producer observation.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProbeSendObservation {
    pub accepted_messages: u16,
}
