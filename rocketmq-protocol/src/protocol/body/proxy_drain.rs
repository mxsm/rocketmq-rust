// Copyright 2026 The RocketMQ Rust Authors
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

use serde::Deserialize;
use serde::Serialize;

pub const PROXY_DRAIN_SCHEMA_VERSION: &str = "rocketmq.proxy-drain.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProxyDrainOperationRequestBody {
    pub schema_version: String,
    pub operation_id: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyDrainPendingBody {
    pub active_connections: usize,
    pub sessions: usize,
    pub receipt_handles: usize,
    pub prepared_transactions: usize,
    pub telemetry_links: usize,
    pub remoting_channels: usize,
    pub telemetry_commands: usize,
    pub rpc_in_flight: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyDrainStateResponseBody {
    pub schema_version: String,
    pub phase: String,
    pub operation_id: Option<String>,
    pub admission_open: bool,
    pub routing_open: bool,
    pub readiness_published: bool,
    pub zero_pending: bool,
    pub pending: ProxyDrainPendingBody,
}

#[cfg(test)]
mod tests {
    use crate::protocol::RemotingDeserializable;
    use crate::protocol::RemotingSerializable;

    use super::*;

    #[test]
    fn operation_request_rejects_unknown_fields() {
        let invalid = br#"{
            "schemaVersion":"rocketmq.proxy-drain.v1",
            "operationId":"restart-1",
            "unexpected":true
        }"#;
        assert!(ProxyDrainOperationRequestBody::decode(invalid).is_err());
    }

    #[test]
    fn state_response_round_trips_with_all_pending_dimensions() {
        let expected = ProxyDrainStateResponseBody {
            schema_version: PROXY_DRAIN_SCHEMA_VERSION.to_owned(),
            phase: "draining".to_owned(),
            operation_id: Some("restart-1".to_owned()),
            admission_open: false,
            routing_open: false,
            readiness_published: false,
            zero_pending: false,
            pending: ProxyDrainPendingBody {
                active_connections: 2,
                sessions: 1,
                receipt_handles: 3,
                prepared_transactions: 4,
                telemetry_links: 1,
                remoting_channels: 1,
                telemetry_commands: 5,
                rpc_in_flight: 6,
            },
        };
        let encoded = expected.encode().unwrap();
        let decoded = ProxyDrainStateResponseBody::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded, expected);
    }
}
