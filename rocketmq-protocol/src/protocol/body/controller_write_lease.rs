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

use serde::Deserialize;
use serde::Serialize;

/// Rust-native Controller response that authorizes one Broker master to write.
///
/// This body is intentionally not part of the Java Controller wire contract.
/// It is returned only after the corresponding heartbeat has been committed by
/// the Rust Controller quorum.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControllerWriteLeaseGrant {
    pub broker_id: i64,
    pub master_epoch: i32,
    pub generation: u64,
    pub lease_duration_millis: u64,
    pub safety_margin_millis: u64,
}

impl ControllerWriteLeaseGrant {
    pub const DEFAULT_LEASE_DURATION_MILLIS: u64 = 10_000;
    pub const DEFAULT_SAFETY_MARGIN_MILLIS: u64 = 2_000;
}

#[cfg(test)]
mod tests {
    use super::ControllerWriteLeaseGrant;

    #[test]
    fn grant_uses_stable_camel_case_wire_fields() {
        let grant = ControllerWriteLeaseGrant {
            broker_id: 0,
            master_epoch: 3,
            generation: 7,
            lease_duration_millis: 10_000,
            safety_margin_millis: 2_000,
        };

        let json = serde_json::to_string(&grant).expect("grant should serialize");
        assert!(json.contains("\"brokerId\":0"));
        assert!(json.contains("\"masterEpoch\":3"));
        assert_eq!(serde_json::from_str::<ControllerWriteLeaseGrant>(&json).unwrap(), grant);
    }
}
