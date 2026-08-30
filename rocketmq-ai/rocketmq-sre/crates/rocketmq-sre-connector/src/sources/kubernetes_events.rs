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

use serde_json::Value;
use serde_json::json;

pub(super) fn project_event(item: &Value) -> Value {
    json!({
        "schema_version": "rocketmq.kubernetes-event.v1",
        "name": item.pointer("/metadata/name"),
        "object_kind": item
            .pointer("/regarding/kind")
            .or_else(|| item.pointer("/involvedObject/kind")),
        "object_name": item
            .pointer("/regarding/name")
            .or_else(|| item.pointer("/involvedObject/name")),
        "reason": item.get("reason"),
        "type": item.get("type"),
        "action": item.get("action"),
        "count": item
            .pointer("/series/count")
            .or_else(|| item.get("count")),
        "first_timestamp": item
            .get("eventTime")
            .or_else(|| item.get("firstTimestamp")),
        "last_timestamp": item
            .pointer("/series/lastObservedTime")
            .or_else(|| item.get("lastTimestamp"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_projection_excludes_message_addresses_and_reporting_identity() {
        let event = json!({
            "metadata": {"name": "broker-rollout"},
            "regarding": {"kind": "Deployment", "name": "broker"},
            "reason": "ScalingReplicaSet",
            "type": "Normal",
            "message": "token=secret at 10.0.0.2",
            "reportingInstance": "node.internal.example",
            "series": {"count": 3, "lastObservedTime": "2026-07-27T00:00:00Z"}
        });
        let projected = project_event(&event);
        let encoded = serde_json::to_string(&projected).expect("projection");
        assert!(encoded.contains("ScalingReplicaSet"));
        assert!(!encoded.contains("token"));
        assert!(!encoded.contains("10.0.0.2"));
        assert!(!encoded.contains("node.internal"));
    }
}
