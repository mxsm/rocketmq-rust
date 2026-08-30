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

const RELEASE_REASONS: [&str; 10] = [
    "ScalingReplicaSet",
    "SuccessfulCreate",
    "SuccessfulDelete",
    "SuccessfulRescale",
    "SuccessfulUpdate",
    "ProgressDeadlineExceeded",
    "FailedCreate",
    "FailedDelete",
    "FailedAttachVolume",
    "FailedMount",
];

pub(super) fn project_change(item: &Value) -> Option<Value> {
    let reason = item.get("reason").and_then(Value::as_str)?;
    RELEASE_REASONS.contains(&reason).then(|| {
        json!({
            "schema_version": "rocketmq.change-event.v1",
            "reason": reason,
            "type": item.get("type"),
            "action": item.get("action"),
            "object_kind": item
                .pointer("/regarding/kind")
                .or_else(|| item.pointer("/involvedObject/kind")),
            "object_name": item
                .pointer("/regarding/name")
                .or_else(|| item.pointer("/involvedObject/name")),
            "observed_at": item
                .pointer("/series/lastObservedTime")
                .or_else(|| item.get("eventTime"))
                .or_else(|| item.get("lastTimestamp"))
                .or_else(|| item.pointer("/metadata/creationTimestamp"))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeline_keeps_only_known_release_and_failure_events() {
        assert!(
            project_change(&json!({
                "reason": "SuccessfulUpdate",
                "involvedObject": {"kind": "StatefulSet", "name": "broker"},
                "message": "image private.example/secret"
            }))
            .is_some()
        );
        assert!(project_change(&json!({"reason": "ArbitraryMessage"})).is_none());
    }
}
