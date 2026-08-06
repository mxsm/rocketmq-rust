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

use rocketmq_sre_contracts::AutonomyMode;

use super::model::AutonomyTransitionRequest;

#[test]
fn autonomous_transition_accepts_only_a_bounded_opaque_approval_reference() {
    let request = AutonomyTransitionRequest {
        target_mode: AutonomyMode::Autonomous,
        reason: Some("target qualification and owner review completed".to_owned()),
        owner_confirmed: true,
        owner_approval_ref: Some("approval://target/autonomy/logger-ttl".to_owned()),
    };

    assert_eq!(
        request.validated_owner_approval_ref(),
        Some("approval://target/autonomy/logger-ttl")
    );
}

#[test]
fn approval_reference_rejects_missing_url_personal_and_unbounded_values() {
    for approval_ref in [
        None,
        Some("https://approvals.example/123"),
        Some("operator@example.com"),
        Some("approval://target/../escape"),
        Some("approval://TARGET/autonomy"),
        Some("approval://target/token=value"),
    ] {
        let request = AutonomyTransitionRequest {
            target_mode: AutonomyMode::Autonomous,
            reason: None,
            owner_confirmed: true,
            owner_approval_ref: approval_ref.map(str::to_owned),
        };

        assert!(request.validated_owner_approval_ref().is_none());
    }

    let request = AutonomyTransitionRequest {
        target_mode: AutonomyMode::Autonomous,
        reason: None,
        owner_confirmed: true,
        owner_approval_ref: Some(format!("approval://target/{}", "a".repeat(160))),
    };
    assert!(request.validated_owner_approval_ref().is_none());
}

#[test]
fn transition_json_keeps_non_autonomous_callers_compatible() {
    let request: AutonomyTransitionRequest = serde_json::from_value(serde_json::json!({
        "target_mode": "supervised",
        "reason": "qualified shadow cohort",
        "owner_confirmed": true
    }))
    .expect("legacy non-Autonomous transition request");

    assert!(request.owner_approval_ref.is_none());
}
