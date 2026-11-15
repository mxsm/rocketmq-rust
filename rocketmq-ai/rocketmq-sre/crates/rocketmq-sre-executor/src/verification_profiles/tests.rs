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

use serde_json::Value;
use serde_json::json;

use super::*;

#[test]
fn exact_selected_profile_is_private_and_unrelated_rules_do_not_change_its_digest() {
    let rule = rule();
    let first = parse(vec![rule.clone()]).unwrap();
    let tenant: TenantId = serde_json::from_value(rule["tenant_id"].clone()).unwrap();
    let cluster: ClusterId = serde_json::from_value(rule["cluster_id"].clone()).unwrap();
    let spec = spec();
    let selected = first
        .resolve(
            tenant,
            cluster,
            ExecutionAction::ObservabilityLoggerLevelTtl,
            "1.0.0",
            "broker/private-fixture",
            &spec,
        )
        .unwrap()
        .unwrap();
    let mut unrelated = rule.clone();
    unrelated["target"] = json!("broker/other-fixture");
    let reordered = parse(vec![unrelated, rule]).unwrap();
    assert_eq!(
        reordered
            .resolve(
                tenant,
                cluster,
                ExecutionAction::ObservabilityLoggerLevelTtl,
                "1.0.0",
                "broker/private-fixture",
                &spec
            )
            .unwrap(),
        Some(selected)
    );
    assert!(!format!("{first:?}").contains("private-fixture"));
    assert!(!format!("{first:?}").contains("PrivateVersion"));
    assert!(
        first
            .resolve(
                TenantId::new(),
                cluster,
                ExecutionAction::ObservabilityLoggerLevelTtl,
                "1.0.0",
                "broker/private-fixture",
                &spec
            )
            .unwrap()
            .is_none()
    );
    assert!(
        first
            .resolve(
                tenant,
                cluster,
                ExecutionAction::ObservabilityLoggerLevelTtl,
                "1.0.0",
                "broker/foreign-fixture",
                &spec
            )
            .unwrap()
            .is_none()
    );
}

#[test]
fn ambiguous_versions_wildcards_unknown_fields_and_limits_are_rejected() {
    let base = rule();
    let mut another_version = base.clone();
    another_version["declared_target_version"] = json!("PrivateVersion-2");
    assert!(parse(vec![base.clone(), another_version]).is_err());
    for (field, value) in [
        ("target", json!("broker/*")),
        ("descriptor_version", json!("0.9.0")),
        ("declared_target_version", json!("")),
        ("max_source_age_seconds", json!(u64::MAX)),
        ("max_sample_gap_seconds", json!(0)),
        ("private_unknown_field", json!("must-not-appear-in-errors")),
    ] {
        let mut invalid = base.clone();
        invalid[field] = value;
        let error = parse(vec![invalid]).unwrap_err();
        assert!(!error.to_string().contains("must-not-appear-in-errors"));
    }
    let duplicate_property = br#"{"schema_version":"rocketmq-sre.execution-verification-profiles.v1","schema_version":"rocketmq-sre.execution-verification-profiles.v1","rules":[]}"#;
    assert!(VerificationProfiles::from_json(duplicate_property).is_err());
    assert!(VerificationProfiles::from_json(&vec![b' '; MAX_BYTES + 1]).is_err());
}

fn parse(rules: Vec<Value>) -> Result<VerificationProfiles, ExecutorError> {
    VerificationProfiles::from_json(&serde_json::to_vec(&json!({"schema_version":SCHEMA,"rules":rules})).unwrap())
}

fn rule() -> Value {
    json!({
        "tenant_id": TenantId::new(), "cluster_id": ClusterId::new(),
        "action":"observability.logger_level_ttl.v1", "descriptor_version":"1.0.0",
        "target":"broker/private-fixture", "declared_target_version":"PrivateVersion-1",
        "policy_revision":"1", "max_source_age_seconds":30, "max_sample_gap_seconds":10
    })
}

fn spec() -> VerificationSpec {
    VerificationSpec {
        resource_conditions: vec!["logger_level_applied".to_owned(), "ttl_restore_scheduled".to_owned()],
        technical_slis: vec!["runtime_error_ratio".to_owned()],
        stable_window_seconds: 30,
        max_wait_seconds: 120,
    }
}
