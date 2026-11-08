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

//! Projection discards unknown field values but marks their records read-only; no JSON replacement.

use rocketmq_dashboard_common::*;
use serde_json::Value;

fn text(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .chars()
        .take(512)
        .collect()
}
fn unknown(value: &Value, keys: &[&str]) -> bool {
    value
        .as_object()
        .is_none_or(|object| object.keys().any(|key| !keys.contains(&key.as_str())))
}
fn canonical(value: String, known: &[&str]) -> String {
    known
        .iter()
        .find(|known| value.eq_ignore_ascii_case(known))
        .map_or(value.clone(), |known| (*known).to_owned())
}
fn string_field(value: &Value, key: &str, max: usize, optional: bool) -> bool {
    match value.get(key) {
        Some(Value::String(value)) => value.len() <= max,
        None | Some(Value::Null) => optional,
        _ => false,
    }
}
fn string_list(value: Option<&Value>, comma_string: bool) -> bool {
    match value {
        None | Some(Value::Null) => true,
        Some(Value::Array(values)) => values.iter().all(Value::is_string),
        Some(Value::String(_)) => comma_string,
        _ => false,
    }
}
fn strings(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::String(value)) => value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect(),
        Some(Value::Array(values)) => values.iter().filter_map(Value::as_str).map(str::to_owned).collect(),
        _ => Vec::new(),
    }
}
pub(super) fn users(target: &BrokerIdentity, values: Vec<Value>) -> Vec<AclUser> {
    values
        .into_iter()
        .take(ACL_ROW_LIMIT)
        .map(|value| {
            let username = text(&value, "username");
            let user_type = value
                .get("userType")
                .and_then(Value::as_str)
                .map(|value| canonical(value.into(), ACL_USER_TYPES));
            let status = value
                .get("userStatus")
                .and_then(Value::as_str)
                .map(|value| canonical(value.into(), ACL_USER_STATUSES));
            let read_only = unknown(&value, &["username", "password", "userType", "userStatus"])
                || !string_field(&value, "username", 255, false)
                || !string_field(&value, "password", 4096, true)
                || username.is_empty()
                || username.len() > 255
                || !user_type
                    .as_deref()
                    .is_some_and(|value| ACL_USER_TYPES.contains(&value))
                || !status
                    .as_deref()
                    .is_some_and(|value| ACL_USER_STATUSES.contains(&value));
            let password = value
                .get("password")
                .and_then(Value::as_str)
                .filter(|value| value.len() <= 4096)
                .map(|value| AclSecret::new(value.into()));
            AclUser {
                target: target.clone(),
                username,
                user_type,
                status,
                password,
                read_only,
            }
        })
        .collect()
}
pub(super) fn policies(target: &BrokerIdentity, values: Vec<Value>) -> Vec<AclPolicy> {
    let mut output = Vec::new();
    for value in values.into_iter().take(ACL_ROW_LIMIT) {
        let subject = text(&value, "subject");
        let root_unknown = unknown(&value, &["subject", "policies"])
            || !string_field(&value, "subject", 255, false)
            || !subject
                .strip_prefix("User:")
                .is_some_and(|name| !name.is_empty() && name.trim() == name && !name.chars().any(char::is_control));
        let Some(policies) = value.get("policies").and_then(Value::as_array) else {
            output.push(AclPolicy {
                target: target.clone(),
                subject,
                policy_type: String::new(),
                entries: Vec::new(),
                read_only: true,
            });
            continue;
        };
        for policy in policies.iter().take(ACL_ROW_LIMIT) {
            let policy_type = canonical(text(policy, "policyType"), ACL_POLICY_TYPES);
            let mut read_only = root_unknown
                || unknown(policy, &["policyType", "entries"])
                || !ACL_POLICY_TYPES.contains(&policy_type.as_str());
            let mut entries = Vec::new();
            if let Some(raw_entries) = policy.get("entries").and_then(Value::as_array) {
                for raw in raw_entries.iter().take(ACL_ROW_LIMIT) {
                    let entry = AclPolicyEntry {
                        resource: text(raw, "resource"),
                        actions: strings(raw.get("actions"))
                            .into_iter()
                            .map(|value| canonical(value, ACL_ACTIONS))
                            .collect(),
                        source_ips: strings(raw.get("sourceIps")),
                        decision: canonical(text(raw, "decision"), ACL_DECISIONS),
                    };
                    read_only |= unknown(raw, &["resource", "actions", "sourceIps", "decision"])
                        || !string_field(raw, "resource", 512, false)
                        || !string_list(raw.get("actions"), true)
                        || !string_list(raw.get("sourceIps"), false)
                        || entry.validate().is_err();
                    entries.push(entry);
                }
                read_only |= raw_entries.len() > ACL_ROW_LIMIT;
            } else {
                read_only = true;
            }
            read_only |= entries.is_empty();
            output.push(AclPolicy {
                target: target.clone(),
                subject: subject.clone(),
                policy_type,
                entries,
                read_only,
            });
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn acl_decoder_keeps_only_backend_secrets_and_blocks_unknown_policy_fields_and_enums() {
        let target = BrokerIdentity {
            cluster: "c".into(),
            broker_name: "b".into(),
            broker_id: 0,
            address: "b:1".into(),
        };
        let user = users(&target, vec![serde_json::json!({"username":"a", "password":"backend-secret", "userType":"Normal", "userStatus":"enable"})]).remove(0);
        assert_eq!(user.password.as_ref().unwrap().expose(), "backend-secret");
        assert!(!format!("{user:?}").contains("backend-secret"));
        assert!(
            users(
                &target,
                vec![serde_json::json!({"username":"a", "userType":"Future", "userStatus":"enable"})]
            )[0]
            .read_only
        );
        let documents = vec![
            serde_json::json!({"subject":"User:a", "policies":[{"policyType":"Custom", "entries":[{"resource":"Topic:t", "actions":["Pub"], "sourceIps":[], "decision":"Allow", "futureField":{"private":"unknown"}}]}]}),
        ];
        let policies = policies(&target, documents);
        assert!(policies[0].read_only);
        assert!(!format!("{policies:?}").contains("private"));
        for action in ["Any", "FutureAction"] {
            let documents = vec![
                serde_json::json!({"subject":"User:a", "policies":[{"policyType":"Custom", "entries":[{"resource":"Topic:t", "actions":[action], "sourceIps":[], "decision":"Allow"}]}]}),
            ];
            assert!(super::policies(&target, documents)[0].read_only);
        }
    }
}
