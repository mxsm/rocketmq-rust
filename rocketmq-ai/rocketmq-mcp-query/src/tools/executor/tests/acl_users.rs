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

//! `rocketmq_list_acl_users`: the ACL users of a cluster, for a caller that administers access.
//! The Tool and these tests exist only with the `security-admin` feature.

use super::*;
use crate::tools::security_tools;

const TOOL: &str = "rocketmq_list_acl_users";

/// What the fake adapter answers for the Tool: one user that two Brokers hold differently.
pub(super) fn answer(
    adapter: &FakeAdapter,
    args: security_tools::ListAclUsersArgs,
) -> Result<QueryResult<security_tools::ListAclUsersOutput>, ToolFailure> {
    if adapter.fail {
        return Err(ToolFailure::Operational(ToolExecutionError::Backend(None)));
    }
    let row = |broker_name: &str, user_status: &str| security_tools::AclUserRow {
        username: "ops".to_string(),
        broker_name: broker_name.to_string(),
        user_type: Some("Super".to_string()),
        user_status: Some(user_status.to_string()),
    };
    let mut result = QueryResult::bypass(security_tools::ListAclUsersOutput {
        cluster: args.cluster,
        user_count: 1,
        users: vec![row("broker-a", "enable"), row("broker-b", "disable")],
        truncated: false,
        queried_broker_count: 2,
        unanswered_brokers: Vec::new(),
        generated_at: "1".to_string(),
    });
    if adapter.partial {
        result.partial = true;
        result.warnings = vec!["acl_user_brokers_unanswered".to_string()];
        result.data.users.truncate(1);
        result.data.unanswered_brokers = vec!["broker-b".to_string()];
    }
    Ok(result)
}

/// A Guard over the example roles and a role `security_admin` that holds exactly the Tool.
fn guard_with_security_admins(profile: &str) -> (Guard, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let permissions = directory.path().join("permissions.toml");
    let example = std::fs::read_to_string(permission_path()).unwrap();
    std::fs::write(
        &permissions,
        format!(
            "{example}\n[roles.security_admin]\nallowed_clusters = [\"*\"]\nallow_tools = [\"{TOOL}\"]\ndeny_tools = \
             [\"*\"]\n"
        ),
    )
    .unwrap();
    let guard = test_guard_with_permissions(profile, false, permissions.to_string_lossy().into_owned());
    (guard, directory)
}

fn arguments() -> serde_json::Value {
    serde_json::json!({"cluster": "local-dev"})
}

async fn call(adapter: FakeAdapter, guard: &Guard, arguments: serde_json::Value) -> CallToolResult {
    ToolExecutor::new(adapter, guard.clone())
        .call(CallToolRequestParams::new(TOOL).with_arguments(arguments.as_object().unwrap().clone()))
        .await
        .unwrap()
}

/// Calls the Tool as a caller that holds exactly `role` and `scopes`.
async fn call_as(guard: &Guard, role: &str, scopes: &[&str]) -> CallToolResult {
    let caller = RequestContext {
        principal: crate::guard::context::Principal {
            id: "caller".to_string(),
            tenant: None,
            roles: [role.to_string()].into_iter().collect(),
            scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
            allowed_clusters: None,
        },
        client: None,
    };
    ToolExecutor::new(
        FakeAdapter {
            fail: false,
            partial: false,
        },
        guard.clone(),
    )
    .with_request_context(caller)
    .call(CallToolRequestParams::new(TOOL).with_arguments(arguments().as_object().unwrap().clone()))
    .await
    .unwrap()
}

fn error(result: &CallToolResult) -> serde_json::Value {
    serde_json::from_str(&content_text(result)).unwrap()
}

#[tokio::test]
async fn a_security_administrator_gets_the_users_of_every_broker() {
    // The local `security_admin` profile holds the scope, and the role of that name the Tool.
    let (guard, _permissions) = guard_with_security_admins("security_admin");
    let adapter = FakeAdapter {
        fail: false,
        partial: false,
    };
    let result = call(adapter, &guard, arguments()).await;

    assert_eq!(result.is_error, Some(false), "{}", content_text(&result));
    let data = &result.structured_content.as_ref().unwrap()["data"];
    assert_eq!(data["user_count"], 1);
    assert_eq!(
        data["users"],
        serde_json::json!([
            {"username": "ops", "broker_name": "broker-a", "user_type": "Super", "user_status": "enable"},
            {"username": "ops", "broker_name": "broker-b", "user_type": "Super", "user_status": "disable"},
        ])
    );
    // The summary counts and names no user.
    assert!(
        content_text(&result).starts_with("Cluster local-dev holds 1 ACL users in 2 rows; 2 of 2 Brokers answered.")
    );
    assert!(result
        .content
        .iter()
        .all(|content| content.as_resource_link().is_none()));
    let records = guard.audit_log().records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, AuditStatus::Success);
    assert_eq!(records[0].risk_level, crate::guard::RiskLevel::SecurityAdmin);

    let partial = call(
        FakeAdapter {
            fail: false,
            partial: true,
        },
        &guard,
        arguments(),
    )
    .await;
    let structured = partial.structured_content.as_ref().unwrap();
    assert_eq!(structured["partial"], true);
    assert_eq!(
        structured["warnings"],
        serde_json::json!(["acl_user_brokers_unanswered"])
    );
    assert_eq!(
        structured["data"]["unanswered_brokers"],
        serde_json::json!(["broker-b"])
    );
    assert!(
        content_text(&partial).starts_with("Cluster local-dev holds 1 ACL users in 1 rows; 1 of 2 Brokers answered.")
    );

    let down = call(
        FakeAdapter {
            fail: true,
            partial: false,
        },
        &guard,
        arguments(),
    )
    .await;
    assert_eq!(error(&down)["code"], "source_unavailable");
}

#[tokio::test]
async fn listing_users_takes_both_the_scope_and_a_role_that_holds_the_tool() {
    // No other local profile implies the scope, whatever its role holds.
    for profile in ["read_only", "diagnose", "operator"] {
        let (guard, _permissions) = guard_with_security_admins(profile);
        let denied = call(
            FakeAdapter {
                fail: false,
                partial: false,
            },
            &guard,
            arguments(),
        )
        .await;
        assert_eq!(error(&denied)["code"], "unauthorized_scope", "{profile}");
    }

    let (guard, _permissions) = guard_with_security_admins("read_only");
    // Every other scope together does not make up for the missing one.
    let every_other_scope = [
        "rocketmq:read",
        "rocketmq:diagnose",
        "rocketmq:plan",
        "rocketmq:message-search",
    ];
    let unscoped = call_as(&guard, "security_admin", &every_other_scope).await;
    assert_eq!(error(&unscoped)["code"], "unauthorized_scope");

    // The scope does not make up for a role that does not hold the Tool.
    let scoped = ["rocketmq:read", "rocketmq:security-admin"];
    for role in ["read_only", "diagnose", "operator"] {
        let denied = call_as(&guard, role, &scoped).await;
        assert_eq!(error(&denied)["code"], "permission_denied", "{role}");
    }

    let allowed = call_as(&guard, "security_admin", &scoped).await;
    assert_eq!(allowed.is_error, Some(false), "{}", content_text(&allowed));
    // A caller that only lists users does not need to read anything else.
    let only_the_scope = call_as(&guard, "security_admin", &["rocketmq:security-admin"]).await;
    assert_eq!(
        only_the_scope.is_error,
        Some(false),
        "{}",
        content_text(&only_the_scope)
    );

    // Every refusal is recorded, and none of them reached the adapter's answer.
    let records = guard.audit_log().records();
    assert_eq!(records.len(), 6);
    let refused = records
        .iter()
        .filter(|record| record.status != AuditStatus::Success)
        .count();
    assert_eq!(refused, 4);
}

#[tokio::test]
async fn the_tool_takes_a_configured_cluster_and_nothing_else() {
    let (guard, _permissions) = guard_with_security_admins("security_admin");
    for (arguments, code) in [
        (serde_json::json!({}), "invalid_arguments"),
        (serde_json::json!({"cluster": "unconfigured"}), "cluster_not_allowed"),
        (
            serde_json::json!({"cluster": "local-dev", "broker_addr": "10.0.0.1:10911"}),
            "invalid_arguments",
        ),
        // There is no way to ask for one user: a guessed name must not be confirmed one by one.
        (
            serde_json::json!({"cluster": "local-dev", "username": "ops"}),
            "invalid_arguments",
        ),
    ] {
        let adapter = FakeAdapter {
            fail: false,
            partial: false,
        };
        let result = call(adapter, &guard, arguments.clone()).await;
        assert_eq!(result.is_error, Some(true), "{arguments}");
        assert_eq!(error(&result)["code"], code, "{arguments}");
    }
}
