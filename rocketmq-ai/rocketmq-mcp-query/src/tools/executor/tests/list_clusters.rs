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

//! `rocketmq_list_clusters`: what a caller sees of the configured clusters.

use std::collections::BTreeSet;

use super::*;
use crate::config::ControllerAlias;
use crate::config::ProxyAlias;
use crate::guard::context::Principal;

const TOOL: &str = "rocketmq_list_clusters";

/// Two clusters whose configuration holds addresses and a physical name that stay private.
pub(super) fn configured_clusters() -> Vec<ClusterConfig> {
    clusters(None)
}

/// The same clusters, with `beta` bound to a tenant when one is given.
fn clusters(beta_tenant: Option<&str>) -> Vec<ClusterConfig> {
    let cluster = |name: &str, default: Option<bool>, tenant: Option<&str>| ClusterConfig {
        name: name.to_string(),
        namesrv_addr: format!("10.11.12.13:9876;{name}-namesrv.internal:9876"),
        default,
        rocketmq_cluster_name: Some(format!("Physical-{name}")),
        tenant: tenant.map(ToString::to_string),
        credentials: None,
        proxies: vec![ProxyAlias {
            name: format!("{name}-proxy"),
            endpoint: "10.11.12.14:8081".to_string(),
        }],
        controllers: vec![ControllerAlias {
            name: format!("{name}-controller"),
            endpoint: "10.11.12.15:9878".to_string(),
        }],
    };
    vec![cluster("alpha", Some(true), None), cluster("beta", None, beta_tenant)]
}

/// A Guard over `clusters` whose only role, `reader`, is defined by `role`.
fn guard_with_role(role: &str, clusters: &[ClusterConfig], rate_limit_per_minute: u32) -> (Guard, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let permissions = directory.path().join("permissions.toml");
    std::fs::write(&permissions, format!("[roles.reader]\n{role}\n")).unwrap();
    let guard = Guard::new(
        SecurityConfig {
            profile: "reader".to_string(),
            allow_change_planning: false,
            sanitize_output: true,
            rate_limit_per_minute,
            permissions_file: permissions.to_string_lossy().into_owned(),
            max_concurrent_requests_per_cluster: 1,
            pseudonym_key_env: None,
            http_rate_limit_per_minute: None,
        },
        AuditConfig {
            enabled: true,
            sink: "memory".to_string(),
            path: String::new(),
            queue_capacity: 16,
            max_record_bytes: 16 * 1024,
            queue_max_bytes: 1024 * 1024,
            max_file_bytes: 0,
        },
        clusters,
    )
    .unwrap();
    (guard, directory)
}

const EVERY_CLUSTER: &str = "allowed_clusters = [\"*\"]\nallow_tools = [\"rocketmq_list_clusters\"]";

fn caller(tenant: Option<&str>, allowed_clusters: Option<&[&str]>) -> RequestContext {
    RequestContext {
        principal: Principal {
            id: "caller".to_string(),
            tenant: tenant.map(ToString::to_string),
            roles: BTreeSet::from(["reader".to_string()]),
            scopes: BTreeSet::from(["rocketmq:read".to_string()]),
            allowed_clusters: allowed_clusters.map(|clusters| clusters.iter().map(ToString::to_string).collect()),
        },
        client: None,
    }
}

async fn call(guard: &Guard, context: RequestContext, arguments: serde_json::Value) -> CallToolResult {
    ToolExecutor::new(
        FakeAdapter {
            fail: false,
            partial: false,
        },
        guard.clone(),
    )
    .with_request_context(context)
    .call(CallToolRequestParams::new(TOOL).with_arguments(arguments.as_object().unwrap().clone()))
    .await
    .unwrap()
}

/// The cluster names of a successful result, in the order they were returned.
fn names(result: &CallToolResult) -> Vec<String> {
    assert_ne!(result.is_error, Some(true), "{}", content_text(result));
    result.structured_content.as_ref().unwrap()["data"]["clusters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|cluster| cluster["name"].as_str().unwrap().to_string())
        .collect()
}

fn error_code(result: &CallToolResult) -> String {
    assert_eq!(result.is_error, Some(true));
    serde_json::from_str::<serde_json::Value>(&content_text(result)).unwrap()["code"]
        .as_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn list_clusters_returns_only_authorized_clusters() {
    let no_arguments = serde_json::json!({});

    // The role reaches every cluster.
    let (everywhere, _permissions) = guard_with_role(EVERY_CLUSTER, &clusters(None), 60);
    let result = call(&everywhere, caller(None, None), no_arguments.clone()).await;
    assert_eq!(names(&result), ["alpha", "beta"]);
    let structured = result.structured_content.as_ref().unwrap();
    assert_eq!(structured["schema_version"], "rocketmq-mcp.v2");
    assert!(structured.get("cluster").is_none(), "the envelope names no cluster");
    assert_eq!(structured["partial"], false);
    assert_eq!(
        structured["data"]["clusters"][0],
        serde_json::json!({
            "name": "alpha",
            "default": true,
            "proxy_names": ["alpha-proxy"],
            "controller_names": ["alpha-controller"],
        })
    );
    assert_eq!(structured["data"]["clusters"][1]["default"], false);

    // The role reaches one cluster of the two.
    let one = "allowed_clusters = [\"beta\"]\nallow_tools = [\"rocketmq_list_clusters\"]";
    let (restricted, _permissions) = guard_with_role(one, &clusters(None), 60);
    let result = call(&restricted, caller(None, None), no_arguments.clone()).await;
    assert_eq!(names(&result), ["beta"]);

    // The token narrows a role that reaches every cluster.
    let result = call(&everywhere, caller(None, Some(&["alpha"])), no_arguments.clone()).await;
    assert_eq!(names(&result), ["alpha"]);

    // No cluster is reachable: the call succeeds and says so, it does not fall back to all.
    let none = "allowed_clusters = []\nallow_tools = [\"rocketmq_list_clusters\"]";
    let (unreachable, _permissions) = guard_with_role(none, &clusters(None), 60);
    let result = call(&unreachable, caller(None, None), no_arguments).await;
    assert!(names(&result).is_empty());
}

#[tokio::test]
async fn list_clusters_hides_tenant_bound_clusters_from_other_tenants() {
    let (guard, _permissions) = guard_with_role(EVERY_CLUSTER, &clusters(Some("tenant-beta")), 60);
    for (tenant, visible) in [
        (None, vec!["alpha"]),
        (Some("tenant-other"), vec!["alpha"]),
        (Some("tenant-beta"), vec!["alpha", "beta"]),
    ] {
        let result = call(&guard, caller(tenant, None), serde_json::json!({})).await;
        assert_eq!(names(&result), visible, "tenant={tenant:?}");
    }
}

#[tokio::test]
async fn list_clusters_output_contains_no_addresses() {
    let (guard, _permissions) = guard_with_role(EVERY_CLUSTER, &clusters(Some("tenant-beta")), 60);
    let result = call(&guard, caller(Some("tenant-beta"), None), serde_json::json!({})).await;
    assert_eq!(names(&result), ["alpha", "beta"]);

    let published = format!(
        "{}\n{}",
        serde_json::to_string(&result).unwrap(),
        serde_json::to_string(&guard.audit_log().records()).unwrap()
    );
    for private in [
        "10.11.12",
        "9876",
        "8081",
        "9878",
        "namesrv",
        "endpoint",
        "Physical-",
        "tenant-beta",
    ] {
        assert!(
            !published.contains(private),
            "the result or its audit record holds {private}"
        );
    }
    // The summary says how many clusters there are and which one an omitted `cluster` selects.
    assert!(
        content_text(&result).starts_with("2 cluster(s) are available to you; the default is alpha."),
        "{}",
        content_text(&result)
    );
}

#[tokio::test]
async fn list_clusters_requires_read_scope() {
    let (guard, _permissions) = guard_with_role(EVERY_CLUSTER, &clusters(None), 60);
    let mut diagnose_only = caller(None, None);
    diagnose_only.principal.scopes = BTreeSet::from(["rocketmq:diagnose".to_string()]);
    let denied = call(&guard, diagnose_only, serde_json::json!({})).await;
    assert_eq!(error_code(&denied), "unauthorized_scope");

    // The scope alone is not enough either: a role has to grant the Tool.
    let other_tool = "allowed_clusters = [\"*\"]\nallow_tools = [\"rocketmq_list_topics\"]";
    let (without_grant, _permissions) = guard_with_role(other_tool, &clusters(None), 60);
    let denied = call(&without_grant, caller(None, None), serde_json::json!({})).await;
    assert_eq!(error_code(&denied), "permission_denied");
}

#[tokio::test]
async fn list_clusters_takes_no_cluster_argument() {
    let (guard, _permissions) = guard_with_role(EVERY_CLUSTER, &clusters(None), 60);
    let rejected = call(&guard, caller(None, None), serde_json::json!({"cluster": "alpha"})).await;
    assert_eq!(error_code(&rejected), "invalid_arguments");
    let content = serde_json::from_str::<serde_json::Value>(&content_text(&rejected)).unwrap();
    assert_eq!(
        content["violations"],
        serde_json::json!([{"path": "/*", "constraint": "additional_property"}])
    );
    // It was authorized before its arguments were read, and audited without a cluster.
    let records = guard.audit_log().records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].tool, TOOL);
    assert_eq!(records[0].cluster, None);
    assert_eq!(records[0].status, AuditStatus::Failure);
}

#[tokio::test]
async fn list_clusters_is_rate_limited_per_principal_and_takes_no_cluster_permit() {
    let role =
        "allowed_clusters = [\"*\"]\nallow_tools = [\"rocketmq_list_clusters\", \"rocketmq_get_cluster_overview\"]";
    let (guard, _permissions) = guard_with_role(role, &clusters(None), 2);

    // Every cluster's only concurrency permit is held by a call that has not finished.
    let overview = ToolId::GetClusterOverview.descriptor();
    let mut held = Vec::new();
    for cluster in ["alpha", "beta"] {
        let arguments = serde_json::json!({ "cluster": cluster }).as_object().unwrap().clone();
        let begin = || guard.begin_tool_call(&caller(None, None), overview.name, overview.risk_level, &arguments);
        held.push(begin().unwrap());
        assert_eq!(
            begin().unwrap_err(),
            GuardRejection::ConcurrencyLimited,
            "cluster={cluster}"
        );
    }

    for _ in 0..2 {
        let result = call(&guard, caller(None, None), serde_json::json!({})).await;
        assert_eq!(names(&result), ["alpha", "beta"]);
    }
    let limited = call(&guard, caller(None, None), serde_json::json!({})).await;
    assert_eq!(error_code(&limited), "rate_limited");
    // The refusal says when the next call is due: two a minute is one every thirty seconds.
    let refusal = serde_json::from_str::<serde_json::Value>(&content_text(&limited)).unwrap();
    assert_eq!(refusal["retryable"], true);
    let retry_after_ms = refusal["retry_after_ms"].as_u64().unwrap();
    assert!((1..=30_000).contains(&retry_after_ms), "{retry_after_ms}");

    // The budget belongs to the principal, not to a cluster: another principal still has its own.
    let other = || {
        let mut other = caller(None, None);
        other.principal.id = "other-caller".to_string();
        other
    };
    assert_eq!(
        names(&call(&guard, other(), serde_json::json!({})).await),
        ["alpha", "beta"]
    );

    // A busy cluster is not a used-up budget: it has a code of its own and names no wait.
    let adapter = FakeAdapter {
        fail: false,
        partial: false,
    };
    let busy = ToolExecutor::new(adapter, guard.clone())
        .with_request_context(other())
        .call(
            CallToolRequestParams::new(overview.name)
                .with_arguments(serde_json::json!({ "cluster": "alpha" }).as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_eq!(error_code(&busy), "concurrency_limited");
    let refusal = serde_json::from_str::<serde_json::Value>(&content_text(&busy)).unwrap();
    assert_eq!(refusal["retryable"], true);
    assert_eq!(refusal.get("retry_after_ms"), None);
    drop(held);
}
