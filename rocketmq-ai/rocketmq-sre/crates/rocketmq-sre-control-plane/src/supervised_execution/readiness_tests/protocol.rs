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

use axum::http::StatusCode;
use rocketmq_sre_contracts::ActionPlanId;
use serde_json::json;
use std::sync::atomic::Ordering;

use super::support::*;

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL and real JWT/HTTP extraction"]
async fn postgres_readiness_rejects_malformed_closed_or_oversized_payloads_with_safe_json() {
    let fixture = Fixture::new().await;
    let path = format!("/v1/incidents/{}/action-readiness", fixture.incident_id);
    let original = json!(fixture.request);
    let mut cases = Vec::new();
    let mut body = original.clone();
    body["private-unknown-key-marker"] = json!(true);
    cases.push(body);
    let mut body = original.clone();
    body["cluster_id"] = json!("private-invalid-uuid-marker");
    cases.push(body);
    let mut body = original.clone();
    body["plan_id"] = json!(ActionPlanId::new());
    cases.push(body);
    let mut body = original.clone();
    body["parameters"] = json!({"private-large-key-marker":"x".repeat(256*1024)});
    cases.push(body);
    for body in cases {
        let (status, result) = fixture.call(&path, &fixture.auth, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{result}");
        assert_eq!(result["schema_version"], "rocketmq-sre.error.v1");
        assert_eq!(result["code"], "invalid_action_readiness");
        assert!(!result.to_string().contains("private-"));
    }
    let mut forbidden = original;
    forbidden["parameters"]["message_body"] = json!("private-forbidden-marker");
    let (status, result) = fixture.call(&path, &fixture.auth, forbidden).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(result["code"], "forbidden_action_field");
    assert!(!result.to_string().contains("private-"));
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 0);
    fixture.close().await;
}
