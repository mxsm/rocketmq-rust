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

use std::sync::LazyLock;

use serde_json::Value;

const PHASE_ONE_OPENAPI: &str = include_str!("../../../openapi/rocketmq-sre-phase01.openapi.json");

static DOCUMENT: LazyLock<Value> = LazyLock::new(|| {
    // Invariant: the checked-in document is parsed by this module's tests and
    // by the UI type-generation contract before it can be accepted.
    serde_json::from_str(PHASE_ONE_OPENAPI).expect("the checked-in Phase 01 OpenAPI document must be valid JSON")
});

pub(crate) fn document() -> Value {
    DOCUMENT.clone()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    const REQUIRED_PUBLIC_PATHS: &[&str] = &[
        "/healthz",
        "/readyz",
        "/v1/assets",
        "/v1/assets/dashboard-link",
        "/v1/capabilities",
        "/v1/capabilities/coverage",
        "/v1/clusters",
        "/v1/clusters/onboard",
        "/v1/clusters/{id}",
        "/v1/clusters/{id}/capabilities",
        "/v1/clusters/{id}/connector",
        "/v1/clusters/{id}/handshake",
        "/v1/clusters/{id}/inventory/latest",
        "/v1/clusters/{id}/offboard",
        "/v1/conversations",
        "/v1/conversations/{id}",
        "/v1/events/stream",
        "/v1/evidence",
        "/v1/evidence/{id}",
        "/v1/evidence/{id}/content",
        "/v1/incidents",
        "/v1/incidents/{id}",
        "/v1/incidents/{id}/diagnose",
        "/v1/inspections",
        "/v1/inspections/{id}",
        "/v1/inspections/{id}/report",
        "/v1/inspections/{id}/run",
        "/v1/inventory/{id}",
        "/v1/investigations",
        "/v1/investigations/{id}",
        "/v1/investigations/{id}/promote",
        "/v1/knowledge",
        "/v1/knowledge/import",
        "/v1/knowledge/search",
        "/v1/knowledge/{id}",
        "/v1/knowledge/{id}/feedback",
        "/v1/knowledge/{id}/review",
        "/v1/message-journeys",
        "/v1/models/capabilities",
        "/v1/models/invocations",
        "/v1/models/status",
        "/v1/openapi.json",
        "/v1/recommendations",
        "/v1/recommendations/{id}/disposition",
        "/v1/topology",
        "/v1/topology/diff",
    ];

    #[test]
    fn checked_in_document_covers_the_phase_one_public_surface() {
        let document = document();
        let paths = document["paths"].as_object().expect("OpenAPI paths must be an object");
        let actual = paths.keys().map(String::as_str).collect::<BTreeSet<_>>();
        let required = REQUIRED_PUBLIC_PATHS.iter().copied().collect::<BTreeSet<_>>();

        assert_eq!(actual, required);
        assert!(actual.iter().all(|path| !path.starts_with("/internal/")));
    }

    #[test]
    fn every_operation_is_named_versioned_and_has_a_response_contract() {
        let document = document();
        let paths = document["paths"].as_object().expect("OpenAPI paths must be an object");
        let mut operation_ids = BTreeSet::new();

        for (path, path_item) in paths {
            let operations = path_item.as_object().expect("OpenAPI path item must be an object");
            for (method, operation) in operations {
                assert!(
                    matches!(method.as_str(), "get" | "post"),
                    "unsupported method {method} at {path}"
                );
                let operation_id = operation["operationId"]
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| panic!("{method} {path} must have an operationId"));
                assert!(
                    operation_ids.insert(operation_id),
                    "duplicate operationId {operation_id} at {method} {path}"
                );
                assert!(
                    operation["responses"]
                        .as_object()
                        .is_some_and(|value| !value.is_empty()),
                    "{method} {path} must have a response contract"
                );
            }
        }
    }

    #[test]
    fn document_freezes_the_read_only_rocketmq_boundary() {
        let document = document();
        assert_eq!(document["openapi"], "3.1.0");
        assert_eq!(document["x-rocketmq-effective-access"], "read_only");
        assert_eq!(document["x-rocketmq-cluster-mutation-supported"], false);

        let encoded = serde_json::to_string(&document).expect("OpenAPI JSON");
        for forbidden in ["\"delete\":", "/apply", "/reset", "/restart", "/scale", "/truncate"] {
            assert!(!encoded.contains(forbidden), "forbidden OpenAPI surface: {forbidden}");
        }
    }
}
