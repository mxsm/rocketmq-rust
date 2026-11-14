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

use crate::ConnectorError;
use rocketmq_sre_contracts::ClusterId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const ENV: &str = "ROCKETMQ_SRE_SEND_LATENCY_POLICIES_JSON";

/// Operator-declared estimate predicate; it is not a Broker-enforced timeout.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SendLatencyPolicy {
    pub(crate) cluster_id: ClusterId,
    pub(crate) topic: String,
    pub(crate) p99_millis: u64,
}

pub(super) fn load(clusters: &BTreeMap<String, ClusterId>) -> Result<Vec<SendLatencyPolicy>, ConnectorError> {
    match std::env::var(ENV) {
        Ok(raw) => parse(&raw, &clusters.values().copied().collect()),
        Err(std::env::VarError::NotPresent) => Ok(Vec::new()),
        Err(_) => Err(invalid()),
    }
}

fn invalid() -> ConnectorError {
    ConnectorError::configuration("send latency policies must be bounded exact-cluster/topic millisecond predicates")
}

fn parse(raw: &str, clusters: &BTreeSet<ClusterId>) -> Result<Vec<SendLatencyPolicy>, ConnectorError> {
    if raw.len() > 8192 {
        return Err(invalid());
    }
    let policies: Vec<SendLatencyPolicy> = serde_json::from_str(raw).map_err(|_| invalid())?;
    if policies.len() > 64 {
        return Err(invalid());
    }
    let mut seen = BTreeSet::new();
    for policy in &policies {
        if !clusters.contains(&policy.cluster_id)
            || !(1..=3_600_000).contains(&policy.p99_millis)
            || policy.topic.is_empty()
            || policy.topic.len() > 255
            || !policy
                .topic
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'%' | b':'))
            || !seen.insert((policy.cluster_id, &policy.topic))
        {
            return Err(invalid());
        }
    }
    Ok(policies)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn send_latency_policy_is_explicit_exact_scoped_bounded_and_never_a_default_500ms() {
        let cluster = ClusterId::new();
        let known = BTreeSet::from([cluster]);
        assert!(parse("[]", &known).unwrap().is_empty());
        let record = json!({"cluster_id":cluster,"topic":"orders","p99_millis":200});
        assert_eq!(
            parse(&json!([record.clone()]).to_string(), &known).unwrap()[0].p99_millis,
            200
        );
        for row in [
            json!({"cluster_id":ClusterId::new(),"topic":"orders","p99_millis":200}),
            json!({"cluster_id":cluster,"topic":"*","p99_millis":200}),
            json!({"cluster_id":cluster,"topic":"orders","p99_millis":0}),
            json!({"cluster_id":cluster,"topic":"orders","p99_millis":3_600_001}),
            json!({"cluster_id":cluster,"topic":"orders","p99_millis":200,"allow_unscoped":true}),
            json!({"cluster_id":cluster,"topic":"orders","p99_millis":1.5}),
        ] {
            assert!(parse(&json!([row]).to_string(), &known).is_err());
        }
        assert!(parse(&json!([record.clone(), record]).to_string(), &known).is_err());
        assert!(parse(&" ".repeat(8193), &known).is_err());
        assert!(parse(&json!([{"cluster_id":cluster,"topic":"orders"}]).to_string(), &known).is_err());
    }
}
