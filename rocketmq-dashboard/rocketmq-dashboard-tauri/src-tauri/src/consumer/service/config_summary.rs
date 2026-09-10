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

use std::collections::BTreeMap;

use rocketmq_admin_core::core::consumer_workspace::WorkspaceFailureStage;
use serde::Serialize;
use serde_json::Value;

use super::*;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConfigTarget {
    cluster_name: String,
    broker_name: String,
    broker_address: String,
    config: Option<ConsumerConfigView>,
    error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConsumerConfigSummary {
    consumer_group: String,
    targets: Vec<ConfigTarget>,
    discovery_failures: Vec<String>,
    complete: bool,
    inconsistent_fields: Vec<String>,
    // Only values common to successful reads. `complete` qualifies the coverage.
    effective: BTreeMap<String, Value>,
}

impl ConsumerManager {
    pub(crate) async fn query_consumer_config_summary(&self, group: String) -> ConsumerResult<ConsumerConfigSummary> {
        let group = validate_consumer_group_name(&group)?;
        let mut session = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session).await?;
        let admin = &mut session
            .as_mut()
            .ok_or_else(|| ConsumerError::Validation("Consumer connection unavailable.".into()))?
            .admin;
        // Workspace discovery retains failed inventory targets; the legacy catalog does not.
        let discovery = admin.consumer_configuration(&group).await.map_err(map_admin_error)?;
        let discovery_failures = discovery
            .failures
            .into_iter()
            .filter(|failure| failure.stage == WorkspaceFailureStage::Inventory)
            .map(|failure| failure.target)
            .collect();
        let mut targets = Vec::with_capacity(discovery.targets.len());
        for target in discovery.targets {
            let identity = target.target;
            let result = admin
                .query_dashboard_consumer_config(&DashboardConsumerConfigRequest {
                    consumer_group: group.clone(),
                    address: Some(identity.broker_address.clone()),
                })
                .await;
            let (config, error) = match result {
                Ok(config) => (Some(map_consumer_config_view(config)), None),
                Err(_) => (None, Some("Configuration unavailable on this Broker.".into())),
            };
            targets.push(ConfigTarget {
                cluster_name: identity.cluster_name,
                broker_name: identity.broker_name,
                broker_address: identity.broker_address,
                config,
                error,
            });
        }
        summarize(group, targets, discovery_failures)
    }
}

fn summarize(
    group: String,
    targets: Vec<ConfigTarget>,
    discovery_failures: Vec<String>,
) -> ConsumerResult<ConsumerConfigSummary> {
    let mut values = Vec::new();
    for config in targets.iter().filter_map(|target| target.config.as_ref()) {
        let mut config = config.clone();
        config.subscription_topics.sort();
        config
            .attributes
            .sort_by(|a, b| a.key.cmp(&b.key).then(a.value.cmp(&b.value)));
        let Value::Object(mut fields) = serde_json::to_value(config)
            .map_err(|_| ConsumerError::Validation("Cannot represent Consumer configuration.".into()))?
        else {
            return Err(ConsumerError::Validation("Invalid Consumer configuration.".into()));
        };
        for identity in ["consumerGroup", "brokerName", "brokerAddress"] {
            fields.remove(identity);
        }
        values.push(fields);
    }
    let mut effective = BTreeMap::new();
    let mut inconsistent_fields = Vec::new();
    if let Some(first) = values.first() {
        for (field, value) in first {
            if values.iter().all(|fields| fields.get(field) == Some(value)) {
                effective.insert(field.clone(), value.clone());
            } else {
                inconsistent_fields.push(field.clone());
            }
        }
    }
    Ok(ConsumerConfigSummary {
        consumer_group: group,
        complete: !targets.is_empty() && values.len() == targets.len() && discovery_failures.is_empty(),
        targets,
        discovery_failures,
        inconsistent_fields,
        effective,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(name: &str, retries: Option<i32>) -> ConfigTarget {
        let config = retries.map(|retry_max_times| ConsumerConfigView {
            consumer_group: "group".into(),
            broker_name: name.into(),
            broker_address: name.into(),
            consume_enable: true,
            consume_from_min_enable: true,
            consume_broadcast_enable: true,
            consume_message_orderly: false,
            retry_queue_nums: 1,
            retry_max_times,
            broker_id: 0,
            which_broker_when_consume_slowly: 1,
            notify_consumer_ids_changed_enable: true,
            group_sys_flag: 0,
            consume_timeout_minute: 15,
            group_retry_policy_json: "{}".into(),
            subscription_topic_count: 0,
            subscription_topics: vec![],
            attributes: vec![],
        });
        ConfigTarget {
            cluster_name: "cluster".into(),
            broker_name: name.into(),
            broker_address: name.into(),
            error: config.is_none().then(|| "unavailable".into()),
            config,
        }
    }

    #[test]
    fn summary_preserves_differences_common_fields_and_failures() {
        let summary = summarize(
            "group".into(),
            vec![target("a", Some(2)), target("b", Some(3)), target("c", None)],
            vec![],
        )
        .unwrap();
        assert!(!summary.complete);
        assert_eq!(summary.inconsistent_fields, ["retryMaxTimes"]);
        assert_eq!(summary.effective["consumeEnable"], true);
        assert!(!summary.effective.contains_key("retryMaxTimes"));
        assert!(summary.targets[2].error.is_some());
        assert_eq!(summary.targets[1].config.as_ref().unwrap().retry_max_times, 3);
    }

    #[test]
    fn summary_never_claims_complete_for_missing_or_undiscovered_targets() {
        assert!(!summarize("group".into(), vec![], vec![]).unwrap().complete);
        let single = summarize("group".into(), vec![target("a", Some(2))], vec![]).unwrap();
        assert!(single.complete);
        assert_eq!(single.effective["retryMaxTimes"], 2);
        assert!(
            !summarize("group".into(), vec![target("a", Some(2))], vec!["b".into()])
                .unwrap()
                .complete
        );
    }
}
