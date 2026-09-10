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

use crate::error::{DashboardError, DashboardResult};
use rocketmq_admin_core::client_adapter::AdminSession;
use rocketmq_admin_core::core::topic::{TopicAdmin, TopicCatalog, TopicCatalogRequest};
use serde::Deserialize;

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TopicWriteMode {
    Create,
    Update,
}

#[derive(Clone, Copy)]
pub(crate) enum TopicIntent {
    Create,
    Existing,
}

impl From<TopicWriteMode> for TopicIntent {
    fn from(mode: TopicWriteMode) -> Self {
        match mode {
            TopicWriteMode::Create => Self::Create,
            TopicWriteMode::Update => Self::Existing,
        }
    }
}

pub(crate) trait TopicCatalogSource {
    async fn current_catalog(&mut self) -> DashboardResult<TopicCatalog>;
}
impl TopicCatalogSource for AdminSession {
    async fn current_catalog(&mut self) -> DashboardResult<TopicCatalog> {
        self.get_topic_catalog(&TopicCatalogRequest::default())
            .await
            .map_err(DashboardError::Admin)
    }
}

// The private constructor ensures mutation closures run only after authoritative validation.
pub(crate) struct CheckedTopic(TopicCatalog);
impl CheckedTopic {
    pub(crate) fn execute<T>(self, write: impl FnOnce(TopicCatalog) -> T) -> T {
        write(self.0)
    }
}

pub(crate) async fn check_topic(
    source: &mut impl TopicCatalogSource,
    topic: &str,
    intent: TopicIntent,
) -> DashboardResult<CheckedTopic> {
    let topic = topic.trim();
    if topic.is_empty() {
        return Err(DashboardError::Validation("Topic name is required.".into()));
    }
    let catalog = source.current_catalog().await?;
    let existing = catalog.items.iter().find(|item| item.topic == topic);
    if topic == "TBW102"
        || rocketmq_model::topic::is_system_topic(topic)
        || existing.is_some_and(|item| item.system_topic)
    {
        return Err(DashboardError::Validation(
            "System topics are read-only in the dashboard.".into(),
        ));
    }
    match (intent, existing.is_some()) {
        (TopicIntent::Create, true) => {
            return Err(DashboardError::Validation("Topic already exists; use Update.".into()));
        }
        (TopicIntent::Existing, false) => {
            return Err(DashboardError::Validation(
                "Topic does not exist; refresh the catalog.".into(),
            ));
        }
        _ => {}
    }
    Ok(CheckedTopic(catalog))
}

pub(crate) fn validate_targets(catalog: &TopicCatalog, clusters: &[String], brokers: &[String]) -> DashboardResult<()> {
    if clusters.is_empty() && brokers.is_empty() {
        return Err(DashboardError::Validation(
            "Select at least one current cluster or Broker.".into(),
        ));
    }
    if clusters
        .iter()
        .any(|cluster| !catalog.targets.iter().any(|target| &target.cluster_name == cluster))
        || brokers.iter().any(|broker| {
            !catalog.targets.iter().any(|target| {
                target.broker_names.contains(broker) && (clusters.is_empty() || clusters.contains(&target.cluster_name))
            })
        })
    {
        return Err(DashboardError::Validation(
            "A selected target no longer belongs to the current catalog.".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_admin_core::core::topic::{TopicCatalogItem, TopicTargetOption};
    struct FakeAdmin {
        catalog: TopicCatalog,
        writes: usize,
    }
    impl TopicCatalogSource for FakeAdmin {
        async fn current_catalog(&mut self) -> DashboardResult<TopicCatalog> {
            Ok(self.catalog.clone())
        }
    }
    fn admin(system: bool) -> FakeAdmin {
        FakeAdmin {
            writes: 0,
            catalog: TopicCatalog {
                items: vec![TopicCatalogItem {
                    topic: "orders".into(),
                    category: "NORMAL".into(),
                    message_type: "NORMAL".into(),
                    clusters: vec!["cluster".into()],
                    brokers: vec!["broker".into()],
                    read_queue_count: 4,
                    write_queue_count: 4,
                    perm: 6,
                    order: false,
                    system_topic: system,
                }],
                targets: vec![TopicTargetOption {
                    cluster_name: "cluster".into(),
                    broker_names: vec!["broker".into()],
                }],
            },
        }
    }
    #[tokio::test]
    async fn prohibited_mutations_never_reach_the_executor() {
        for (system, name, intent) in [
            (true, "orders", TopicIntent::Existing),
            (false, "orders", TopicIntent::Create),
            (false, "missing", TopicIntent::Existing),
            (false, "TBW102", TopicIntent::Create),
        ] {
            let mut admin = admin(system);
            let result = check_topic(&mut admin, name, intent).await;
            assert!(result.is_err());
            if let Ok(checked) = result {
                checked.execute(|_| admin.writes += 1);
            }
            assert_eq!(admin.writes, 0);
        }
        let mut admin = admin(false);
        check_topic(&mut admin, "orders", TopicIntent::Existing)
            .await
            .unwrap()
            .execute(|_| admin.writes += 1);
        assert_eq!(admin.writes, 1);
        assert!(validate_targets(&admin.catalog, &["stale-cluster".into()], &[]).is_err());
        assert!(validate_targets(&admin.catalog, &[], &["stale-broker".into()]).is_err());
        assert!(validate_targets(&admin.catalog, &["cluster".into()], &["broker".into()]).is_ok());
    }
}
