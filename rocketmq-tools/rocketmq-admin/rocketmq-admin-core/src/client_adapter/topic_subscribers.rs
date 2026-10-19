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

//! Read-client implementation of the address-free Topic subscriber observation.

use std::collections::BTreeSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::MQAdminReadExt;
use rocketmq_client_rust::MQAdminTopicSubscriberReadExt;
use rocketmq_error::Error as CanonicalError;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::group_list::GroupList;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;

use crate::core::query::AdminQueryResult;
use crate::core::query::AdminQuerySource;
use crate::core::topic_observation::QueryTopicSubscribersRequest;
use crate::core::topic_observation::QueryTopicSubscribersResult;
use crate::core::topic_observation::MAX_TOPIC_SUBSCRIBER_GROUPS;
use crate::core::topic_observation::TOPIC_SUBSCRIBERS_TRUNCATED_WARNING;
use crate::core::AdminError;
use crate::core::AdminResult;
use crate::topic_observation::promote_to_admin_error;
use crate::topic_observation::selected_cluster_route_masters;
use crate::topic_observation::source_failure;

/// The per-Broker read whose failures this observation reports.
const SOURCE: AdminQuerySource = AdminQuerySource::SubscriptionGroups;

#[allow(async_fn_in_trait)]
trait TopicSubscriberSource: Send {
    async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError>;
    async fn topic_route(&self, topic: &str) -> Result<Option<TopicRouteData>, CanonicalError>;
    async fn consumer_groups(&self, broker_addr: CheetahString, topic: &str) -> Result<GroupList, CanonicalError>;
}

impl TopicSubscriberSource for DefaultMQAdminExt {
    async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError> {
        MQAdminReadExt::examine_broker_cluster_info(self)
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)
    }

    async fn topic_route(&self, topic: &str) -> Result<Option<TopicRouteData>, CanonicalError> {
        MQAdminReadExt::examine_topic_route_info(self, CheetahString::from(topic))
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)
    }

    async fn consumer_groups(&self, broker_addr: CheetahString, topic: &str) -> Result<GroupList, CanonicalError> {
        MQAdminTopicSubscriberReadExt::topic_consumer_groups_at(self, broker_addr, CheetahString::from(topic))
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)
    }
}

pub(crate) async fn query_topic_subscribers(
    admin: &DefaultMQAdminExt,
    request: &QueryTopicSubscribersRequest,
) -> AdminResult<AdminQueryResult<QueryTopicSubscribersResult>> {
    query_topic_subscribers_from(admin, request).await
}

/// Joins what every selected-cluster master of the Topic route records.
///
/// One Broker answers for the groups it has seen, so the answer of a single Broker can miss a
/// group. A Broker that fails is reported and the others still answer; the query fails as a
/// whole only when none of them does.
async fn query_topic_subscribers_from<S: TopicSubscriberSource>(
    source: &S,
    request: &QueryTopicSubscribersRequest,
) -> AdminResult<AdminQueryResult<QueryTopicSubscribersResult>> {
    let request = QueryTopicSubscribersRequest::try_new(&request.cluster, &request.topic)?;
    let cluster_info = source
        .cluster_info()
        .await
        .map_err(|source| promote_to_admin_error("examine_broker_cluster_info", source))?;
    let route = source
        .topic_route(&request.topic)
        .await
        .map_err(|source| promote_to_admin_error("examine_topic_route_info", source))?
        .ok_or_else(|| AdminError::topic_not_found(&request.topic))?;
    let (targets, mut failures) = selected_cluster_route_masters(&cluster_info, &route, &request.cluster, SOURCE)?;

    let mut groups = BTreeSet::new();
    let mut successful_sources = 0usize;
    for (broker_name, broker_addr) in targets {
        match source.consumer_groups(broker_addr, &request.topic).await {
            Ok(listed) => {
                successful_sources += 1;
                groups.extend(
                    listed
                        .group_list
                        .into_iter()
                        .map(|group| group.to_string())
                        .filter(|group| !group.is_empty()),
                );
            }
            Err(error) => failures.push(source_failure(SOURCE, &broker_name, &error)),
        }
    }

    let truncated = groups.len() > MAX_TOPIC_SUBSCRIBER_GROUPS;
    let result = QueryTopicSubscribersResult {
        topic: request.topic,
        consumer_groups: groups.into_iter().take(MAX_TOPIC_SUBSCRIBER_GROUPS).collect(),
        truncated,
    };
    let mut result = AdminQueryResult::from_sources(result, successful_sources, failures)?;
    if truncated {
        result.partial = true;
        result.warnings.push(TOPIC_SUBSCRIBERS_TRUNCATED_WARNING.to_string());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Mutex;

    use rocketmq_model::common::mix_all;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;

    use super::*;
    use crate::core::query::AdminQueryFailureCode;
    use crate::core::AdminFailure;

    const ADDRESS_A: &str = "127.0.0.1:10911";
    const ADDRESS_B: &str = "127.0.0.2:10911";
    const ADDRESS_C: &str = "127.0.0.3:10911";

    #[derive(Default)]
    struct FakeSource {
        cluster_info: ClusterInfo,
        route: Option<TopicRouteData>,
        /// The groups each Broker address records; an address listed as `None` fails.
        groups: BTreeMap<&'static str, Option<Vec<String>>>,
        calls: Mutex<Vec<String>>,
    }

    impl TopicSubscriberSource for FakeSource {
        async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError> {
            Ok(self.cluster_info.clone())
        }

        async fn topic_route(&self, _topic: &str) -> Result<Option<TopicRouteData>, CanonicalError> {
            Ok(self.route.clone())
        }

        async fn consumer_groups(&self, broker_addr: CheetahString, _topic: &str) -> Result<GroupList, CanonicalError> {
            self.calls.lock().unwrap().push(broker_addr.to_string());
            match self.groups.get(broker_addr.as_str()) {
                Some(Some(groups)) => Ok(group_list(groups)),
                Some(None) => Err(
                    CanonicalError::new(&rocketmq_error::PROTOCOL_RESPONSE_FAILED).with_context(
                        rocketmq_error::ErrorContext::new()
                            .with_text(rocketmq_error::fields::OPERATION_DIAGNOSTIC, "topic_subscribers_test")
                            .with_secret_presence(rocketmq_error::fields::REASON_PRESENT),
                    ),
                ),
                None => Ok(GroupList::default()),
            }
        }
    }

    fn group_list(groups: &[String]) -> GroupList {
        GroupList {
            group_list: groups.iter().map(|group| CheetahString::from(group.as_str())).collect(),
        }
    }

    fn names(groups: &[&str]) -> Option<Vec<String>> {
        Some(groups.iter().map(|group| (*group).to_owned()).collect())
    }

    fn topology(brokers: &[(&str, &str, &str)]) -> (ClusterInfo, TopicRouteData) {
        let mut broker_table = HashMap::new();
        let mut cluster_table = HashMap::<CheetahString, HashSet<CheetahString>>::new();
        let mut route_brokers = Vec::new();
        for (cluster, broker, address) in brokers {
            let data = BrokerData::new(
                (*cluster).into(),
                (*broker).into(),
                HashMap::from([(mix_all::MASTER_ID, (*address).into())]),
                None,
            );
            broker_table.insert((*broker).into(), data.clone());
            cluster_table
                .entry((*cluster).into())
                .or_default()
                .insert((*broker).into());
            route_brokers.push(data);
        }
        (
            ClusterInfo::new(Some(broker_table), Some(cluster_table)),
            TopicRouteData {
                broker_datas: route_brokers,
                ..Default::default()
            },
        )
    }

    fn source(brokers: &[(&str, &str, &str)], groups: &[(&'static str, Option<Vec<String>>)]) -> FakeSource {
        let (cluster_info, route) = topology(brokers);
        FakeSource {
            cluster_info,
            route: Some(route),
            groups: groups.iter().cloned().collect(),
            calls: Mutex::default(),
        }
    }

    fn request() -> QueryTopicSubscribersRequest {
        QueryTopicSubscribersRequest::try_new("cluster-a", "orders").unwrap()
    }

    #[tokio::test]
    async fn subscribers_are_joined_over_every_master_of_the_selected_cluster() {
        let source = source(
            &[
                ("cluster-a", "broker-a", ADDRESS_A),
                ("cluster-a", "broker-b", ADDRESS_B),
                ("cluster-b", "broker-c", ADDRESS_C),
            ],
            &[
                (ADDRESS_A, names(&["workers", "billing", ""])),
                // The second Broker has seen a group that the first one has not.
                (ADDRESS_B, names(&["billing", "audit"])),
                (ADDRESS_C, names(&["other-cluster-group"])),
            ],
        );

        let result = query_topic_subscribers_from(&source, &request()).await.unwrap();

        assert_eq!(result.data.topic, "orders");
        assert_eq!(result.data.consumer_groups, ["audit", "billing", "workers"]);
        assert!(!result.data.truncated);
        assert!(!result.partial);
        assert!(result.source_failures.is_empty());
        // The Broker of the other cluster is never asked.
        assert_eq!(*source.calls.lock().unwrap(), [ADDRESS_A, ADDRESS_B]);
    }

    #[tokio::test]
    async fn a_broker_that_fails_is_reported_and_the_others_still_answer() {
        let source = source(
            &[
                ("cluster-a", "broker-a", ADDRESS_A),
                ("cluster-a", "broker-b", ADDRESS_B),
            ],
            &[(ADDRESS_A, None), (ADDRESS_B, names(&["workers"]))],
        );

        let result = query_topic_subscribers_from(&source, &request()).await.unwrap();

        assert_eq!(result.data.consumer_groups, ["workers"]);
        assert!(result.partial);
        assert_eq!(result.source_failures.len(), 1);
        let failure = &result.source_failures[0];
        assert_eq!(failure.source(), AdminQuerySource::SubscriptionGroups);
        assert_eq!(failure.code(), AdminQueryFailureCode::InvalidResponse);
        assert_eq!(failure.logical_target(), "broker-a");
        // The evidence names the Broker by its logical name and never by its address.
        let evidence = serde_json::to_string(&result.source_failures).unwrap();
        assert!(!evidence.contains(ADDRESS_A));
    }

    #[tokio::test]
    async fn no_answer_at_all_is_an_error_and_an_empty_answer_is_not() {
        let brokers = [("cluster-a", "broker-a", ADDRESS_A)];
        let unreachable = source(&brokers, &[(ADDRESS_A, None)]);
        let error = query_topic_subscribers_from(&unreachable, &request())
            .await
            .unwrap_err();
        assert_eq!(error.failure(), AdminFailure::Backend);

        // A Topic that nobody consumes has an authoritative empty answer.
        let unconsumed = source(&brokers, &[]);
        let result = query_topic_subscribers_from(&unconsumed, &request()).await.unwrap();
        assert!(result.data.consumer_groups.is_empty());
        assert!(!result.partial);

        // A Topic without a route, or without one in the selected cluster, is not found.
        let mut unknown = source(&brokers, &[]);
        unknown.route = None;
        let error = query_topic_subscribers_from(&unknown, &request()).await.unwrap_err();
        assert_eq!(error.failure(), AdminFailure::NotFound);
        let elsewhere = source(&[("cluster-b", "broker-c", ADDRESS_C)], &[]);
        assert!(query_topic_subscribers_from(&elsewhere, &request()).await.is_err());
        assert!(elsewhere.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_number_of_names_is_bounded_and_the_cut_is_reported() {
        let many = (0..=MAX_TOPIC_SUBSCRIBER_GROUPS)
            .map(|index| format!("group-{index:05}"))
            .collect::<Vec<_>>();
        let source = source(&[("cluster-a", "broker-a", ADDRESS_A)], &[(ADDRESS_A, Some(many))]);

        let result = query_topic_subscribers_from(&source, &request()).await.unwrap();

        assert_eq!(result.data.consumer_groups.len(), MAX_TOPIC_SUBSCRIBER_GROUPS);
        assert_eq!(result.data.consumer_groups[0], "group-00000");
        assert!(result.data.truncated);
        assert!(result.partial);
        assert_eq!(result.warnings, [TOPIC_SUBSCRIBERS_TRUNCATED_WARNING]);
    }
}
