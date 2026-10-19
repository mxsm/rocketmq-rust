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

//! Read-client implementation of the address-free Lite Topic observation of one Broker.

use cheetah_string::CheetahString;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::MQAdminLiteReadExt;
use rocketmq_client_rust::MQAdminReadExt;
use rocketmq_error::Error as CanonicalError;
use rocketmq_model::common::mix_all;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;

use crate::core::lite::LiteBrokerObservation;
use crate::core::lite::LiteBrokerQueryRequest;
use crate::core::lite::LiteTopicObservation;
use crate::core::lite::MAX_LITE_TOPICS;
use crate::core::lite::MAX_LITE_TOPIC_GROUPS;
use crate::core::query::AdminQuerySource;
use crate::core::AdminError;
use crate::core::AdminResult;
use crate::exact_broker::resolve_exact_broker_targets;
use crate::topic_observation::promote_to_admin_error;

const OPERATION: &str = "query_lite_broker_info";

#[allow(async_fn_in_trait)]
trait LiteBrokerSource: Send {
    async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError>;
    async fn lite_info(&self, broker_addr: CheetahString) -> Result<GetBrokerLiteInfoResponseBody, CanonicalError>;
}

impl LiteBrokerSource for DefaultMQAdminExt {
    async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError> {
        MQAdminReadExt::examine_broker_cluster_info(self)
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)
    }

    async fn lite_info(&self, broker_addr: CheetahString) -> Result<GetBrokerLiteInfoResponseBody, CanonicalError> {
        MQAdminLiteReadExt::broker_lite_info_at(self, broker_addr)
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)
    }
}

pub(crate) async fn query_lite_broker_info(
    admin: &DefaultMQAdminExt,
    request: &LiteBrokerQueryRequest,
) -> AdminResult<LiteBrokerObservation> {
    query_lite_broker_info_from(admin, request).await
}

/// Asks the master of the named Broker. Lite Topics are served by the master, and a replica
/// reports the limits it would apply and not what is in use.
async fn query_lite_broker_info_from<S: LiteBrokerSource>(
    source: &S,
    request: &LiteBrokerQueryRequest,
) -> AdminResult<LiteBrokerObservation> {
    let request = LiteBrokerQueryRequest::try_new(&request.cluster, &request.broker_name)?;
    let cluster_info = source
        .cluster_info()
        .await
        .map_err(|source| promote_to_admin_error("examine_broker_cluster_info", source))?;
    let (targets, _) = resolve_exact_broker_targets(
        cluster_info,
        &request.cluster,
        &request.broker_name,
        AdminQuerySource::BrokerRuntime,
    )?;
    let master = targets
        .into_iter()
        .find_map(|(broker_id, address)| (broker_id == mix_all::MASTER_ID).then_some(address))
        .ok_or_else(|| AdminError::unavailable(OPERATION, "The Broker has no master that can be asked"))?;
    let info = source
        .lite_info(master)
        .await
        .map_err(|error| AdminError::backend_source("get_broker_lite_info", error))?;
    Ok(project(request.broker_name, &info))
}

fn project(broker_name: String, info: &GetBrokerLiteInfoResponseBody) -> LiteBrokerObservation {
    let mut topics = info
        .get_topic_meta()
        .iter()
        .filter(|(topic, _)| !topic.is_empty())
        .map(|(topic, expiration_minutes)| {
            let mut groups = info
                .get_group_meta()
                .get(topic)
                .into_iter()
                .flatten()
                .filter(|group| !group.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            groups.sort();
            let bound_group_count = groups.len();
            groups.truncate(MAX_LITE_TOPIC_GROUPS);
            LiteTopicObservation {
                topic: topic.to_string(),
                expiration_minutes: *expiration_minutes,
                bound_group_count,
                bound_groups: groups,
            }
        })
        .collect::<Vec<_>>();
    topics.sort_by(|left, right| left.topic.cmp(&right.topic));
    let lite_topic_count = topics.len();
    let truncated = lite_topic_count > MAX_LITE_TOPICS
        || topics
            .iter()
            .take(MAX_LITE_TOPICS)
            .any(|topic| topic.bound_group_count > topic.bound_groups.len());
    topics.truncate(MAX_LITE_TOPICS);
    LiteBrokerObservation {
        broker_name,
        store_type: info.get_store_type().map(ToString::to_string),
        max_lmq_num: info.get_max_lmq_num(),
        current_lmq_num: info.get_current_lmq_num(),
        lite_subscription_count: info.get_lite_subscription_count(),
        order_info_count: info.get_order_info_count(),
        consume_queue_count: info.get_cq_table_size(),
        offset_count: info.get_offset_table_size(),
        event_count: info.get_event_map_size(),
        lite_topic_count,
        lite_topics: topics,
        truncated,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Mutex;

    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;

    use super::*;
    use crate::core::AdminFailure;

    const MASTER: &str = "127.0.0.1:10911";
    const SLAVE: &str = "127.0.0.2:10911";

    struct FakeSource {
        cluster_info: ClusterInfo,
        info: Option<GetBrokerLiteInfoResponseBody>,
        asked: Mutex<Vec<String>>,
    }

    impl LiteBrokerSource for FakeSource {
        async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError> {
            Ok(self.cluster_info.clone())
        }

        async fn lite_info(&self, broker_addr: CheetahString) -> Result<GetBrokerLiteInfoResponseBody, CanonicalError> {
            self.asked.lock().unwrap().push(broker_addr.to_string());
            self.info.clone().ok_or_else(|| {
                CanonicalError::new(&rocketmq_error::PROTOCOL_RESPONSE_FAILED).with_context(
                    rocketmq_error::ErrorContext::new()
                        .with_text(rocketmq_error::fields::OPERATION_DIAGNOSTIC, "lite_observation_test")
                        .with_secret_presence(rocketmq_error::fields::REASON_PRESENT),
                )
            })
        }
    }

    fn source(instances: &[(u64, &str)], info: Option<GetBrokerLiteInfoResponseBody>) -> FakeSource {
        let broker = BrokerData::new(
            "cluster-a".into(),
            "broker-a".into(),
            instances.iter().map(|(id, address)| (*id, (*address).into())).collect(),
            None,
        );
        FakeSource {
            cluster_info: ClusterInfo::new(
                Some(HashMap::from([("broker-a".into(), broker)])),
                Some(HashMap::from([(
                    CheetahString::from("cluster-a"),
                    HashSet::from([CheetahString::from("broker-a")]),
                )])),
            ),
            info,
            asked: Mutex::default(),
        }
    }

    fn request(broker_name: &str) -> LiteBrokerQueryRequest {
        LiteBrokerQueryRequest::try_new("cluster-a", broker_name).unwrap()
    }

    fn reported() -> GetBrokerLiteInfoResponseBody {
        let mut info = GetBrokerLiteInfoResponseBody::new();
        info.set_store_type("RocksDB".into());
        info.set_max_lmq_num(1_000);
        info.set_current_lmq_num(42);
        info.set_lite_subscription_count(7);
        info.set_order_info_count(3);
        info.set_cq_table_size(50);
        info.set_offset_table_size(60);
        info.set_event_map_size(5);
        info.set_topic_meta(HashMap::from([("lite-orders".into(), 30), ("lite-audit".into(), -1)]));
        info.set_group_meta(HashMap::from([(
            CheetahString::from("lite-orders"),
            HashSet::from([CheetahString::from("workers"), CheetahString::from("billing")]),
        )]));
        info
    }

    #[tokio::test]
    async fn the_master_is_asked_and_its_lite_state_is_reported_by_logical_names() {
        let source = source(&[(1, SLAVE), (0, MASTER)], Some(reported()));

        let observed = query_lite_broker_info_from(&source, &request("broker-a"))
            .await
            .unwrap();

        assert_eq!(*source.asked.lock().unwrap(), [MASTER]);
        assert_eq!(observed.broker_name, "broker-a");
        assert_eq!(observed.store_type.as_deref(), Some("RocksDB"));
        assert_eq!((observed.max_lmq_num, observed.current_lmq_num), (1_000, 42));
        assert_eq!(
            (
                observed.lite_subscription_count,
                observed.order_info_count,
                observed.consume_queue_count,
                observed.offset_count,
                observed.event_count
            ),
            (7, 3, 50, 60, 5)
        );
        assert_eq!(observed.lite_topic_count, 2);
        assert!(!observed.truncated);
        assert_eq!(
            observed.lite_topics,
            [
                LiteTopicObservation {
                    topic: "lite-audit".to_owned(),
                    expiration_minutes: -1,
                    bound_group_count: 0,
                    bound_groups: Vec::new(),
                },
                LiteTopicObservation {
                    topic: "lite-orders".to_owned(),
                    expiration_minutes: 30,
                    bound_group_count: 2,
                    bound_groups: vec!["billing".to_owned(), "workers".to_owned()],
                },
            ]
        );
        let published = serde_json::to_string(&observed).unwrap();
        assert!(!published.contains(MASTER) && !published.contains(SLAVE));
    }

    #[tokio::test]
    async fn a_broker_without_a_master_or_outside_the_cluster_and_a_failed_read_are_errors() {
        let replica_only = source(&[(1, SLAVE)], Some(reported()));
        let error = query_lite_broker_info_from(&replica_only, &request("broker-a"))
            .await
            .unwrap_err();
        assert_eq!(error.failure(), AdminFailure::Backend);
        assert!(replica_only.asked.lock().unwrap().is_empty());

        let unknown = source(&[(0, MASTER)], Some(reported()));
        let error = query_lite_broker_info_from(&unknown, &request("broker-z"))
            .await
            .unwrap_err();
        assert_eq!(error.failure(), AdminFailure::NotFound);

        let down = source(&[(0, MASTER)], None);
        let error = query_lite_broker_info_from(&down, &request("broker-a"))
            .await
            .unwrap_err();
        assert_eq!(error.failure(), AdminFailure::Backend);
    }

    #[tokio::test]
    async fn the_lists_are_bounded_and_the_counts_are_not() {
        let mut info = GetBrokerLiteInfoResponseBody::new();
        info.set_topic_meta(
            (0..=MAX_LITE_TOPICS)
                .map(|index| (CheetahString::from(format!("lite-{index:04}")), 10))
                .collect(),
        );
        info.set_group_meta(HashMap::from([(
            CheetahString::from("lite-0000"),
            (0..=MAX_LITE_TOPIC_GROUPS)
                .map(|index| CheetahString::from(format!("group-{index:02}")))
                .collect::<HashSet<_>>(),
        )]));
        let source = source(&[(0, MASTER)], Some(info));

        let observed = query_lite_broker_info_from(&source, &request("broker-a"))
            .await
            .unwrap();

        assert_eq!(observed.lite_topic_count, MAX_LITE_TOPICS + 1);
        assert_eq!(observed.lite_topics.len(), MAX_LITE_TOPICS);
        assert_eq!(observed.lite_topics[0].bound_group_count, MAX_LITE_TOPIC_GROUPS + 1);
        assert_eq!(observed.lite_topics[0].bound_groups.len(), MAX_LITE_TOPIC_GROUPS);
        assert!(observed.truncated);
    }
}
