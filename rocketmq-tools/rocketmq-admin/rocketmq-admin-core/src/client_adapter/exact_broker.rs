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

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;

use crate::core::broker::MAX_EXACT_BROKER_INSTANCES;
use crate::core::query::AdminQueryFailureCode;
use crate::core::query::AdminQuerySource;
use crate::core::query::AdminSourceFailure;
use crate::core::AdminError;
use crate::core::AdminResult;

pub(crate) type ExactBrokerTargetResolution = (Vec<(u64, CheetahString)>, Vec<AdminSourceFailure>);

pub(crate) fn resolve_exact_broker_targets(
    cluster_info: ClusterInfo,
    cluster: &str,
    broker_name: &str,
    source: AdminQuerySource,
) -> AdminResult<ExactBrokerTargetResolution> {
    let cluster_table = cluster_info.cluster_addr_table.as_ref().ok_or_else(|| {
        AdminError::backend(
            "resolve_exact_broker_targets",
            "NameServer cluster metadata is unavailable",
        )
    })?;
    let broker_names = cluster_table.get(cluster).cloned().unwrap_or_default();
    if !broker_names.iter().any(|candidate| candidate.as_str() == broker_name) {
        return Err(AdminError::not_found("broker", broker_name));
    }

    let broker_table = cluster_info.broker_addr_table.ok_or_else(|| {
        AdminError::backend(
            "resolve_exact_broker_targets",
            "NameServer Broker metadata is unavailable",
        )
    })?;
    let Some(broker) = broker_table.get(broker_name) else {
        return Ok((
            Vec::new(),
            vec![AdminSourceFailure::new(
                source,
                AdminQueryFailureCode::InvalidResponse,
                false,
                broker_name,
            )],
        ));
    };
    if broker.cluster() != cluster || broker.broker_name().as_str() != broker_name {
        return Ok((
            Vec::new(),
            vec![AdminSourceFailure::new(
                source,
                AdminQueryFailureCode::InvalidResponse,
                false,
                broker_name,
            )],
        ));
    }
    if broker.broker_addrs().is_empty() {
        return Ok((
            Vec::new(),
            vec![AdminSourceFailure::new(
                source,
                AdminQueryFailureCode::InvalidResponse,
                false,
                broker_name,
            )],
        ));
    }
    if broker.broker_addrs().len() > MAX_EXACT_BROKER_INSTANCES {
        return Err(AdminError::backend(
            "resolve_exact_broker_targets",
            "logical Broker has too many physical instances",
        ));
    }

    let mut endpoint_counts = BTreeMap::<&str, usize>::new();
    for endpoint in broker.broker_addrs().values() {
        *endpoint_counts.entry(endpoint.as_str()).or_default() += 1;
    }
    if endpoint_counts.values().any(|count| *count > 1) {
        return Ok((
            Vec::new(),
            vec![AdminSourceFailure::new(
                source,
                AdminQueryFailureCode::InvalidResponse,
                false,
                broker_name,
            )],
        ));
    }

    let mut targets = Vec::new();
    let mut failures = Vec::new();
    for (broker_id, address) in broker.broker_addrs() {
        let logical_target = broker_instance_target(broker_name, *broker_id);
        if !crate::core::broker::is_valid_remoting_endpoint(address.as_str()) {
            failures.push(AdminSourceFailure::new(
                source,
                AdminQueryFailureCode::InvalidResponse,
                false,
                logical_target,
            ));
        } else {
            targets.push((*broker_id, address.clone()));
        }
    }
    targets.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    Ok((targets, failures))
}

fn broker_instance_target(broker_name: &str, broker_id: u64) -> String {
    format!("{broker_name}.{broker_id}")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;

    use super::*;

    const CLUSTER: &str = "DefaultCluster";
    const BROKER: &str = "broker-a";

    #[test]
    fn resolves_single_and_multiple_instances_in_broker_id_order() {
        let (single, failures) = resolve_exact_broker_targets(
            cluster_info([(0, "broker-a.internal:10911")]),
            CLUSTER,
            BROKER,
            AdminQuerySource::BrokerRuntime,
        )
        .unwrap();
        assert_eq!(single.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![0]);
        assert!(failures.is_empty());

        let (multiple, failures) = resolve_exact_broker_targets(
            cluster_info([
                (2, "broker-a-2.internal:10911"),
                (0, "broker-a-0.internal:10911"),
                (1, "broker-a-1.internal:10911"),
            ]),
            CLUSTER,
            BROKER,
            AdminQuerySource::BrokerRuntime,
        )
        .unwrap();
        assert_eq!(multiple.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![0, 1, 2]);
        assert!(failures.is_empty());
    }

    #[test]
    fn differentiates_not_found_from_missing_or_inconsistent_metadata() {
        let missing =
            resolve_exact_broker_targets(ClusterInfo::default(), CLUSTER, BROKER, AdminQuerySource::BrokerRuntime)
                .unwrap_err();
        assert!(matches!(missing, AdminError::Backend { .. }));

        let not_found = resolve_exact_broker_targets(
            ClusterInfo::new(Some(HashMap::new()), Some(HashMap::new())),
            CLUSTER,
            BROKER,
            AdminQuerySource::BrokerRuntime,
        )
        .unwrap_err();
        assert!(matches!(not_found, AdminError::NotFound { .. }));

        let mut wrong_cluster = cluster_info([(0, "broker-a.internal:10911")]);
        wrong_cluster
            .broker_addr_table
            .as_mut()
            .unwrap()
            .get_mut(BROKER)
            .unwrap()
            .set_cluster("OtherCluster".into());
        let (targets, failures) =
            resolve_exact_broker_targets(wrong_cluster, CLUSTER, BROKER, AdminQuerySource::BrokerRuntime).unwrap();
        assert!(targets.is_empty());
        assert_eq!(failures[0].code(), AdminQueryFailureCode::InvalidResponse);

        let mut wrong_name = cluster_info([(0, "broker-a.internal:10911")]);
        wrong_name
            .broker_addr_table
            .as_mut()
            .unwrap()
            .get_mut(BROKER)
            .unwrap()
            .set_broker_name("broker-b".into());
        let (targets, failures) =
            resolve_exact_broker_targets(wrong_name, CLUSTER, BROKER, AdminQuerySource::BrokerRuntime).unwrap();
        assert!(targets.is_empty());
        assert_eq!(failures[0].code(), AdminQueryFailureCode::InvalidResponse);
    }

    #[test]
    fn enforces_entry_cap_before_filtering_and_rejects_duplicate_endpoints() {
        let sixty_four = (0..64)
            .map(|id| (id, format!("broker-a-{id}.internal:10911")))
            .collect::<Vec<_>>();
        assert_eq!(
            resolve_exact_broker_targets(
                cluster_info(sixty_four.iter().map(|(id, endpoint)| (*id, endpoint.as_str()))),
                CLUSTER,
                BROKER,
                AdminQuerySource::BrokerRuntime,
            )
            .unwrap()
            .0
            .len(),
            64
        );

        let sixty_five = (0..65)
            .map(|id| {
                (
                    id,
                    if id == 64 {
                        "".to_string()
                    } else {
                        format!("broker-a-{id}.internal:10911")
                    },
                )
            })
            .collect::<Vec<_>>();
        let overflow = resolve_exact_broker_targets(
            cluster_info(sixty_five.iter().map(|(id, endpoint)| (*id, endpoint.as_str()))),
            CLUSTER,
            BROKER,
            AdminQuerySource::BrokerRuntime,
        )
        .unwrap_err();
        assert!(matches!(overflow, AdminError::Backend { .. }));

        let (targets, failures) = resolve_exact_broker_targets(
            cluster_info([(0, "duplicate.internal:10911"), (1, "duplicate.internal:10911")]),
            CLUSTER,
            BROKER,
            AdminQuerySource::BrokerRuntime,
        )
        .unwrap();
        assert!(targets.is_empty());
        assert_eq!(failures[0].code(), AdminQueryFailureCode::InvalidResponse);
    }

    #[test]
    fn blank_instance_is_a_partial_source_failure() {
        let (targets, failures) = resolve_exact_broker_targets(
            cluster_info([(0, "broker-a.internal:10911"), (1, "")]),
            CLUSTER,
            BROKER,
            AdminQuerySource::BrokerRuntime,
        )
        .unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].logical_target(), "broker-a.1");
    }

    #[test]
    fn exact_source_aggregation_preserves_partial_rows_and_rejects_total_failure() {
        let (targets, failures) = resolve_exact_broker_targets(
            cluster_info([(0, "broker-a.internal:10911"), (1, "")]),
            CLUSTER,
            BROKER,
            AdminQuerySource::BrokerRuntime,
        )
        .unwrap();
        let partial = crate::core::query::AdminQueryResult::from_sources(targets.clone(), targets.len(), failures)
            .expect("one physical source succeeded");
        assert!(partial.partial);
        assert_eq!(partial.data, targets);
        assert_eq!(partial.source_failures.len(), 1);

        let (targets, failures) = resolve_exact_broker_targets(
            cluster_info([(0, "")]),
            CLUSTER,
            BROKER,
            AdminQuerySource::BrokerRuntime,
        )
        .unwrap();
        let error = crate::core::query::AdminQueryResult::from_sources(targets, 0, failures)
            .expect_err("all physical sources failed");
        assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
    }

    fn cluster_info<'a>(addresses: impl IntoIterator<Item = (u64, &'a str)>) -> ClusterInfo {
        let broker_addrs = addresses
            .into_iter()
            .map(|(id, endpoint)| (id, CheetahString::from(endpoint)))
            .collect();
        let broker = BrokerData::new(CLUSTER.into(), BROKER.into(), broker_addrs, None);
        ClusterInfo::new(
            Some(HashMap::from([(BROKER.into(), broker)])),
            Some(HashMap::from([(
                CLUSTER.into(),
                HashSet::from([CheetahString::from(BROKER)]),
            )])),
        )
    }
}
