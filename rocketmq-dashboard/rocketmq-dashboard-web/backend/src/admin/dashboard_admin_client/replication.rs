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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use rocketmq_admin_core::core::infrastructure_observation::BrokerHaObservation;
use rocketmq_admin_core::core::infrastructure_observation::BrokerSyncStateObservation;
use rocketmq_admin_core::core::infrastructure_observation::ControllerMetadataObservation;
use rocketmq_admin_core::core::infrastructure_observation::InfrastructureObservationQueryAdmin;
use rocketmq_admin_core::core::infrastructure_observation::MAX_CONTROLLER_TARGETS;
use rocketmq_admin_core::core::infrastructure_observation::MAX_HA_BROKER_TARGETS;
use rocketmq_admin_core::core::infrastructure_observation::QueryControllerMetadataRequest;
use rocketmq_admin_core::core::infrastructure_observation::QueryHaStatusRequest;
use rocketmq_admin_core::read_client_adapter::ControllerObservationTarget;
use rocketmq_admin_core::read_client_adapter::ReadAdminBuilder;
use rocketmq_admin_core::read_client_adapter::ReadAdminSession;

use super::*;

const ENABLE_CONTROLLER_MODE: &str = "enableControllerMode";
const CONTROLLER_ADDR: &str = "controllerAddr";
/// Each replication RPC fails on its own well inside the topology refresh interval.
const REPLICATION_RPC_TIMEOUT_MILLIS: u64 = 3_000;

/// A broker group whose replication is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationTarget {
    pub cluster_name: String,
    pub broker_name: String,
}

/// What decides which broker of a group is the master.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterElection {
    Configured,
    Controller,
    /// The master's broker config could not be read.
    Unknown,
}

/// Replication of one broker group.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupReplication {
    pub cluster_name: String,
    pub broker_name: String,
    pub election: MasterElection,
    /// Controllers the master names in `controllerAddr`, in Controller mode only.
    pub controller_addresses: Vec<String>,
    /// The master's view of its slaves; `None` when it could not be read.
    pub ha: Option<BrokerHaObservation>,
    /// The Controllers' sync state set, in Controller mode only.
    pub sync_state: Option<BrokerSyncStateObservation>,
}

/// One Controller, with its metadata when it answered.
#[derive(Debug, Clone, PartialEq)]
pub struct ControllerReading {
    pub address: String,
    pub metadata: Option<ControllerMetadataObservation>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReplicationObservation {
    pub groups: Vec<GroupReplication>,
    /// Ordered by address.
    pub controllers: Vec<ControllerReading>,
}

impl DashboardAdminClient {
    /// Reads how each group elects its master, the HA state its master reports and, for groups
    /// in Controller mode, their Controllers and sync state sets.
    ///
    /// The Controllers are the ones each master names in its broker config, so none have to be
    /// configured. A group whose config, HA state or sync state cannot be read is still returned,
    /// without those readings; the read fails as a whole only when its admin session cannot start.
    pub async fn replication(&self, targets: Vec<ReplicationTarget>) -> Result<ReplicationObservation, DashboardError> {
        let mut groups = Vec::with_capacity(targets.len());
        for target in targets {
            let (election, controller_addresses) = match self.broker_config(&target.broker_name).await {
                Ok(config) => controller_settings(&config.entries),
                Err(error) => {
                    tracing::debug!(
                        broker = %target.broker_name,
                        code = %error.code(),
                        "Broker config unavailable for the replication reading"
                    );
                    (MasterElection::Unknown, Vec::new())
                }
            };
            groups.push(GroupReplication {
                cluster_name: target.cluster_name,
                broker_name: target.broker_name,
                election,
                controller_addresses,
                ha: None,
                sync_state: None,
            });
        }
        if groups.is_empty() {
            return Ok(ReplicationObservation::default());
        }

        let controllers = controller_targets(&groups);
        let plan = read_plan(&groups, &controllers);
        let readings = self.read_replication(controllers.clone(), plan).await?;
        Ok(merge_readings(groups, &controllers, readings))
    }

    /// Runs `plan` on a read session with `controllers` as its targets. The session lives in a
    /// service of this client, so it is shut down even when the caller stops waiting.
    async fn read_replication(
        &self,
        controllers: Vec<(String, String)>,
        plan: ReadPlan,
    ) -> Result<Readings, DashboardError> {
        let snapshot = self.admin_config_snapshot().await?;
        let builder = ReadAdminBuilder::new(Arc::clone(&self.client_runtime))
            .namesrv_addr(snapshot.namesrv_addr.clone())
            .admin_group(unique_admin_group())
            .timeout_millis(REPLICATION_RPC_TIMEOUT_MILLIS)
            .vip_channel_enabled(snapshot.use_vip_channel)
            .use_tls(snapshot.use_tls)
            .controller_targets(
                controllers
                    .iter()
                    .map(|(name, address)| ControllerObservationTarget::new(name.clone(), address.clone()))
                    .collect(),
            );
        let builder = match self.admin_credentials.clone() {
            Some(credentials) => builder.credentials(credentials),
            None => builder,
        };
        let cancellation = self.session_tasks.task_group().cancellation_token();
        let (response_tx, response_rx) = oneshot::channel();
        self.session_tasks
            .spawn_service(format!("replication-read-{}", unique_admin_group()), async move {
                let result = async {
                    let mut guard = builder.build_with_guard().await.map_err(DashboardError::from)?;
                    let readings = tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => None,
                        readings = observe(guard.inner_mut(), &plan) => Some(readings),
                    };
                    guard.shutdown().await;
                    readings.ok_or_else(|| {
                        DashboardError::Config("Replication reading was cancelled during shutdown".to_string())
                    })
                }
                .await;
                let _ = response_tx.send(result);
            })
            .map_err(|error| DashboardError::internal_source("Could not start the replication reading", error))?;
        response_rx.await.map_err(|error| {
            DashboardError::internal_source("Replication reading stopped before returning a result", error)
        })?
    }
}

/// Reads `enableControllerMode` and `controllerAddr` from a master's broker config.
fn controller_settings(entries: &BTreeMap<String, String>) -> (MasterElection, Vec<String>) {
    let controller_mode = entries
        .get(ENABLE_CONTROLLER_MODE)
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"));
    if !controller_mode {
        return (MasterElection::Configured, Vec::new());
    }
    let addresses = entries
        .get(CONTROLLER_ADDR)
        .map(|value| {
            value
                .split(';')
                .map(str::trim)
                .filter(|address| !address.is_empty())
                .map(str::to_string)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        })
        .unwrap_or_default();
    (MasterElection::Controller, addresses)
}

/// Every Controller the groups name, ordered by address and named `controller-<n>` for the
/// address-free admin queries. At most [`MAX_CONTROLLER_TARGETS`] are read.
fn controller_targets(groups: &[GroupReplication]) -> Vec<(String, String)> {
    groups
        .iter()
        .flat_map(|group| group.controller_addresses.iter())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .take(MAX_CONTROLLER_TARGETS)
        .enumerate()
        .map(|(index, address)| (format!("controller-{}", index + 1), address.clone()))
        .collect()
}

/// One HA query: some groups of a cluster, with the Controller that leads them for the sync state.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HaQuery {
    cluster_name: String,
    broker_names: Vec<String>,
    controller: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ReadPlan {
    ha: Vec<HaQuery>,
    /// The cluster passed along with the metadata query and the Controllers it reads.
    metadata: Option<(String, Vec<String>)>,
}

/// Groups that share a cluster and a Controller set share a query; any Controller of the set
/// leads the admin core to the set's leader for the sync state.
fn read_plan(groups: &[GroupReplication], controllers: &[(String, String)]) -> ReadPlan {
    let name_of = controllers
        .iter()
        .map(|(name, address)| (address.as_str(), name.as_str()))
        .collect::<BTreeMap<_, _>>();
    let mut batches: BTreeMap<(&str, Option<&str>), Vec<String>> = BTreeMap::new();
    let mut metadata_cluster = None;
    for group in groups {
        let controller = (group.election == MasterElection::Controller)
            .then(|| {
                group
                    .controller_addresses
                    .iter()
                    .find_map(|address| name_of.get(address.as_str()).copied())
            })
            .flatten();
        if controller.is_some() && metadata_cluster.is_none() {
            metadata_cluster = Some(group.cluster_name.clone());
        }
        batches
            .entry((group.cluster_name.as_str(), controller))
            .or_default()
            .push(group.broker_name.clone());
    }
    let ha = batches
        .into_iter()
        .flat_map(|((cluster_name, controller), broker_names)| {
            broker_names
                .chunks(MAX_HA_BROKER_TARGETS)
                .map(|chunk| HaQuery {
                    cluster_name: cluster_name.to_string(),
                    broker_names: chunk.to_vec(),
                    controller: controller.map(str::to_string),
                })
                .collect::<Vec<_>>()
        })
        .collect();
    ReadPlan {
        ha,
        metadata: metadata_cluster.map(|cluster| (cluster, controllers.iter().map(|(name, _)| name.clone()).collect())),
    }
}

#[derive(Debug, Default)]
struct Readings {
    ha: BTreeMap<String, BrokerHaObservation>,
    sync_states: BTreeMap<String, BrokerSyncStateObservation>,
    metadata: BTreeMap<String, ControllerMetadataObservation>,
}

/// Runs the plan. A failed query only leaves its own readings out.
async fn observe(session: &mut ReadAdminSession, plan: &ReadPlan) -> Readings {
    let mut readings = Readings::default();
    for query in &plan.ha {
        let request = QueryHaStatusRequest::try_new(
            query.cluster_name.clone(),
            query.broker_names.clone(),
            query.controller.is_some(),
            query.controller.clone(),
        );
        let result = match request {
            Ok(request) => session.query_ha_status(&request).await,
            Err(error) => Err(error),
        };
        match result {
            Ok(result) => {
                if result.partial {
                    tracing::warn!(
                        cluster = %query.cluster_name,
                        failed_sources = result.source_failures.len(),
                        "Some replication readings of the cluster are unavailable"
                    );
                }
                readings.ha.extend(
                    result
                        .data
                        .brokers
                        .into_iter()
                        .map(|broker| (broker.broker_name.clone(), broker)),
                );
                for state in result.data.controller_sync_states {
                    readings.sync_states.extend(
                        state
                            .brokers
                            .into_iter()
                            .map(|broker| (broker.broker_name.clone(), broker)),
                    );
                }
            }
            Err(error) => {
                tracing::warn!(cluster = %query.cluster_name, %error, "Replication reading of the cluster failed");
            }
        }
    }
    if let Some((cluster_name, controllers)) = &plan.metadata {
        let request = QueryControllerMetadataRequest::try_new(cluster_name.clone(), controllers.clone());
        let result = match request {
            Ok(request) => session.query_controller_metadata(&request).await,
            Err(error) => Err(error),
        };
        match result {
            Ok(result) => {
                if result.partial {
                    tracing::warn!(
                        failed_sources = result.source_failures.len(),
                        "Some Controllers did not report their metadata"
                    );
                }
                readings.metadata = result
                    .data
                    .controllers
                    .into_iter()
                    .map(|controller| (controller.controller_name.clone(), controller))
                    .collect();
            }
            Err(error) => tracing::warn!(%error, "Controller metadata reading failed"),
        }
    }
    readings
}

fn merge_readings(
    mut groups: Vec<GroupReplication>,
    controllers: &[(String, String)],
    mut readings: Readings,
) -> ReplicationObservation {
    for group in &mut groups {
        group.ha = readings.ha.remove(&group.broker_name);
        group.sync_state = readings.sync_states.remove(&group.broker_name);
    }
    ReplicationObservation {
        groups,
        controllers: controllers
            .iter()
            .map(|(name, address)| ControllerReading {
                address: address.clone(),
                metadata: readings.metadata.remove(name),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group(cluster: &str, broker: &str, election: MasterElection, controllers: &[&str]) -> GroupReplication {
        GroupReplication {
            cluster_name: cluster.to_string(),
            broker_name: broker.to_string(),
            election,
            controller_addresses: controllers.iter().map(|address| (*address).to_string()).collect(),
            ha: None,
            sync_state: None,
        }
    }

    fn config(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn reads_controller_mode_and_addresses_from_the_broker_config() {
        assert_eq!(
            controller_settings(&config(&[("brokerName", "broker-a")])),
            (MasterElection::Configured, Vec::new())
        );
        assert_eq!(
            controller_settings(&config(&[
                ("enableControllerMode", "false"),
                ("controllerAddr", "10.0.0.1:9878")
            ])),
            (MasterElection::Configured, Vec::new())
        );
        assert_eq!(
            controller_settings(&config(&[
                ("enableControllerMode", " TRUE "),
                ("controllerAddr", "10.0.0.2:9878; 10.0.0.1:9878;;10.0.0.2:9878")
            ])),
            (
                MasterElection::Controller,
                vec!["10.0.0.1:9878".to_string(), "10.0.0.2:9878".to_string()]
            )
        );
    }

    #[test]
    fn names_every_controller_once_in_address_order() {
        let groups = [
            group(
                "c1",
                "broker-a",
                MasterElection::Controller,
                &["10.0.0.2:9878", "10.0.0.1:9878"],
            ),
            group(
                "c1",
                "broker-b",
                MasterElection::Controller,
                &["10.0.0.1:9878", "10.0.0.2:9878"],
            ),
            group("c2", "broker-c", MasterElection::Configured, &[]),
        ];
        assert_eq!(
            controller_targets(&groups),
            [
                ("controller-1".to_string(), "10.0.0.1:9878".to_string()),
                ("controller-2".to_string(), "10.0.0.2:9878".to_string())
            ]
        );
    }

    #[test]
    fn plans_one_ha_query_per_cluster_and_controller_set() {
        let groups = [
            group(
                "ControllerCluster",
                "broker-c",
                MasterElection::Controller,
                &["10.0.0.1:9878"],
            ),
            group(
                "ControllerCluster",
                "broker-d",
                MasterElection::Controller,
                &["10.0.0.1:9878"],
            ),
            group("ControllerCluster", "broker-e", MasterElection::Unknown, &[]),
            group("DefaultCluster", "broker-a", MasterElection::Configured, &[]),
        ];
        let controllers = controller_targets(&groups);
        let plan = read_plan(&groups, &controllers);
        assert_eq!(
            plan.ha,
            [
                HaQuery {
                    cluster_name: "ControllerCluster".to_string(),
                    broker_names: vec!["broker-e".to_string()],
                    controller: None,
                },
                HaQuery {
                    cluster_name: "ControllerCluster".to_string(),
                    broker_names: vec!["broker-c".to_string(), "broker-d".to_string()],
                    controller: Some("controller-1".to_string()),
                },
                HaQuery {
                    cluster_name: "DefaultCluster".to_string(),
                    broker_names: vec!["broker-a".to_string()],
                    controller: None,
                },
            ]
        );
        assert_eq!(
            plan.metadata,
            Some(("ControllerCluster".to_string(), vec!["controller-1".to_string()]))
        );

        let without_controllers = read_plan(&groups[3..], &[]);
        assert_eq!(without_controllers.ha.len(), 1);
        assert_eq!(without_controllers.metadata, None);
    }

    #[test]
    fn splits_large_clusters_into_bounded_queries() {
        let groups = (0..MAX_HA_BROKER_TARGETS + 1)
            .map(|index| group("big", &format!("broker-{index:03}"), MasterElection::Configured, &[]))
            .collect::<Vec<_>>();
        let plan = read_plan(&groups, &[]);
        assert_eq!(
            plan.ha.iter().map(|query| query.broker_names.len()).collect::<Vec<_>>(),
            [MAX_HA_BROKER_TARGETS, 1]
        );
    }

    #[test]
    fn merges_readings_into_their_groups_and_controllers() {
        let groups = vec![
            group("c1", "broker-a", MasterElection::Controller, &["10.0.0.1:9878"]),
            group("c1", "broker-b", MasterElection::Configured, &[]),
        ];
        let controllers = controller_targets(&groups);
        let ha = BrokerHaObservation {
            broker_name: "broker-a".to_string(),
            broker_id: 0,
            master_commit_log_max_offset: 100,
            in_sync_slave_count: 1,
            pending_group_transfer_request_count: 0,
            pending_group_transfer_oldest_wait_millis: 0,
            group_transfer_ack_notify_count: 0,
            connections: Vec::new(),
        };
        let metadata = ControllerMetadataObservation {
            controller_name: "controller-1".to_string(),
            group: None,
            leader_id: Some("1".to_string()),
            is_leader: Some(true),
            peer_count: Some(3),
            last_log_index: Some(9),
            committed_log_index: Some(9),
            applied_log_index: Some(9),
        };
        let readings = Readings {
            ha: BTreeMap::from([("broker-a".to_string(), ha.clone())]),
            sync_states: BTreeMap::new(),
            metadata: BTreeMap::from([("controller-1".to_string(), metadata.clone())]),
        };
        let observation = merge_readings(groups, &controllers, readings);
        assert_eq!(observation.groups[0].ha, Some(ha));
        assert_eq!(observation.groups[1].ha, None);
        assert_eq!(
            observation.controllers,
            [ControllerReading {
                address: "10.0.0.1:9878".to_string(),
                metadata: Some(metadata),
            }]
        );
    }
}
