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

use super::*;
use crate::{
    core::infrastructure_observation::{QueryControllerMetadataRequest, QueryHaStatusRequest},
    read_client_adapter::ControllerObservationTarget,
};

pub(super) async fn observe(source: &DefaultMQAdminExt) -> AdminResult<TopologyReplicationRead> {
    let cluster = source.cluster().await?;
    let (all, invalid) = targets(&cluster);
    let selected = all.iter().filter(|target| target.broker_id == 0).collect::<Vec<_>>();
    let mut quality = TopologyReadQuality::complete(selected.len());
    quality.truncated = selected.len() > TOPOLOGY_REPLICATION_GROUPS;
    quality.inventory_complete = !quality.truncated && invalid == 0;
    quality.total_complete = invalid == 0;
    quality.failures = invalid;
    let mut groups = Vec::new();
    for master in selected.into_iter().take(TOPOLOGY_REPLICATION_GROUPS) {
        let (election, controllers) = match source.config(master).await {
            Ok(entries) => {
                quality.successful_reads += 1;
                controller_settings(&entries)
            }
            Err(_) => {
                quality.failures += 1;
                (TopologyMasterElection::Unknown, Vec::new())
            }
        };
        let members = all
            .iter()
            .filter(|member| member.cluster == master.cluster && member.broker_name == master.broker_name)
            .collect::<Vec<_>>();
        quality.truncated |=
            members.len() > crate::core::infrastructure_observation::MAX_BROKER_INSTANCES_PER_LOGICAL_BROKER;
        groups.push(TopologyReplicationRow {
            cluster: master.cluster.clone(),
            broker_name: master.broker_name.clone(),
            election,
            controllers,
            members: members
                .into_iter()
                .take(crate::core::infrastructure_observation::MAX_BROKER_INSTANCES_PER_LOGICAL_BROKER)
                .cloned()
                .collect(),
            ha: None,
            sync_state: None,
        });
    }
    let all_controllers = groups
        .iter()
        .flat_map(|group| group.controllers.iter().cloned())
        .collect::<BTreeSet<_>>();
    let controller_total = all_controllers.len();
    let controller_total_complete = quality.inventory_complete
        && !groups
            .iter()
            .any(|group| group.controllers.len() > TOPOLOGY_CONTROLLERS);
    quality.truncated |= all_controllers.len() > TOPOLOGY_CONTROLLERS;
    let controllers = all_controllers
        .into_iter()
        .take(TOPOLOGY_CONTROLLERS)
        .enumerate()
        .map(|(index, address)| ControllerObservationTarget::new(format!("topology-controller-{index}"), address))
        .collect::<Vec<_>>();
    let controllers = match crate::infrastructure_observation::validate_controller_targets(controllers) {
        Ok(controllers) => controllers,
        Err(_) => {
            quality.failures += 1;
            Vec::new()
        }
    };
    for group in &mut groups {
        let controller = (group.election == TopologyMasterElection::Controller)
            .then(|| {
                controllers
                    .iter()
                    .find(|target| group.controllers.iter().any(|address| address == target.endpoint()))
            })
            .flatten();
        let request = QueryHaStatusRequest::try_new(
            &group.cluster,
            [group.broker_name.clone()],
            controller.is_some(),
            controller.map(|target| target.name().to_owned()),
        );
        let reading = match request {
            Ok(request) => {
                crate::infrastructure_observation::query_topology_ha_status(source, &controllers, &request, &cluster)
                    .await
            }
            Err(error) => Err(error),
        };
        match reading {
            Ok(reading) => {
                quality.failures += reading.source_failures.len();
                group.ha = reading
                    .data
                    .brokers
                    .into_iter()
                    .find(|broker| broker.broker_name == group.broker_name);
                group.sync_state = reading
                    .data
                    .controller_sync_states
                    .into_iter()
                    .flat_map(|controller| controller.brokers)
                    .find(|broker| broker.broker_name == group.broker_name);
                quality.successful_reads += usize::from(group.ha.is_some()) + usize::from(group.sync_state.is_some());
            }
            Err(_) => quality.failures += 1,
        }
    }
    let mut metadata = BTreeMap::new();
    if let Some(group) = groups.first().filter(|_| !controllers.is_empty()) {
        let request = QueryControllerMetadataRequest::try_new(
            &group.cluster,
            controllers.iter().map(|target| target.name().to_string()),
        );
        let reading = match request {
            Ok(request) => {
                crate::infrastructure_observation::query_controller_metadata(source, &controllers, &request).await
            }
            Err(error) => Err(error),
        };
        match reading {
            Ok(reading) => {
                quality.failures += reading.source_failures.len();
                quality.successful_reads += reading.data.controllers.len();
                metadata.extend(
                    reading
                        .data
                        .controllers
                        .into_iter()
                        .map(|controller| (controller.controller_name.clone(), controller)),
                );
            }
            Err(_) => quality.failures += 1,
        }
    }
    if quality.truncated {
        quality.inventory_complete = false;
    }
    Ok(TopologyReplicationRead {
        groups: TopologyRead { items: groups, quality },
        controllers: controllers
            .into_iter()
            .map(|controller| TopologyControllerRow {
                address: controller.endpoint().to_string(),
                metadata: metadata.remove(controller.name()),
            })
            .collect(),
        controller_total,
        controller_total_complete,
    })
}

pub(super) fn controller_settings(entries: &BTreeMap<String, String>) -> (TopologyMasterElection, Vec<String>) {
    match entries
        .get("enableControllerMode")
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("false") => (TopologyMasterElection::Configured, Vec::new()),
        Some("true") => {
            let mut addresses = BTreeSet::new();
            for address in entries
                .get("controllerAddr")
                .into_iter()
                .flat_map(|value| value.split(';'))
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                addresses.insert(address.to_string());
                if addresses.len() > TOPOLOGY_CONTROLLERS {
                    break;
                }
            }
            (TopologyMasterElection::Controller, addresses.into_iter().collect())
        }
        _ => (TopologyMasterElection::Unknown, Vec::new()),
    }
}
