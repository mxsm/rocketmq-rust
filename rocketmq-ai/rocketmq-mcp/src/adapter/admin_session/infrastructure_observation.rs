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

use rocketmq_admin_core::core::infrastructure_observation as admin;
use rocketmq_admin_core::core::infrastructure_observation::InfrastructureObservationQueryAdmin;

use super::map_logical_admin_error;
use super::AdminCoreSession;
use crate::model::contract::QueryPayload;
use crate::tools::executor::ToolExecutionError;
use crate::tools::infrastructure_tools as tool;

impl AdminCoreSession {
    pub(super) async fn query_ha_status_observation(
        &mut self,
        broker_names: &[String],
        include_sync_state: bool,
        controller_names: &[String],
    ) -> Result<QueryPayload<tool::GetHaStatusOutput>, ToolExecutionError> {
        let request = admin::QueryHaStatusRequest::try_new(
            self.cluster.rocketmq_cluster_name.clone(),
            broker_names.iter().cloned(),
            include_sync_state,
            controller_names.iter().cloned(),
        )
        .map_err(map_logical_admin_error)?;
        let result = self
            .admin_mut()?
            .query_ha_status(&request)
            .await
            .map_err(map_logical_admin_error)?;
        let cluster = self.cluster.name.clone();
        Ok(QueryPayload::from_admin(result).map(|result| tool::GetHaStatusOutput {
            cluster,
            brokers: result
                .brokers
                .into_iter()
                .map(|broker| tool::BrokerHaObservation {
                    broker_name: broker.broker_name,
                    broker_id: broker.broker_id,
                    master_commit_log_max_offset: broker.master_commit_log_max_offset,
                    in_sync_slave_count: broker.in_sync_slave_count,
                    pending_group_transfer_request_count: broker.pending_group_transfer_request_count,
                    pending_group_transfer_oldest_wait_millis: broker.pending_group_transfer_oldest_wait_millis,
                    group_transfer_ack_notify_count: broker.group_transfer_ack_notify_count,
                    connections: broker
                        .connections
                        .into_iter()
                        .map(|connection| tool::HaConnectionObservation {
                            replica: logical_broker(connection.replica),
                            slave_ack_offset: connection.slave_ack_offset,
                            diff: connection.diff,
                            in_sync: connection.in_sync,
                            transferred_bytes_per_second: connection.transferred_bytes_per_second,
                            transfer_from_where: connection.transfer_from_where,
                        })
                        .collect(),
                })
                .collect(),
            controller_sync_states: result
                .controller_sync_states
                .into_iter()
                .map(|state| tool::ControllerSyncStateObservation {
                    controller_name: state.controller_name,
                    brokers: state
                        .brokers
                        .into_iter()
                        .map(|broker| tool::BrokerSyncStateObservation {
                            broker_name: broker.broker_name,
                            master_broker_id: broker.master_broker_id,
                            master_epoch: broker.master_epoch,
                            sync_state_set_epoch: broker.sync_state_set_epoch,
                            in_sync_replicas: broker.in_sync_replicas.into_iter().map(logical_broker).collect(),
                            not_in_sync_replicas: broker.not_in_sync_replicas.into_iter().map(logical_broker).collect(),
                        })
                        .collect(),
                })
                .collect(),
        }))
    }

    pub(super) async fn query_controller_metadata_observation(
        &mut self,
        controller_names: &[String],
    ) -> Result<QueryPayload<tool::GetControllerMetadataOutput>, ToolExecutionError> {
        let request = admin::QueryControllerMetadataRequest::try_new(
            self.cluster.rocketmq_cluster_name.clone(),
            controller_names.iter().cloned(),
        )
        .map_err(map_logical_admin_error)?;
        let result = self
            .admin_mut()?
            .query_controller_metadata(&request)
            .await
            .map_err(map_logical_admin_error)?;
        let cluster = self.cluster.name.clone();
        Ok(
            QueryPayload::from_admin(result).map(|result| tool::GetControllerMetadataOutput {
                cluster,
                controllers: result
                    .controllers
                    .into_iter()
                    .map(|controller| tool::ControllerMetadataObservation {
                        controller_name: controller.controller_name,
                        group: controller.group,
                        leader_id: controller.leader_id,
                        is_leader: controller.is_leader,
                        peer_count: controller.peer_count,
                        last_log_index: controller.last_log_index,
                        committed_log_index: controller.committed_log_index,
                        applied_log_index: controller.applied_log_index,
                    })
                    .collect(),
            }),
        )
    }

    pub(super) async fn query_nameserver_config_summary_observation(
        &mut self,
    ) -> Result<QueryPayload<tool::GetNameserverConfigSummaryOutput>, ToolExecutionError> {
        let request = admin::QueryNameserverConfigSummaryRequest::try_new(self.cluster.rocketmq_cluster_name.clone())
            .map_err(map_logical_admin_error)?;
        let result = self
            .admin_mut()?
            .query_nameserver_config_summary(&request)
            .await
            .map_err(map_logical_admin_error)?;
        let cluster = self.cluster.name.clone();
        Ok(
            QueryPayload::from_admin(result).map(|result| tool::GetNameserverConfigSummaryOutput {
                cluster,
                nameservers: result
                    .nameservers
                    .into_iter()
                    .map(|nameserver| tool::NameserverConfigObservation {
                        nameserver_name: nameserver.nameserver_name,
                        values: tool::NameserverConfigValues {
                            cluster_test: nameserver.values.cluster_test,
                            order_message_enable: nameserver.values.order_message_enable,
                            return_order_topic_config_to_broker: nameserver.values.return_order_topic_config_to_broker,
                            client_request_thread_pool_nums: nameserver.values.client_request_thread_pool_nums,
                            client_request_thread_pool_queue_capacity: nameserver
                                .values
                                .client_request_thread_pool_queue_capacity,
                            scan_not_active_broker_interval_ms: nameserver.values.scan_not_active_broker_interval_ms,
                            unregister_broker_queue_capacity: nameserver.values.unregister_broker_queue_capacity,
                            support_acting_master: nameserver.values.support_acting_master,
                        },
                    })
                    .collect(),
                inconsistent_fields: result
                    .inconsistent_fields
                    .into_iter()
                    .map(|field| match field {
                        admin::NameserverConfigDifferenceField::ClusterTest => {
                            tool::NameserverConfigDifferenceField::ClusterTest
                        }
                        admin::NameserverConfigDifferenceField::OrderMessageEnable => {
                            tool::NameserverConfigDifferenceField::OrderMessageEnable
                        }
                        admin::NameserverConfigDifferenceField::ReturnOrderTopicConfigToBroker => {
                            tool::NameserverConfigDifferenceField::ReturnOrderTopicConfigToBroker
                        }
                        admin::NameserverConfigDifferenceField::ClientRequestThreadPoolNums => {
                            tool::NameserverConfigDifferenceField::ClientRequestThreadPoolNums
                        }
                        admin::NameserverConfigDifferenceField::ClientRequestThreadPoolQueueCapacity => {
                            tool::NameserverConfigDifferenceField::ClientRequestThreadPoolQueueCapacity
                        }
                        admin::NameserverConfigDifferenceField::ScanNotActiveBrokerIntervalMs => {
                            tool::NameserverConfigDifferenceField::ScanNotActiveBrokerIntervalMs
                        }
                        admin::NameserverConfigDifferenceField::UnregisterBrokerQueueCapacity => {
                            tool::NameserverConfigDifferenceField::UnregisterBrokerQueueCapacity
                        }
                        admin::NameserverConfigDifferenceField::SupportActingMaster => {
                            tool::NameserverConfigDifferenceField::SupportActingMaster
                        }
                    })
                    .collect(),
            }),
        )
    }
}

fn logical_broker(value: admin::LogicalBrokerInstance) -> tool::LogicalBrokerInstance {
    tool::LogicalBrokerInstance {
        broker_name: value.broker_name,
        broker_id: value.broker_id,
    }
}
