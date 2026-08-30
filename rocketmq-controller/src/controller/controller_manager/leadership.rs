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

use super::*;

impl ControllerManager {
    pub(super) async fn start_leadership_watch_loop(self: &Arc<Self>) -> Result<()> {
        let weak_manager = Arc::downgrade(self);
        let interval = Duration::from_millis(self.config.snapshot().heartbeat_interval_ms.max(100));
        let task_group = self.ensure_manager_task_group()?;
        self.synchronize_leadership_gate().await?;
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.clone());
        let task_config = ScheduledTaskConfig::fixed_delay("controller.leadership-watch", interval);

        scheduled_tasks
            .schedule_fixed_delay(task_config, move || {
                let weak_manager = weak_manager.clone();
                async move {
                    let Some(manager) = weak_manager.upgrade() else {
                        return;
                    };

                    if !manager.is_running() {
                        return;
                    }

                    if let Err(error) = manager.synchronize_leadership_gate().await {
                        warn!("Failed to apply leadership state transition: {}", error);
                    }
                }
            })
            .map_err(|error| {
                ControllerError::runtime_error(format!("Failed to schedule leadership watch task: {error}"))
            })?;

        *self.leadership_watch_tasks.lock() = Some(scheduled_tasks);
        Ok(())
    }

    pub(super) async fn synchronize_leadership_gate(&self) -> Result<bool> {
        let mut gate = self.leadership_gate.lock().await;
        self.synchronize_leadership_gate_locked(&mut gate).await
    }

    pub(super) async fn start_broker_role_notifier_and_synchronize(&self, task_group: &TaskGroup) -> Result<()> {
        let mut gate = self.leadership_gate.lock().await;
        if gate.stopping {
            return Err(ControllerError::runtime_error(
                "Controller leadership gate cannot be started after shutdown",
            ));
        }

        self.broker_role_notifier.start(task_group)?;
        let is_leader = self.is_leader();
        self.apply_leadership_state(is_leader).await?;
        gate.applied_is_leader = is_leader;
        Ok(())
    }

    async fn synchronize_leadership_gate_locked(&self, gate: &mut LeadershipGateState) -> Result<bool> {
        if gate.stopping {
            return Ok(false);
        }

        let is_leader = self.is_leader();
        if is_leader != gate.applied_is_leader {
            self.apply_leadership_state(is_leader).await?;
            gate.applied_is_leader = is_leader;
        }
        Ok(is_leader)
    }

    pub(super) async fn stop_leadership_gate(&self) -> Result<()> {
        let mut gate = self.leadership_gate.lock().await;
        gate.stopping = true;
        self.apply_leadership_state(false).await?;
        gate.applied_is_leader = false;
        Ok(())
    }

    async fn apply_leadership_state(&self, is_leader: bool) -> Result<()> {
        if is_leader {
            self.raft_controller.start_scheduling().await.map_err(|error| {
                ControllerError::runtime_error(format!("Failed to start controller scheduling: {error}"))
            })?;
            self.broker_role_notifier.enable();
            info!(
                "Leader-only scheduling enabled on controller {}",
                self.config.snapshot().node_id
            );
        } else {
            self.raft_controller.stop_scheduling().await.map_err(|error| {
                ControllerError::runtime_error(format!("Failed to stop controller scheduling: {error}"))
            })?;
            self.broker_role_notifier.reset();
            info!(
                "Leader-only scheduling disabled and notify dispatch state cleared on controller {}",
                self.config.snapshot().node_id
            );
        }
        Ok(())
    }

    pub(crate) fn broker_role_notifier_snapshot(&self) -> NotifySnapshot {
        self.broker_role_notifier.snapshot()
    }

    pub async fn notify_broker_role_changed(&self, mut response: RemotingCommand) -> Result<()> {
        response.make_custom_header_to_net();
        let response_header = response
            .decode_command_custom_header::<ElectMasterResponseHeader>()
            .map_err(|error| {
                ControllerError::serialization_source(
                    "decode elect-master response header for broker role notify",
                    error,
                )
            })?;

        let Some(body) = response.body() else {
            return Ok(());
        };

        let response_body = ElectMasterResponseBody::decode(body).map_err(|error| {
            ControllerError::serialization_source("decode elect-master response body for broker role notify", error)
        })?;

        let Some(member_group) = response_body.broker_member_group else {
            return Ok(());
        };

        let Some(master_broker_id) = response_header.master_broker_id.and_then(|id| u64::try_from(id).ok()) else {
            warn!(
                "Skip broker role notify because master broker id is absent, broker={}",
                member_group.broker_name
            );
            return Ok(());
        };

        let Some(master_epoch) = response_header.master_epoch else {
            warn!(broker = %member_group.broker_name, "Skip broker role notify because master epoch is absent");
            return Ok(());
        };
        let Ok(master_epoch) = rocketmq_store_api::MasterEpoch::try_from(master_epoch) else {
            warn!(broker = %member_group.broker_name, master_epoch, "Skip broker role notify because master epoch is invalid");
            return Ok(());
        };
        let Some(sync_state_set_epoch) = response_header.sync_state_set_epoch else {
            warn!(broker = %member_group.broker_name, "Skip broker role notify because sync-state-set epoch is absent");
            return Ok(());
        };
        let Ok(sync_state_set_epoch) = rocketmq_store_api::SyncStateSetEpoch::try_from(sync_state_set_epoch) else {
            warn!(broker = %member_group.broker_name, sync_state_set_epoch, "Skip broker role notify because sync-state-set epoch is invalid");
            return Ok(());
        };
        let master_address = response_header.master_address.clone().map(|value| value.to_string());
        let sync_state_set = SyncStateSet::with_values(response_body.sync_state_set, sync_state_set_epoch.get())
            .encode()
            .map_err(|error| {
                ControllerError::serialization_source("encode sync state set for broker role notify", error)
            })?;

        let mut tasks = Vec::new();
        for (broker_id, broker_addr) in member_group.broker_addrs {
            if !self.heartbeat_manager.is_broker_active(
                &member_group.cluster,
                &member_group.broker_name,
                broker_id as i64,
            ) {
                continue;
            }

            let key = NotifyKey {
                cluster_name: member_group.cluster.to_string(),
                broker_name: member_group.broker_name.to_string(),
                broker_id,
            };
            let state = match NotifyState::try_new(
                master_broker_id,
                master_epoch,
                sync_state_set_epoch,
                master_address.clone(),
            ) {
                Ok(state) => state,
                Err(error) => {
                    warn!(%error, broker = %member_group.broker_name, "Skip broker role notify because authority is invalid");
                    return Ok(());
                }
            };
            tasks.push(NotifyTask::new(
                key,
                state,
                broker_addr.clone(),
                response_header.master_address.clone(),
                sync_state_set.clone(),
            ));
        }

        self.submit_broker_role_notifications(tasks).await
    }

    pub(super) async fn submit_broker_role_notifications<I>(&self, tasks: I) -> Result<()>
    where
        I: IntoIterator<Item = NotifyTask>,
    {
        let mut leadership_gate = self.leadership_gate.lock().await;
        if !self.synchronize_leadership_gate_locked(&mut leadership_gate).await? {
            return Ok(());
        }

        for task in tasks {
            let broker_id = task.key.broker_id;
            let broker_name = task.key.broker_name.clone();
            let broker_addr = task.broker_addr.clone();
            let outcome = self.broker_role_notifier.submit(task);
            if matches!(outcome, SubmitOutcome::Full | SubmitOutcome::Closed) {
                warn!(
                    ?outcome,
                    target = %broker_addr,
                    broker_id,
                    broker = %broker_name,
                    "Broker role notify was not retained"
                );
            }
        }

        Ok(())
    }
}
