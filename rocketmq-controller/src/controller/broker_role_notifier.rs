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

mod actor;
#[cfg(test)]
mod tests;

use cheetah_string::CheetahString;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::header::notify_broker_role_change_request_header::NotifyBrokerRoleChangedRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_store_api::MasterEpoch;
use rocketmq_store_api::StoreContractViolation;
use rocketmq_store_api::SyncStateSetEpoch;
use rocketmq_store_api::WriteAuthority;

pub(crate) use actor::BrokerRoleNotifier;
pub(crate) use actor::NotifySnapshot;
pub(crate) use actor::SubmitOutcome;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct NotifyKey {
    pub cluster_name: String,
    pub broker_name: String,
    pub broker_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NotifyState {
    pub authority: WriteAuthority,
    pub sync_state_set_epoch: SyncStateSetEpoch,
    pub master_address: Option<String>,
}

impl NotifyState {
    pub(crate) fn try_new(
        master_broker_id: u64,
        master_epoch: MasterEpoch,
        sync_state_set_epoch: SyncStateSetEpoch,
        master_address: Option<String>,
    ) -> Result<Self, StoreContractViolation> {
        Ok(Self {
            authority: WriteAuthority::try_from_u64(master_broker_id, master_epoch)?,
            sync_state_set_epoch,
            master_address,
        })
    }

    fn is_same_or_newer_than(&self, current: &Self) -> bool {
        self.authority.master_epoch() > current.authority.master_epoch()
            || (self.authority.master_epoch() == current.authority.master_epoch()
                && (self.authority != current.authority
                    || (self.sync_state_set_epoch >= current.sync_state_set_epoch
                        && self.master_address == current.master_address)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NotifyTask {
    pub key: NotifyKey,
    pub state: NotifyState,
    pub broker_addr: CheetahString,
    pub master_address: Option<CheetahString>,
    pub sync_state_set: Vec<u8>,
    attempt: u32,
    generation: u64,
}

impl NotifyTask {
    pub(crate) fn new(
        key: NotifyKey,
        state: NotifyState,
        broker_addr: CheetahString,
        master_address: Option<CheetahString>,
        sync_state_set: Vec<u8>,
    ) -> Self {
        Self {
            key,
            state,
            broker_addr,
            master_address,
            sync_state_set,
            attempt: 0,
            generation: 0,
        }
    }

    fn build_request(&self, command_factory: &RemotingCommandFactory) -> RemotingCommand {
        let request_header = NotifyBrokerRoleChangedRequestHeader {
            master_address: self.master_address.clone(),
            master_epoch: Some(self.state.authority.master_epoch().get()),
            sync_state_set_epoch: Some(self.state.sync_state_set_epoch.get()),
            master_broker_id: Some(self.state.authority.broker_id() as u64),
        };
        command_factory
            .create_request_command(RequestCode::NotifyBrokerRoleChanged, request_header)
            .set_body(self.sync_state_set.clone())
    }

    fn retry(&self) -> Self {
        let mut next = self.clone();
        next.attempt += 1;
        next
    }

    #[cfg(test)]
    fn new_for_test(key: NotifyKey, state: NotifyState) -> Self {
        Self::new(
            key,
            state,
            CheetahString::from_static_str("127.0.0.1:10911"),
            None,
            Vec::new(),
        )
    }
}
