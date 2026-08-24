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

//! Pure read-only Producer catalog and applied-query state.

use rocketmq_dashboard_common::{
    ConsumerClientIdentity, ConsumerObservation, ProducerConnectionQuery, ProducerConnectionQueryDraft,
    ProducerConnections, ProducerFilterDraft, ProducerInventory, ProducerPage, filter_page_producers,
};

use crate::{
    features::dashboard_store::{ResourceRequest, ResourceSlot},
    services::consumers::ConsumerRequestScope,
    state::{Loadable, UiError},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProducerLoadRequest {
    resource: ResourceRequest,
    pub scope: ConsumerRequestScope,
}

impl ProducerLoadRequest {
    fn new(resource: ResourceRequest) -> Self {
        Self {
            scope: ConsumerRequestScope {
                revision: resource.revision(),
                epoch: resource.epoch(),
            },
            resource,
        }
    }
}

pub struct ProducersStore {
    pub inventory: ResourceSlot<ProducerInventory>,
    pub draft_filter: ProducerFilterDraft,
    pub applied_filter: ProducerFilterDraft,
    pub page: usize,
    pub draft_query: ProducerConnectionQueryDraft,
    pub applied_query: Option<ProducerConnectionQuery>,
    pub connections: ResourceSlot<ConsumerObservation<ProducerConnections>>,
    pub selected_client: Option<ConsumerClientIdentity>,
}

impl Default for ProducersStore {
    fn default() -> Self {
        Self {
            inventory: ResourceSlot::default(),
            draft_filter: ProducerFilterDraft::default(),
            applied_filter: ProducerFilterDraft::default(),
            page: 1,
            draft_query: ProducerConnectionQueryDraft::default(),
            applied_query: None,
            connections: ResourceSlot::default(),
            selected_client: None,
        }
    }
}

impl ProducersStore {
    pub fn begin_inventory(&mut self, revision: u64) -> Option<ProducerLoadRequest> {
        self.inventory.begin(revision).map(ProducerLoadRequest::new)
    }

    pub fn finish_inventory(
        &mut self,
        request: ProducerLoadRequest,
        revision: u64,
        result: Result<ProducerInventory, UiError>,
    ) -> bool {
        let accepted = self.inventory.finish(
            request.resource,
            revision,
            result.map(|inventory| {
                (!inventory.groups.is_empty()
                    || inventory.observation != rocketmq_dashboard_common::ConsumerObservationState::Complete
                    || !inventory.failures.is_empty())
                .then_some(inventory)
            }),
        );
        if accepted {
            self.page = self.page().page;
        }
        accepted
    }

    pub fn page(&self) -> ProducerPage {
        let groups = match &self.inventory.state {
            Loadable::Ready(inventory) => inventory.groups.as_slice(),
            _ => &[],
        };
        filter_page_producers(groups, &self.applied_filter, self.page)
    }

    pub fn apply_filter(&mut self) {
        self.applied_filter = self.draft_filter.clone();
        self.page = 1;
    }

    pub fn set_page(&mut self, page: usize) {
        self.page = page.max(1);
        self.page = self.page().page;
    }

    pub fn apply_query(
        &mut self,
    ) -> Result<ProducerConnectionQuery, rocketmq_dashboard_common::ProducerValidationError> {
        let query = ProducerConnectionQuery::try_from_draft(&self.draft_query)?;
        self.applied_query = Some(query.clone());
        self.connections.clear();
        self.selected_client = None;
        Ok(query)
    }

    pub fn begin_connections(&mut self, revision: u64) -> Option<ProducerLoadRequest> {
        self.connections.begin(revision).map(ProducerLoadRequest::new)
    }

    pub fn finish_connections(
        &mut self,
        request: ProducerLoadRequest,
        revision: u64,
        result: Result<ConsumerObservation<ProducerConnections>, UiError>,
    ) -> bool {
        self.connections.finish(request.resource, revision, result.map(Some))
    }

    pub fn select_client(&mut self, client: ConsumerClientIdentity) {
        self.selected_client = Some(client);
    }

    pub fn close_client(&mut self) {
        self.selected_client = None;
    }

    pub fn clear_for_revision(&mut self) {
        self.inventory.clear();
        self.connections.clear();
        self.applied_query = None;
        self.selected_client = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_and_group_are_both_required_before_apply_changes_identity() {
        let mut store = ProducersStore::default();
        store.draft_query.group = "orders-producer".into();
        assert!(store.apply_query().is_err());
        assert!(store.applied_query.is_none());
        store.draft_query.topic = "orders".into();
        let query = store.apply_query().expect("query");
        assert_eq!(query.topic(), "orders");
        assert_eq!(query.group().as_str(), "orders-producer");
    }
}
