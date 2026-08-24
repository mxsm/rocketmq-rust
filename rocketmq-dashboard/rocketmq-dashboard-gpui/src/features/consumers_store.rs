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

//! Pure Consumer catalog, route-driven detail, and bounded diagnostic state.

use rocketmq_dashboard_common::{
    ConsumerClients, ConsumerConfiguration, ConsumerDiagnosticKind, ConsumerDiagnosticPayload, ConsumerFilterDraft,
    ConsumerGroupObservation, ConsumerIdentity, ConsumerInventory, ConsumerObservation, ConsumerObservationState,
    ConsumerPage, ConsumerProgress, ConsumerSort, filter_sort_page_consumers,
};

use crate::{
    features::dashboard_store::{ResourceRequest, ResourceSlot},
    route::ConsumerTab,
    services::consumers::ConsumerRequestScope,
    state::{Loadable, UiError},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerLoadRequest {
    resource: ResourceRequest,
    pub scope: ConsumerRequestScope,
}

impl ConsumerLoadRequest {
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

pub struct ConsumerDetailStore {
    pub group: ConsumerIdentity,
    pub active_tab: ConsumerTab,
    pub overview: ResourceSlot<ConsumerGroupObservation>,
    pub clients: ResourceSlot<ConsumerObservation<ConsumerClients>>,
    pub progress: ResourceSlot<ConsumerObservation<ConsumerProgress>>,
    pub configuration: ResourceSlot<ConsumerConfiguration>,
    pub offset_actions: ResourceSlot<ConsumerObservation<ConsumerProgress>>,
    pub diagnostic: ResourceSlot<ConsumerDiagnosticPayload>,
    pub diagnostic_client: Option<rocketmq_dashboard_common::ConsumerClientIdentity>,
    pub diagnostic_kind: Option<ConsumerDiagnosticKind>,
}

impl ConsumerDetailStore {
    pub fn new(group: ConsumerIdentity, active_tab: ConsumerTab) -> Self {
        Self {
            group,
            active_tab,
            overview: ResourceSlot::default(),
            clients: ResourceSlot::default(),
            progress: ResourceSlot::default(),
            configuration: ResourceSlot::default(),
            offset_actions: ResourceSlot::default(),
            diagnostic: ResourceSlot::default(),
            diagnostic_client: None,
            diagnostic_kind: None,
        }
    }

    pub fn set_tab(&mut self, tab: ConsumerTab) {
        self.active_tab = tab;
    }

    pub fn active_is_idle(&self) -> bool {
        match self.active_tab {
            ConsumerTab::Overview => matches!(self.overview.state, Loadable::Idle),
            ConsumerTab::Clients => matches!(self.clients.state, Loadable::Idle),
            ConsumerTab::Progress => matches!(self.progress.state, Loadable::Idle),
            ConsumerTab::Configuration => matches!(self.configuration.state, Loadable::Idle),
            ConsumerTab::OffsetActions => matches!(self.offset_actions.state, Loadable::Idle),
        }
    }

    pub fn begin_active(&mut self, revision: u64) -> Option<ConsumerLoadRequest> {
        match self.active_tab {
            ConsumerTab::Overview => self.overview.begin(revision),
            ConsumerTab::Clients => self.clients.begin(revision),
            ConsumerTab::Progress => self.progress.begin(revision),
            ConsumerTab::Configuration => self.configuration.begin(revision),
            ConsumerTab::OffsetActions => self.offset_actions.begin(revision),
        }
        .map(ConsumerLoadRequest::new)
    }

    pub fn finish_overview(
        &mut self,
        request: ConsumerLoadRequest,
        revision: u64,
        result: Result<ConsumerInventory, UiError>,
    ) -> bool {
        self.overview.finish(
            request.resource,
            revision,
            result.map(|inventory| inventory.groups.into_iter().find(|item| item.identity == self.group)),
        )
    }

    pub fn finish_clients(
        &mut self,
        request: ConsumerLoadRequest,
        revision: u64,
        result: Result<ConsumerObservation<ConsumerClients>, UiError>,
    ) -> bool {
        self.clients.finish(request.resource, revision, result.map(Some))
    }

    pub fn finish_progress(
        &mut self,
        request: ConsumerLoadRequest,
        revision: u64,
        result: Result<ConsumerObservation<ConsumerProgress>, UiError>,
    ) -> bool {
        self.progress.finish(request.resource, revision, result.map(Some))
    }

    pub fn finish_configuration(
        &mut self,
        request: ConsumerLoadRequest,
        revision: u64,
        result: Result<ConsumerConfiguration, UiError>,
    ) -> bool {
        self.configuration.finish(
            request.resource,
            revision,
            result.map(|value| {
                (!value.snapshots.is_empty()
                    || value.observation != ConsumerObservationState::Complete
                    || !value.failures.is_empty())
                .then_some(value)
            }),
        )
    }

    pub fn finish_offset_actions(
        &mut self,
        request: ConsumerLoadRequest,
        revision: u64,
        result: Result<ConsumerObservation<ConsumerProgress>, UiError>,
    ) -> bool {
        self.offset_actions.finish(request.resource, revision, result.map(Some))
    }

    pub fn begin_diagnostic(
        &mut self,
        revision: u64,
        client: rocketmq_dashboard_common::ConsumerClientIdentity,
        kind: ConsumerDiagnosticKind,
    ) -> Option<ConsumerLoadRequest> {
        self.clear_diagnostic();
        self.diagnostic_client = Some(client);
        self.diagnostic_kind = Some(kind);
        self.diagnostic.begin(revision).map(ConsumerLoadRequest::new)
    }

    pub fn finish_diagnostic(
        &mut self,
        request: ConsumerLoadRequest,
        revision: u64,
        result: Result<ConsumerDiagnosticPayload, UiError>,
    ) -> bool {
        self.diagnostic.finish(request.resource, revision, result.map(Some))
    }

    pub fn clear_diagnostic(&mut self) {
        if let Loadable::Ready(payload) = &mut self.diagnostic.state {
            payload.clear();
        }
        self.diagnostic.clear();
        self.diagnostic_client = None;
        self.diagnostic_kind = None;
    }

    pub fn invalidate_overview_and_progress(&mut self) {
        self.overview.clear();
        self.progress.clear();
        self.offset_actions.clear();
    }
}

impl Drop for ConsumerDetailStore {
    fn drop(&mut self) {
        self.clear_diagnostic();
    }
}

pub struct ConsumersStore {
    pub inventory: ResourceSlot<ConsumerInventory>,
    pub draft_filter: ConsumerFilterDraft,
    pub applied_filter: ConsumerFilterDraft,
    pub sort: ConsumerSort,
    pub page: usize,
    pub detail: Option<ConsumerDetailStore>,
    pending_route: Option<(ConsumerIdentity, ConsumerTab)>,
    focus_restore_pending: bool,
}

impl Default for ConsumersStore {
    fn default() -> Self {
        Self {
            inventory: ResourceSlot::default(),
            draft_filter: ConsumerFilterDraft::default(),
            applied_filter: ConsumerFilterDraft::default(),
            sort: ConsumerSort::default(),
            page: 1,
            detail: None,
            pending_route: None,
            focus_restore_pending: false,
        }
    }
}

impl ConsumersStore {
    pub fn begin_inventory(&mut self, revision: u64) -> Option<ConsumerLoadRequest> {
        self.inventory.begin(revision).map(ConsumerLoadRequest::new)
    }

    pub fn finish_inventory(
        &mut self,
        request: ConsumerLoadRequest,
        revision: u64,
        result: Result<ConsumerInventory, UiError>,
    ) -> bool {
        let accepted = self.inventory.finish(
            request.resource,
            revision,
            result.map(|inventory| {
                (!inventory.groups.is_empty()
                    || inventory.observation != ConsumerObservationState::Complete
                    || !inventory.failures.is_empty())
                .then_some(inventory)
            }),
        );
        if accepted {
            self.reconcile_pending_route();
            self.clamp_page();
        }
        accepted
    }

    pub fn page(&self) -> ConsumerPage {
        let items = match &self.inventory.state {
            Loadable::Ready(inventory) => inventory.groups.as_slice(),
            _ => &[],
        };
        filter_sort_page_consumers(items, &self.applied_filter, self.sort, self.page)
    }

    pub fn apply_filter(&mut self) {
        self.applied_filter = self.draft_filter.clone().normalized();
        self.page = 1;
        self.clamp_page();
    }

    pub fn set_page(&mut self, page: usize) {
        self.page = page.max(1);
        self.clamp_page();
    }

    pub fn open_route(&mut self, group: ConsumerIdentity, tab: ConsumerTab) {
        self.close_detail();
        if self.inventory_group(&group).is_some() {
            self.detail = Some(ConsumerDetailStore::new(group, tab));
        } else {
            self.pending_route = Some((group, tab));
        }
    }

    pub fn close_detail(&mut self) {
        self.detail = None;
        self.pending_route = None;
        self.focus_restore_pending = true;
    }

    pub fn take_focus_restore(&mut self) -> bool {
        std::mem::take(&mut self.focus_restore_pending)
    }

    pub fn clear_for_revision(&mut self) {
        self.inventory.clear();
        self.close_detail();
    }

    fn inventory_group(&self, group: &ConsumerIdentity) -> Option<&ConsumerGroupObservation> {
        match &self.inventory.state {
            Loadable::Ready(inventory) => inventory.groups.iter().find(|item| &item.identity == group),
            _ => None,
        }
    }

    fn reconcile_pending_route(&mut self) {
        let Some((group, tab)) = self.pending_route.take() else {
            return;
        };
        if self.inventory_group(&group).is_some() {
            self.detail = Some(ConsumerDetailStore::new(group, tab));
        }
    }

    fn clamp_page(&mut self) {
        self.page = self.page().page;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_dashboard_common::{
        ConnectionScope, ConsumerCapabilities, ConsumerCategory, ConsumerConnectionState, ConsumerSortDirection,
        ConsumerSortKey, ConsumerUnknownReason,
    };

    fn group(index: usize) -> ConsumerGroupObservation {
        ConsumerGroupObservation {
            identity: ConsumerIdentity::parse(format!("group-{index:02}")).expect("group"),
            category: ConsumerCategory::Application,
            connection_state: ConsumerObservation::Complete(ConsumerConnectionState::Connected),
            client_count: ConsumerObservation::Complete(index),
            lag: ConsumerObservation::Complete(index as i64),
            consume_type: ConsumerObservation::Complete("PUSH".into()),
            message_model: ConsumerObservation::Complete("CLUSTERING".into()),
            targets: Vec::new(),
        }
    }

    fn inventory(count: usize) -> ConsumerInventory {
        ConsumerInventory {
            groups: (0..count).map(group).collect(),
            targets: Vec::new(),
            observation: ConsumerObservationState::Complete,
            failures: Vec::new(),
            capabilities: ConsumerCapabilities::for_scope(ConnectionScope::NameServer),
        }
    }

    #[test]
    fn filters_sort_and_page_clamp_are_store_driven() {
        let mut store = ConsumersStore::default();
        store.inventory.replace(inventory(23));
        store.sort.key = ConsumerSortKey::Lag;
        store.sort.direction = ConsumerSortDirection::Descending;
        store.set_page(99);
        assert_eq!(store.page, 3);
        assert_eq!(store.page().items[0].identity.as_str(), "group-02");
    }

    #[test]
    fn stale_revision_cannot_replace_inventory() {
        let mut store = ConsumersStore::default();
        let request = store.begin_inventory(4).expect("request");
        assert!(!store.finish_inventory(request, 5, Ok(inventory(1))));
        assert!(matches!(store.inventory.state, Loadable::InitialLoading));
    }

    #[test]
    fn all_five_tabs_have_independent_lazy_slots() {
        let mut detail =
            ConsumerDetailStore::new(ConsumerIdentity::parse("orders").expect("group"), ConsumerTab::Overview);
        assert!(detail.begin_active(1).is_some());
        detail.set_tab(ConsumerTab::Clients);
        assert!(detail.active_is_idle());
        assert!(detail.begin_active(1).is_some());
        detail.set_tab(ConsumerTab::Progress);
        assert!(detail.begin_active(1).is_some());
        detail.set_tab(ConsumerTab::Configuration);
        assert!(detail.begin_active(1).is_some());
        detail.set_tab(ConsumerTab::OffsetActions);
        assert!(detail.begin_active(1).is_some());
    }

    #[test]
    fn deep_link_waits_for_inventory_and_unknown_remains_unknown() {
        let mut store = ConsumersStore::default();
        store.open_route(
            ConsumerIdentity::parse("group-00").expect("group"),
            ConsumerTab::Clients,
        );
        let request = store.begin_inventory(1).expect("request");
        assert!(store.finish_inventory(request, 1, Ok(inventory(1))));
        assert_eq!(
            store.detail.as_ref().map(|detail| detail.active_tab),
            Some(ConsumerTab::Clients)
        );
        let unknown = ConsumerObservation::<usize>::Unknown {
            reason: ConsumerUnknownReason::Unavailable,
        };
        assert_eq!(unknown.value(), None);
    }
}
