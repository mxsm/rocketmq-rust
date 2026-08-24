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

//! Broker list filtering/paging and stable list-detail selection state.

use rocketmq_dashboard_common::{
    BrokerIdentity, BrokerInventoryFilter, BrokerInventoryItem, BrokerInventorySort, broker_inventory_count,
    broker_inventory_page,
};

use crate::{
    features::dashboard_store::{ResourceRequest, ResourceSlot},
    route::{AppRoute, BrokerTab, RouteKey},
    state::UiError,
};

#[derive(Clone, Debug, PartialEq)]
pub struct SelectedBroker {
    pub identity: BrokerIdentity,
    pub stale: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BrokerOpenIntent {
    pub identity: BrokerIdentity,
    pub route: AppRoute,
    pub tab: BrokerTab,
}

pub struct BrokersStore {
    pub inventory: ResourceSlot<Vec<BrokerInventoryItem>>,
    pub filter: BrokerInventoryFilter,
    pub sort: BrokerInventorySort,
    pub page: usize,
    pub page_size: usize,
    pub selected: Option<SelectedBroker>,
    focus_restore_pending: bool,
}

impl Default for BrokersStore {
    fn default() -> Self {
        Self {
            inventory: ResourceSlot::default(),
            filter: BrokerInventoryFilter::default(),
            sort: BrokerInventorySort::default(),
            page: 0,
            page_size: 20,
            selected: None,
            focus_restore_pending: false,
        }
    }
}

impl BrokersStore {
    pub fn begin_refresh(&mut self, revision: u64) -> Option<ResourceRequest> {
        self.inventory.begin(revision)
    }

    pub fn finish_refresh(
        &mut self,
        request: ResourceRequest,
        revision: u64,
        result: Result<Vec<BrokerInventoryItem>, UiError>,
    ) -> bool {
        let result = result.map(|items| (!items.is_empty()).then_some(items));
        if !self.inventory.finish(request, revision, result) {
            return false;
        }
        if let Some(selected) = self.selected.as_mut() {
            selected.stale = !self
                .inventory
                .state
                .value()
                .is_some_and(|items| items.iter().any(|item| item.identity == selected.identity));
        }
        true
    }

    pub fn visible_page(&self) -> Vec<BrokerInventoryItem> {
        self.inventory.state.value().map_or_else(Vec::new, |items| {
            broker_inventory_page(items, &self.filter, self.sort, self.page, self.page_size)
        })
    }

    pub fn find_by_address(&self, address: &str) -> Option<BrokerInventoryItem> {
        self.inventory
            .state
            .value()
            .and_then(|items| items.iter().find(|item| item.identity.address == address))
            .cloned()
    }

    pub fn filtered_count(&self) -> usize {
        self.inventory
            .state
            .value()
            .map_or(0, |items| broker_inventory_count(items, &self.filter))
    }

    pub fn page_count(&self) -> usize {
        self.filtered_count().div_ceil(self.page_size.max(1)).max(1)
    }

    pub fn can_advance_page(&self) -> bool {
        self.page.saturating_add(1) < self.page_count()
    }

    pub fn select(&mut self, identity: BrokerIdentity, tab: BrokerTab) -> Result<BrokerOpenIntent, UiError> {
        let route_key = RouteKey::parse(identity.address.clone()).map_err(|_| {
            UiError::new(
                "The Broker address cannot be represented as a route.",
                crate::state::UiErrorCode::Validation,
                false,
            )
        })?;
        self.selected = Some(SelectedBroker {
            identity: identity.clone(),
            stale: false,
        });
        self.focus_restore_pending = false;
        Ok(BrokerOpenIntent {
            identity,
            route: AppRoute::BrokerDetail { broker: route_key, tab },
            tab,
        })
    }

    pub fn close_sheet(&mut self) {
        self.focus_restore_pending = true;
    }

    pub fn take_focus_restore(&mut self) -> bool {
        std::mem::take(&mut self.focus_restore_pending)
    }

    pub fn invalidate(&mut self) {
        self.inventory.invalidate();
        if let Some(selected) = self.selected.as_mut() {
            selected.stale = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_dashboard_common::{BrokerRole, EndpointAvailability, Observed};

    use super::*;

    fn item(name: &str, address: &str) -> BrokerInventoryItem {
        BrokerInventoryItem {
            identity: BrokerIdentity {
                cluster: "cluster-a".into(),
                broker_name: name.into(),
                broker_id: 0,
                address: address.into(),
            },
            role: BrokerRole::Master,
            version: Observed::Unknown,
            availability: EndpointAvailability::Unknown,
            produce_tps: Observed::Unknown,
            consume_tps: Observed::Unknown,
        }
    }

    #[test]
    fn row_opens_typed_sheet_route_and_close_restores_list_focus() {
        let mut store = BrokersStore::default();
        let selected = item("a", "127.0.0.1:10911");
        let intent = store
            .select(selected.identity.clone(), BrokerTab::Runtime)
            .expect("typed route");
        assert_eq!(
            intent.route,
            AppRoute::BrokerDetail {
                broker: RouteKey::parse("127.0.0.1:10911").expect("route key"),
                tab: BrokerTab::Runtime,
            }
        );
        store.close_sheet();
        assert!(store.take_focus_restore());
        assert!(!store.take_focus_restore());
    }

    #[test]
    fn refresh_keeps_sheet_and_marks_missing_selection_stale_without_switching_target() {
        let mut store = BrokersStore::default();
        let first = store.begin_refresh(1).expect("request");
        let selected = item("a", "127.0.0.1:10911");
        assert!(store.finish_refresh(first, 1, Ok(vec![selected.clone()])));
        store
            .select(selected.identity.clone(), BrokerTab::Overview)
            .expect("select");
        let refresh = store.begin_refresh(1).expect("refresh");
        assert!(store.finish_refresh(refresh, 1, Ok(vec![item("b", "127.0.0.1:20911")])));
        let retained = store.selected.expect("sheet retained");
        assert_eq!(retained.identity, selected.identity);
        assert!(retained.stale);
    }

    #[test]
    fn filter_sort_and_page_are_applied_to_real_inventory() {
        let mut store = BrokersStore {
            page_size: 1,
            ..BrokersStore::default()
        };
        store.filter.keyword = "20911".into();
        let request = store.begin_refresh(1).expect("request");
        assert!(store.finish_refresh(
            request,
            1,
            Ok(vec![item("a", "127.0.0.1:10911"), item("b", "127.0.0.1:20911")])
        ));
        assert_eq!(store.visible_page(), vec![item("b", "127.0.0.1:20911")]);
        assert_eq!(store.filtered_count(), 1);
        assert_eq!(store.page_count(), 1);
        assert!(!store.can_advance_page());
        assert_eq!(
            store.find_by_address("127.0.0.1:10911"),
            Some(item("a", "127.0.0.1:10911"))
        );
    }

    #[test]
    fn deep_link_searches_complete_inventory_and_next_uses_page_count() {
        let mut store = BrokersStore {
            page_size: 1,
            ..BrokersStore::default()
        };
        let request = store.begin_refresh(1).expect("request");
        assert!(store.finish_refresh(
            request,
            1,
            Ok(vec![item("a", "127.0.0.1:10911"), item("b", "127.0.0.1:20911")])
        ));
        assert_eq!(store.page_count(), 2);
        assert!(store.can_advance_page());
        assert_eq!(
            store.find_by_address("127.0.0.1:20911"),
            Some(item("b", "127.0.0.1:20911"))
        );
        store.page = 1;
        assert!(!store.can_advance_page());
    }
}
