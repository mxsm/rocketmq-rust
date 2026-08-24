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

//! Pure Topic catalog, detail, request-freshness, and bounded-overlay state.

use std::collections::BTreeMap;

use rocketmq_dashboard_common::{
    SortDirection, TopicCategory, TopicCompleteness, TopicConfigView, TopicConsumersView, TopicFilterDraft,
    TopicIdentity, TopicInventory, TopicInventoryItem, TopicPage, TopicRouteView, TopicSort, TopicStatsView,
    TopicTargetFailure, filter_sort_page_topics,
};

use crate::{
    features::dashboard_store::{ResourceRequest, ResourceSlot},
    route::{AppRoute, RouteKey, TopicTab},
    services::topics::TopicRequestScope,
    state::{Loadable, UiError},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopicLoadRequest {
    resource: ResourceRequest,
    pub scope: TopicRequestScope,
}

impl TopicLoadRequest {
    fn new(resource: ResourceRequest) -> Self {
        Self {
            scope: TopicRequestScope {
                revision: resource.revision(),
                epoch: resource.epoch(),
            },
            resource,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicCountEvidence {
    Exact(usize),
    LowerBound(usize),
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedTopic {
    pub item: TopicInventoryItem,
    pub stale: bool,
    pub inventory_completeness: TopicCompleteness,
    pub inventory_failures: Vec<TopicTargetFailure>,
}

impl SelectedTopic {
    fn verified(item: TopicInventoryItem) -> Self {
        Self {
            item,
            stale: false,
            inventory_completeness: TopicCompleteness::Complete,
            inventory_failures: Vec::new(),
        }
    }

    pub fn inventory_verified(&self) -> bool {
        !self.stale && self.inventory_completeness.is_complete() && self.inventory_failures.is_empty()
    }
}

pub struct TopicDetailStore {
    pub selected: SelectedTopic,
    pub active_tab: TopicTab,
    pub overview: ResourceSlot<TopicInventoryItem>,
    pub stats: ResourceSlot<TopicStatsView>,
    pub route: ResourceSlot<TopicRouteView>,
    pub configuration: ResourceSlot<TopicConfigView>,
    pub consumers: ResourceSlot<TopicConsumersView>,
}

impl TopicDetailStore {
    pub fn new(item: TopicInventoryItem, active_tab: TopicTab) -> Self {
        Self {
            selected: SelectedTopic::verified(item),
            active_tab,
            overview: ResourceSlot::default(),
            stats: ResourceSlot::default(),
            route: ResourceSlot::default(),
            configuration: ResourceSlot::default(),
            consumers: ResourceSlot::default(),
        }
    }

    pub fn set_tab(&mut self, tab: TopicTab) {
        self.active_tab = tab;
    }

    pub fn active_is_idle(&self) -> bool {
        match self.active_tab {
            TopicTab::Overview => matches!(self.overview.state, Loadable::Idle),
            TopicTab::Stats => matches!(self.stats.state, Loadable::Idle),
            TopicTab::Route => matches!(self.route.state, Loadable::Idle),
            TopicTab::Configuration => matches!(self.configuration.state, Loadable::Idle),
            TopicTab::Consumers => matches!(self.consumers.state, Loadable::Idle),
        }
    }

    pub fn begin_overview(&mut self, revision: u64) -> Option<TopicLoadRequest> {
        self.overview.begin(revision).map(TopicLoadRequest::new)
    }

    pub fn finish_overview(
        &mut self,
        request: TopicLoadRequest,
        revision: u64,
        result: Result<TopicInventory, UiError>,
    ) -> bool {
        let observation = result.map(|inventory| {
            let item = inventory
                .items
                .iter()
                .find(|item| item.identity == self.selected.item.identity)
                .cloned();
            (inventory, item)
        });
        let (inventory, item) = match observation {
            Ok(observation) => observation,
            Err(error) => return self.overview.finish(request.resource, revision, Err(error)),
        };
        if !self.overview.finish(request.resource, revision, Ok(item.clone())) {
            return false;
        }

        self.selected.inventory_completeness = inventory.completeness;
        self.selected.inventory_failures = inventory.failures;
        match inventory.completeness {
            TopicCompleteness::Complete => match item {
                Some(item) => {
                    self.selected.item = item;
                    self.selected.stale = false;
                }
                None => self.mark_inventory_unverified(),
            },
            TopicCompleteness::Partial { .. } => {
                if let Some(item) = item {
                    self.selected.item = item;
                }
                self.mark_inventory_unverified();
            }
        }
        true
    }

    fn mark_inventory_unverified(&mut self) {
        self.selected.stale = true;
        self.stats.invalidate();
        self.route.invalidate();
        self.configuration.invalidate();
        self.consumers.invalidate();
    }

    pub fn begin_stats(&mut self, revision: u64) -> Option<TopicLoadRequest> {
        self.stats.begin(revision).map(TopicLoadRequest::new)
    }

    pub fn finish_stats(
        &mut self,
        request: TopicLoadRequest,
        revision: u64,
        result: Result<TopicStatsView, UiError>,
    ) -> bool {
        self.stats.finish(
            request.resource,
            revision,
            result.map(|value| {
                (!value.offsets.is_empty() || !value.completeness.is_complete() || !value.failures.is_empty())
                    .then_some(value)
            }),
        )
    }

    pub fn begin_route(&mut self, revision: u64) -> Option<TopicLoadRequest> {
        self.route.begin(revision).map(TopicLoadRequest::new)
    }

    pub fn finish_route(
        &mut self,
        request: TopicLoadRequest,
        revision: u64,
        result: Result<TopicRouteView, UiError>,
    ) -> bool {
        self.route.finish(
            request.resource,
            revision,
            result.map(|value| (!value.brokers.is_empty() || !value.queues.is_empty()).then_some(value)),
        )
    }

    pub fn begin_configuration(&mut self, revision: u64) -> Option<TopicLoadRequest> {
        self.configuration.begin(revision).map(TopicLoadRequest::new)
    }

    pub fn finish_configuration(
        &mut self,
        request: TopicLoadRequest,
        revision: u64,
        result: Result<TopicConfigView, UiError>,
    ) -> bool {
        self.configuration.finish(
            request.resource,
            revision,
            result.map(|value| {
                (!value.targets.is_empty() || !value.completeness.is_complete() || !value.failures.is_empty())
                    .then_some(value)
            }),
        )
    }

    pub fn begin_consumers(&mut self, revision: u64) -> Option<TopicLoadRequest> {
        self.consumers.begin(revision).map(TopicLoadRequest::new)
    }

    pub fn finish_consumers(
        &mut self,
        request: TopicLoadRequest,
        revision: u64,
        result: Result<TopicConsumersView, UiError>,
    ) -> bool {
        self.consumers.finish(
            request.resource,
            revision,
            result.map(|value| {
                (!value.items.is_empty() || !value.completeness.is_complete() || !value.failures.is_empty())
                    .then_some(value)
            }),
        )
    }

    pub fn mark_stale(&mut self) {
        self.selected.stale = true;
        self.overview.invalidate();
        self.stats.invalidate();
        self.route.invalidate();
        self.configuration.invalidate();
        self.consumers.invalidate();
    }
}

pub struct TopicsStore {
    pub inventory: ResourceSlot<TopicInventory>,
    pub draft_filter: TopicFilterDraft,
    pub applied_filter: TopicFilterDraft,
    pub sort: TopicSort,
    pub page: usize,
    pub inventory_generation: u64,
    pub detail: Option<TopicDetailStore>,
    pending_route: Option<(TopicIdentity, TopicTab)>,
    focus_restore_pending: bool,
}

impl Default for TopicsStore {
    fn default() -> Self {
        Self {
            inventory: ResourceSlot::default(),
            draft_filter: TopicFilterDraft::default(),
            applied_filter: TopicFilterDraft::default(),
            sort: TopicSort::default(),
            page: 1,
            inventory_generation: 0,
            detail: None,
            pending_route: None,
            focus_restore_pending: false,
        }
    }
}

impl TopicsStore {
    pub fn begin_inventory(&mut self, revision: u64) -> Option<TopicLoadRequest> {
        self.inventory.begin(revision).map(TopicLoadRequest::new)
    }

    pub fn finish_inventory(
        &mut self,
        request: TopicLoadRequest,
        revision: u64,
        result: Result<TopicInventory, UiError>,
    ) -> bool {
        if !self.inventory.finish(request.resource, revision, result.map(Some)) {
            return false;
        }
        self.inventory_generation = self.inventory_generation.saturating_add(1);
        self.reconcile_selection();
        true
    }

    pub fn replace_inventory(&mut self, inventory: TopicInventory) {
        self.inventory.replace(inventory);
        self.inventory_generation = self.inventory_generation.saturating_add(1);
        self.reconcile_selection();
    }

    pub fn search(&mut self) {
        self.applied_filter = self.draft_filter.clone().normalized();
        self.page = 1;
    }

    pub fn reset_filters(&mut self) {
        self.draft_filter = TopicFilterDraft::default();
        self.applied_filter = TopicFilterDraft::default();
        self.sort.direction = SortDirection::Ascending;
        self.page = 1;
    }

    pub fn visible_page(&self) -> TopicPage {
        self.inventory.state.value().map_or(
            TopicPage {
                items: Vec::new(),
                page: 1,
                page_count: 1,
                total: 0,
            },
            |inventory| filter_sort_page_topics(&inventory.items, &self.applied_filter, self.sort, self.page),
        )
    }

    pub fn category_counts(&self) -> BTreeMap<TopicCategory, TopicCountEvidence> {
        let categories = [
            TopicCategory::Application,
            TopicCategory::Retry,
            TopicCategory::Dlq,
            TopicCategory::System,
            TopicCategory::Unknown,
        ];
        let Some(inventory) = self.inventory.state.value() else {
            return categories
                .into_iter()
                .map(|category| (category, TopicCountEvidence::Unknown))
                .collect();
        };
        categories
            .into_iter()
            .map(|category| {
                let observed = inventory.items.iter().filter(|item| item.category == category).count();
                let evidence = match inventory.completeness {
                    TopicCompleteness::Complete => TopicCountEvidence::Exact(observed),
                    TopicCompleteness::Partial { .. } if observed != 0 => TopicCountEvidence::LowerBound(observed),
                    TopicCompleteness::Partial { .. } => TopicCountEvidence::Unknown,
                };
                (category, evidence)
            })
            .collect()
    }

    pub fn find(&self, identity: &TopicIdentity) -> Option<TopicInventoryItem> {
        self.inventory
            .state
            .value()
            .and_then(|inventory| inventory.items.iter().find(|item| &item.identity == identity))
            .cloned()
    }

    pub fn sync_detail_selection_evidence(&mut self, topic: &TopicIdentity, selected: SelectedTopic) -> bool {
        if &selected.item.identity != topic {
            return false;
        }
        let Some(detail) = &mut self.detail else {
            return false;
        };
        if &detail.selected.item.identity != topic {
            return false;
        }
        detail.selected = selected;
        true
    }

    pub fn open(&mut self, item: TopicInventoryItem, tab: TopicTab) -> Result<AppRoute, UiError> {
        let key = RouteKey::parse(item.identity.as_str().to_owned()).map_err(|_| {
            UiError::new(
                "The Topic identity cannot be represented as a route.",
                crate::state::UiErrorCode::Validation,
                false,
            )
        })?;
        self.detail = Some(TopicDetailStore::new(item, tab));
        self.reconcile_selection();
        self.pending_route = None;
        self.focus_restore_pending = false;
        Ok(AppRoute::TopicDetail { topic: key, tab })
    }

    #[cfg(test)]
    pub fn open_route(&mut self, identity: TopicIdentity, tab: TopicTab) -> Option<AppRoute> {
        if let Some(item) = self.find(&identity) {
            self.open(item, tab).ok()
        } else {
            self.pending_route = Some((identity, tab));
            None
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

    pub fn invalidate(&mut self) {
        self.inventory.invalidate();
        if let Some(detail) = &mut self.detail {
            detail.mark_stale();
        }
    }

    fn reconcile_selection(&mut self) {
        let Some(detail) = &mut self.detail else {
            return;
        };
        let Some(inventory) = self.inventory.state.value() else {
            detail.mark_stale();
            return;
        };
        let refreshed = inventory
            .items
            .iter()
            .find(|item| item.identity == detail.selected.item.identity)
            .cloned();
        detail.selected.inventory_completeness = inventory.completeness;
        detail.selected.inventory_failures = inventory.failures.clone();
        match inventory.completeness {
            TopicCompleteness::Complete => match refreshed {
                Some(item) => {
                    detail.selected.item = item;
                    detail.selected.stale = false;
                }
                None => detail.mark_stale(),
            },
            TopicCompleteness::Partial { .. } => {
                if let Some(item) = refreshed {
                    detail.selected.item = item;
                }
                detail.mark_stale();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_dashboard_common::{
        TopicConsumersView, TopicFailureCode, TopicFailureStage, TopicMessageType, TopicPermission, TopicTargetFailure,
        TopicTargetIdentity,
    };

    use super::*;

    fn item(name: &str, category: TopicCategory) -> TopicInventoryItem {
        TopicInventoryItem {
            identity: TopicIdentity::parse(name).expect("topic"),
            category,
            message_type: TopicMessageType::Normal,
            clusters: vec!["cluster-a".into()],
            brokers: vec!["broker-a".into()],
            read_queue_count: Some(8),
            write_queue_count: Some(8),
            permission: Some(TopicPermission::parse(6).expect("permission")),
            ordered: Some(false),
        }
    }

    fn inventory(items: Vec<TopicInventoryItem>, completeness: TopicCompleteness) -> TopicInventory {
        TopicInventory {
            items,
            targets: vec![TopicTargetIdentity::parse("cluster-a", "broker-a", "127.0.0.1:10911").expect("target")],
            completeness,
            failures: Vec::new(),
        }
    }

    fn partial_inventory(items: Vec<TopicInventoryItem>) -> TopicInventory {
        TopicInventory {
            items,
            targets: vec![TopicTargetIdentity::parse("cluster-a", "broker-a", "127.0.0.1:10911").expect("target")],
            completeness: TopicCompleteness::Partial {
                successful_target_count: 1,
                failed_target_count: 1,
            },
            failures: vec![TopicTargetFailure {
                target: "broker-b".into(),
                stage: TopicFailureStage::CatalogConfig,
                code: TopicFailureCode::Unavailable,
                retryable: true,
            }],
        }
    }

    #[test]
    fn draft_filters_apply_only_on_search_and_reset_clears_them() {
        let mut store = TopicsStore::default();
        let request = store.begin_inventory(7).expect("request");
        assert!(store.finish_inventory(
            request,
            7,
            Ok(inventory(
                vec![
                    item("orders", TopicCategory::Application),
                    item("payments", TopicCategory::Application)
                ],
                TopicCompleteness::Complete,
            ))
        ));
        store.draft_filter.keyword = "orders".into();
        assert_eq!(store.visible_page().total, 2);
        store.search();
        assert_eq!(store.visible_page().total, 1);
        store.reset_filters();
        assert_eq!(store.visible_page().total, 2);
    }

    #[test]
    fn page_size_is_ten_and_requested_page_is_clamped() {
        let mut store = TopicsStore::default();
        let request = store.begin_inventory(1).expect("request");
        let items = (0..21)
            .map(|index| item(&format!("topic-{index:02}"), TopicCategory::Application))
            .collect();
        assert!(store.finish_inventory(request, 1, Ok(inventory(items, TopicCompleteness::Complete))));
        store.page = 99;
        let page = store.visible_page();
        assert_eq!(page.page_count, 3);
        assert_eq!(page.page, 3);
        assert_eq!(page.items.len(), 1);
    }

    #[test]
    fn partial_catalog_counts_are_lower_bounds_or_unknown() {
        let mut store = TopicsStore::default();
        let request = store.begin_inventory(1).expect("request");
        assert!(store.finish_inventory(
            request,
            1,
            Ok(inventory(
                vec![item("orders", TopicCategory::Application)],
                TopicCompleteness::Partial {
                    successful_target_count: 1,
                    failed_target_count: 1,
                },
            ))
        ));
        let counts = store.category_counts();
        assert_eq!(counts[&TopicCategory::Application], TopicCountEvidence::LowerBound(1));
        assert_eq!(counts[&TopicCategory::System], TopicCountEvidence::Unknown);
    }

    #[test]
    fn deep_link_uses_full_inventory_and_missing_selection_remains_pending() {
        let mut store = TopicsStore::default();
        let request = store.begin_inventory(1).expect("request");
        let items = (0..12)
            .map(|index| item(&format!("topic-{index:02}"), TopicCategory::Application))
            .collect::<Vec<_>>();
        assert!(store.finish_inventory(request, 1, Ok(inventory(items, TopicCompleteness::Complete))));
        let route = store.open_route(TopicIdentity::parse("topic-11").expect("topic"), TopicTab::Stats);
        assert!(matches!(
            route,
            Some(AppRoute::TopicDetail {
                tab: TopicTab::Stats,
                ..
            })
        ));
        store.close_detail();
        assert!(store.take_focus_restore());
        assert!(
            store
                .open_route(TopicIdentity::parse("missing").expect("topic"), TopicTab::Overview)
                .is_none()
        );
    }

    #[test]
    fn partial_inventory_missing_the_current_topic_retains_it_as_unverified() {
        let mut store = TopicsStore::default();
        store.replace_inventory(inventory(
            vec![item("orders", TopicCategory::Application)],
            TopicCompleteness::Complete,
        ));
        store
            .open(item("orders", TopicCategory::Application), TopicTab::Overview)
            .expect("route");

        store.replace_inventory(partial_inventory(vec![item("payments", TopicCategory::Application)]));

        let selected = &store.detail.as_ref().expect("selection retained").selected;
        assert_eq!(selected.item.identity.as_str(), "orders");
        assert!(selected.stale);
        assert!(!selected.inventory_verified());
        assert!(matches!(
            selected.inventory_completeness,
            TopicCompleteness::Partial { .. }
        ));
        assert_eq!(selected.inventory_failures.len(), 1);
    }

    #[test]
    fn partial_inventory_containing_the_current_topic_updates_but_does_not_verify_it() {
        let mut store = TopicsStore::default();
        store.replace_inventory(inventory(
            vec![item("orders", TopicCategory::Application)],
            TopicCompleteness::Complete,
        ));
        store
            .open(item("orders", TopicCategory::Application), TopicTab::Overview)
            .expect("route");
        let mut refreshed = item("orders", TopicCategory::Application);
        refreshed.read_queue_count = Some(16);

        store.replace_inventory(partial_inventory(vec![refreshed]));

        let selected = &store.detail.as_ref().expect("selection retained").selected;
        assert_eq!(selected.item.read_queue_count, Some(16));
        assert!(selected.stale);
        assert!(!selected.inventory_verified());
        assert_eq!(selected.inventory_failures[0].target, "broker-b");

        store.replace_inventory(inventory(
            vec![item("orders", TopicCategory::Application)],
            TopicCompleteness::Complete,
        ));
        assert!(store.detail.as_ref().expect("selection").selected.inventory_verified());
    }

    #[test]
    fn independent_overview_inventory_updates_evidence_only_for_the_current_request() {
        let mut detail = TopicDetailStore::new(item("orders", TopicCategory::Application), TopicTab::Overview);
        let stale = detail.begin_overview(7).expect("stale request");
        let current = detail.begin_overview(7).expect("current request");
        let mut partial_item = item("orders", TopicCategory::Application);
        partial_item.read_queue_count = Some(16);

        assert!(!detail.finish_overview(stale, 7, Ok(partial_inventory(vec![partial_item.clone()]))));
        assert!(detail.selected.inventory_verified());
        assert_eq!(detail.selected.item.read_queue_count, Some(8));

        assert!(detail.finish_overview(current, 7, Ok(partial_inventory(vec![partial_item]))));
        assert!(matches!(detail.overview.state, Loadable::Ready(_)));
        assert_eq!(detail.selected.item.read_queue_count, Some(16));
        assert!(detail.selected.stale);
        assert_eq!(detail.selected.inventory_failures[0].target, "broker-b");

        let complete = detail.begin_overview(7).expect("complete request");
        let mut complete_item = item("orders", TopicCategory::Application);
        complete_item.read_queue_count = Some(20);
        assert!(detail.finish_overview(
            complete,
            7,
            Ok(inventory(vec![complete_item], TopicCompleteness::Complete))
        ));
        assert!(detail.selected.inventory_verified());
        assert_eq!(detail.selected.item.read_queue_count, Some(20));

        let missing = detail.begin_overview(7).expect("missing request");
        assert!(detail.finish_overview(missing, 7, Ok(inventory(Vec::new(), TopicCompleteness::Complete))));
        assert!(matches!(detail.overview.state, Loadable::Empty));
        assert!(detail.selected.stale);
        assert!(detail.selected.inventory_completeness.is_complete());
    }

    #[test]
    fn detail_selection_evidence_sync_requires_the_current_identity() {
        let mut store = TopicsStore::default();
        let orders = item("orders", TopicCategory::Application);
        store.replace_inventory(inventory(vec![orders.clone()], TopicCompleteness::Complete));
        store.open(orders, TopicTab::Overview).expect("route");

        let mut evidence = store.detail.as_ref().expect("selection").selected.clone();
        evidence.inventory_completeness = TopicCompleteness::Partial {
            successful_target_count: 1,
            failed_target_count: 1,
        };
        evidence.inventory_failures = partial_inventory(Vec::new()).failures;
        evidence.stale = true;
        let payments = TopicIdentity::parse("payments").expect("topic");
        assert!(!store.sync_detail_selection_evidence(&payments, evidence.clone()));
        assert!(store.detail.as_ref().expect("selection").selected.inventory_verified());

        let orders = TopicIdentity::parse("orders").expect("topic");
        assert!(store.sync_detail_selection_evidence(&orders, evidence));
        let selected = &store.detail.as_ref().expect("selection").selected;
        assert!(!selected.inventory_verified());
        assert_eq!(selected.inventory_failures[0].target, "broker-b");
    }

    #[test]
    fn five_tab_resources_are_lazy_independent_and_stale_revision_is_rejected() {
        let mut detail = TopicDetailStore::new(item("orders", TopicCategory::Application), TopicTab::Stats);
        assert!(detail.active_is_idle());
        let stale = detail.begin_stats(3).expect("stale");
        let current = detail.begin_stats(3).expect("current");
        assert!(!detail.finish_stats(
            stale,
            3,
            Err(UiError::new("stale", crate::state::UiErrorCode::Connection, true))
        ));
        assert!(!detail.finish_stats(
            current,
            4,
            Err(UiError::new(
                "wrong revision",
                crate::state::UiErrorCode::Connection,
                true
            ))
        ));
        assert!(matches!(detail.route.state, Loadable::Idle));
        assert!(matches!(detail.configuration.state, Loadable::Idle));
        assert!(matches!(detail.consumers.state, Loadable::Idle));
        detail.set_tab(TopicTab::Consumers);
        assert!(detail.active_is_idle());
    }

    #[test]
    fn consumers_complete_empty_partial_failure_and_retry_use_distinct_epoch_safe_states() {
        let mut detail = TopicDetailStore::new(item("orders", TopicCategory::Application), TopicTab::Consumers);
        let complete = detail.begin_consumers(3).expect("complete request");
        assert!(detail.finish_consumers(
            complete,
            3,
            Ok(TopicConsumersView {
                topic: TopicIdentity::parse("orders").expect("topic"),
                items: Vec::new(),
                completeness: TopicCompleteness::Complete,
                failures: Vec::new(),
            })
        ));
        assert!(matches!(detail.consumers.state, Loadable::Empty));

        let stale_retry = detail.begin_consumers(3).expect("stale retry");
        let current_retry = detail.begin_consumers(3).expect("current retry");
        let partial = TopicConsumersView {
            topic: TopicIdentity::parse("orders").expect("topic"),
            items: Vec::new(),
            completeness: TopicCompleteness::Partial {
                successful_target_count: 1,
                failed_target_count: 1,
            },
            failures: vec![TopicTargetFailure {
                target: "broker-b".into(),
                stage: TopicFailureStage::Consumer,
                code: TopicFailureCode::Unavailable,
                retryable: true,
            }],
        };
        assert!(!detail.finish_consumers(stale_retry, 3, Ok(partial.clone())));
        assert!(detail.finish_consumers(current_retry, 3, Ok(partial)));
        assert!(matches!(detail.consumers.state, Loadable::Ready(_)));

        let failed = detail.begin_consumers(3).expect("failed refresh");
        assert!(detail.finish_consumers(
            failed,
            3,
            Err(UiError::new(
                "consumer refresh failed",
                crate::state::UiErrorCode::Connection,
                true,
            ))
        ));
        assert!(matches!(
            detail.consumers.state,
            Loadable::Failed { previous: Some(_), .. }
        ));
    }
}
