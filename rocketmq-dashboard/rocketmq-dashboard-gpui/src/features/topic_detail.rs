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

//! Five-tab Topic Sheet with independent lazy resources.

#[path = "topic_detail_render.rs"]
mod render;

use gpui::{Context, EventEmitter, Task};
use rocketmq_dashboard_common::{
    TopicCategory, TopicCompleteness, TopicConfigField, TopicConfigTargetView, TopicConfigView, TopicConsumersView,
    TopicFailureCode, TopicFailureStage, TopicIdentity, TopicInventoryItem, TopicMessageType, TopicPermission,
    TopicTargetFailure, TopicTargetIdentity,
};

use crate::{
    components::key_value,
    features::topics_store::{SelectedTopic, TopicDetailStore},
    route::{AppRoute, ConsumerTab, RouteKey, TopicTab},
    services::{AppServices, topics::TopicCacheInvalidation},
    state::Loadable,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopicDetailIntent {
    SelectionEvidenceUpdated {
        revision: u64,
        topic: TopicIdentity,
        selected: SelectedTopic,
    },
    ReplaceRoute(AppRoute),
    NavigateConsumer(AppRoute),
    EditTarget {
        target: TopicTargetIdentity,
        expected_version: u64,
        read_queue_count: u32,
        write_queue_count: u32,
    },
    Send(TopicIdentity),
    DeleteTopic {
        topic: TopicIdentity,
        clusters: Vec<String>,
    },
    DeleteBroker {
        topic: TopicIdentity,
        target: TopicTargetIdentity,
    },
    ResetOffset {
        topic: TopicIdentity,
        consumer_group: String,
        clusters: Vec<String>,
    },
    SkipAccumulated {
        topic: TopicIdentity,
        consumer_group: String,
        clusters: Vec<String>,
    },
}

pub struct TopicDetail {
    services: AppServices,
    revision: u64,
    pub store: TopicDetailStore,
    overview_task: Option<Task<()>>,
    stats_task: Option<Task<()>>,
    route_task: Option<Task<()>>,
    config_task: Option<Task<()>>,
    consumers_task: Option<Task<()>>,
}

impl EventEmitter<TopicDetailIntent> for TopicDetail {}

impl TopicDetail {
    #[cfg(test)]
    pub fn new(
        services: AppServices,
        revision: u64,
        item: TopicInventoryItem,
        tab: TopicTab,
        cx: &mut Context<Self>,
    ) -> Self {
        Self::new_with_store(services, revision, TopicDetailStore::new(item, tab), cx)
    }

    pub fn new_with_selection(
        services: AppServices,
        revision: u64,
        selected: SelectedTopic,
        tab: TopicTab,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut store = TopicDetailStore::new(selected.item.clone(), tab);
        store.selected = selected;
        Self::new_with_store(services, revision, store, cx)
    }

    fn new_with_store(services: AppServices, revision: u64, store: TopicDetailStore, cx: &mut Context<Self>) -> Self {
        let mut detail = Self {
            services,
            revision,
            store,
            overview_task: None,
            stats_task: None,
            route_task: None,
            config_task: None,
            consumers_task: None,
        };
        detail.refresh_active(cx);
        detail
    }

    pub fn set_revision(&mut self, revision: u64, cx: &mut Context<Self>) {
        if revision != self.revision {
            self.revision = revision;
            self.store.mark_stale();
            cx.notify();
        }
    }

    pub fn set_tab(&mut self, tab: TopicTab, cx: &mut Context<Self>) {
        self.store.set_tab(tab);
        self.refresh_active(cx);
        cx.notify();
    }

    pub fn sync_inventory_selection(&mut self, selected: SelectedTopic, cx: &mut Context<Self>) {
        self.store.selected = selected;
        if !self.store.selected.inventory_verified() {
            self.store.mark_stale();
        } else {
            self.refresh_active(cx);
        }
        cx.notify();
    }

    pub fn apply_mutation_reload(
        &mut self,
        invalidations: &[TopicCacheInvalidation],
        mut configuration: Option<TopicConfigView>,
        mut consumers: Option<TopicConsumersView>,
        failure: Option<&crate::state::UiError>,
        cx: &mut Context<Self>,
    ) {
        let selected = &self.store.selected.item.identity;
        for invalidation in invalidations {
            let slot_failure = |error: Option<&crate::state::UiError>| error.cloned();
            match invalidation {
                TopicCacheInvalidation::Inventory => {}
                TopicCacheInvalidation::Overview(topic) if topic == selected => {
                    if let Some(error) = slot_failure(failure) {
                        self.store.overview.clear_with_error(error);
                    } else {
                        self.store.overview.clear();
                    }
                }
                TopicCacheInvalidation::Stats(topic) if topic == selected => {
                    if let Some(error) = slot_failure(failure) {
                        self.store.stats.clear_with_error(error);
                    } else {
                        self.store.stats.clear();
                    }
                }
                TopicCacheInvalidation::Route(topic) if topic == selected => {
                    if let Some(error) = slot_failure(failure) {
                        self.store.route.clear_with_error(error);
                    } else {
                        self.store.route.clear();
                    }
                }
                TopicCacheInvalidation::Configuration(topic) if topic == selected => {
                    if let Some(configuration) = configuration.take() {
                        let present = !configuration.targets.is_empty()
                            || !configuration.completeness.is_complete()
                            || !configuration.failures.is_empty();
                        self.store
                            .configuration
                            .replace_optional(present.then_some(configuration));
                    } else if let Some(error) = slot_failure(failure) {
                        self.store.configuration.clear_with_error(error);
                    } else {
                        self.store.configuration.clear();
                    }
                }
                TopicCacheInvalidation::Consumers(topic) if topic == selected => {
                    if let Some(consumers) = consumers.take() {
                        let present = !consumers.items.is_empty()
                            || !consumers.completeness.is_complete()
                            || !consumers.failures.is_empty();
                        self.store.consumers.replace_optional(present.then_some(consumers));
                    } else if let Some(error) = slot_failure(failure) {
                        self.store.consumers.clear_with_error(error);
                    } else {
                        self.store.consumers.clear();
                    }
                }
                TopicCacheInvalidation::Overview(_)
                | TopicCacheInvalidation::Stats(_)
                | TopicCacheInvalidation::Route(_)
                | TopicCacheInvalidation::Configuration(_)
                | TopicCacheInvalidation::Consumers(_) => {}
            }
        }
        if failure.is_none() {
            self.refresh_active(cx);
        }
        cx.notify();
    }

    fn refresh_active(&mut self, cx: &mut Context<Self>) {
        if !self.store.active_is_idle() && !self.store.selected.stale {
            return;
        }
        match self.store.active_tab {
            TopicTab::Overview => self.refresh_overview(cx),
            TopicTab::Stats => self.refresh_stats(cx),
            TopicTab::Route => self.refresh_route(cx),
            TopicTab::Configuration => self.refresh_configuration(cx),
            TopicTab::Consumers => self.refresh_consumers(cx),
        }
    }

    fn refresh_overview(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.begin_overview(self.revision) else {
            return;
        };
        let services = self.services.clone();
        self.overview_task = Some(cx.spawn(async move |this, cx| {
            let result = services.topic_inventory(request.scope).await;
            let _ = this.update(cx, |detail, cx| {
                let has_selection_evidence = result.is_ok();
                if detail.store.finish_overview(request, detail.revision, result) && has_selection_evidence {
                    let selected = detail.store.selected.clone();
                    cx.emit(TopicDetailIntent::SelectionEvidenceUpdated {
                        revision: detail.revision,
                        topic: selected.item.identity.clone(),
                        selected,
                    });
                }
                cx.notify();
            });
        }));
    }

    fn refresh_stats(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.begin_stats(self.revision) else {
            return;
        };
        let services = self.services.clone();
        let topic = self.store.selected.item.identity.clone();
        self.stats_task = Some(cx.spawn(async move |this, cx| {
            let result = services.topic_stats(request.scope, topic).await;
            let _ = this.update(cx, |detail, cx| {
                detail.store.finish_stats(request, detail.revision, result);
                cx.notify();
            });
        }));
    }

    fn refresh_route(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.begin_route(self.revision) else {
            return;
        };
        let services = self.services.clone();
        let topic = self.store.selected.item.identity.clone();
        self.route_task = Some(cx.spawn(async move |this, cx| {
            let result = services.topic_route(request.scope, topic).await;
            let _ = this.update(cx, |detail, cx| {
                detail.store.finish_route(request, detail.revision, result);
                cx.notify();
            });
        }));
    }

    fn refresh_configuration(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.begin_configuration(self.revision) else {
            return;
        };
        let services = self.services.clone();
        let topic = self.store.selected.item.identity.clone();
        self.config_task = Some(cx.spawn(async move |this, cx| {
            let result = services.topic_config(request.scope, topic).await;
            let _ = this.update(cx, |detail, cx| {
                detail.store.finish_configuration(request, detail.revision, result);
                cx.notify();
            });
        }));
    }

    fn refresh_consumers(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.begin_consumers(self.revision) else {
            return;
        };
        let services = self.services.clone();
        let topic = self.store.selected.item.identity.clone();
        self.consumers_task = Some(cx.spawn(async move |this, cx| {
            let result = services.topic_consumers(request.scope, topic).await;
            let _ = this.update(cx, |detail, cx| {
                detail.store.finish_consumers(request, detail.revision, result);
                cx.notify();
            });
        }));
    }

    fn select_tab(&mut self, tab: TopicTab, cx: &mut Context<Self>) {
        self.set_tab(tab, cx);
        if let Ok(topic) = RouteKey::parse(self.store.selected.item.identity.as_str().to_owned()) {
            cx.emit(TopicDetailIntent::ReplaceRoute(AppRoute::TopicDetail { topic, tab }));
        }
    }

    fn retry(&mut self, tab: TopicTab, cx: &mut Context<Self>) {
        match tab {
            TopicTab::Overview => self.refresh_overview(cx),
            TopicTab::Stats => self.refresh_stats(cx),
            TopicTab::Route => self.refresh_route(cx),
            TopicTab::Configuration => self.refresh_configuration(cx),
            TopicTab::Consumers => self.refresh_consumers(cx),
        }
        cx.notify();
    }

    #[cfg(test)]
    pub(crate) fn retry_for_test(&mut self, tab: TopicTab, cx: &mut Context<Self>) {
        self.retry(tab, cx);
    }
}
