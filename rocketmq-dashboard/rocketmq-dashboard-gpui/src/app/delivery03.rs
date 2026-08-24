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

//! Delivery 03 page wiring and typed Dashboard/Broker route synchronization.

use gpui::{Context, Subscription, Window};

use super::{LegacyPageCache, RocketmqDashboard};
use crate::{
    features::{brokers::BrokersIntent, dashboard::DashboardIntent},
    route::AppRoute,
};

impl RocketmqDashboard {
    pub(super) fn delivery03_subscriptions(
        pages: &LegacyPageCache,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> [Subscription; 2] {
        [
            cx.subscribe_in(
                &pages.dashboard,
                window,
                |this, _, event: &DashboardIntent, window, cx| match event {
                    DashboardIntent::Navigate(route) => this.navigate(route.clone(), window, cx),
                },
            ),
            cx.subscribe_in(&pages.brokers, window, |this, _, event: &BrokersIntent, window, cx| {
                this.handle_brokers_intent(event.clone(), window, cx);
            }),
        ]
    }

    pub(super) fn sync_broker_route(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let AppRoute::BrokerDetail { broker, tab } = self.history.current().clone() {
            self.legacy_pages
                .brokers
                .update(cx, |brokers, cx| brokers.open_route(broker.as_str(), tab, window, cx));
        } else {
            self.legacy_pages
                .brokers
                .update(cx, |brokers, cx| brokers.close_detail(window, cx));
        }
    }

    pub(super) fn handle_brokers_intent(&mut self, event: BrokersIntent, _window: &mut Window, cx: &mut Context<Self>) {
        match event {
            BrokersIntent::Navigate(route) => {
                // The Broker view already opened the typed Sheet. Updating route history here must
                // not close that Sheet as a generic page navigation would.
                self.history.navigate(route);
            }
            BrokersIntent::ReplaceRoute(route) => self.history.replace(route),
            BrokersIntent::SheetClosed => {
                if matches!(self.history.current(), AppRoute::BrokerDetail { .. }) {
                    self.history.replace(AppRoute::Brokers);
                }
            }
            BrokersIntent::ConfigApplied(invalidations) => {
                self.legacy_pages
                    .dashboard
                    .update(cx, |dashboard, cx| dashboard.consume_invalidations(&invalidations, cx));
            }
        }
        cx.notify();
    }
}
