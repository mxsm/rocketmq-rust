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

//! Consumer entity subscriptions and Sheet/history synchronization.

use gpui::{Context, Subscription, Window};

use super::{LegacyPageCache, RocketmqDashboard};
use crate::{features::consumers::ConsumersIntent, route::AppRoute};

impl RocketmqDashboard {
    pub(super) fn consumer_subscriptions(
        pages: &LegacyPageCache,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> [Subscription; 1] {
        [cx.subscribe_in(
            &pages.consumers,
            window,
            |this, _, event: &ConsumersIntent, window, cx| {
                this.handle_consumers_intent(event.clone(), window, cx);
            },
        )]
    }

    pub(super) fn sync_consumer_route(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !matches!(self.history.current(), AppRoute::Producers) {
            self.legacy_pages
                .producers
                .update(cx, |view, cx| view.close_owned_sheet(window, cx));
        }
        match self.history.current().clone() {
            AppRoute::ConsumerDetail { group, tab } => {
                self.legacy_pages
                    .consumers
                    .update(cx, |view, cx| view.open_route(group.as_str(), tab, window, cx));
                self.legacy_pages
                    .producers
                    .update(cx, |view, cx| view.ensure_loaded(window, cx));
            }
            AppRoute::Consumers => {
                self.legacy_pages.consumers.update(cx, |view, cx| {
                    view.close_detail(window, cx);
                    view.ensure_loaded(window, cx);
                });
            }
            AppRoute::Producers => {
                self.legacy_pages
                    .consumers
                    .update(cx, |view, cx| view.close_detail(window, cx));
                self.legacy_pages
                    .producers
                    .update(cx, |view, cx| view.ensure_loaded(window, cx));
            }
            _ => self
                .legacy_pages
                .consumers
                .update(cx, |view, cx| view.close_detail(window, cx)),
        }
    }

    fn handle_consumers_intent(&mut self, event: ConsumersIntent, window: &mut Window, cx: &mut Context<Self>) {
        match event {
            ConsumersIntent::Navigate(route @ AppRoute::ConsumerDetail { .. }) => self.history.navigate(route),
            ConsumersIntent::Navigate(route) => self.navigate(route, window, cx),
            ConsumersIntent::ReplaceRoute(route) => self.history.replace(route),
            ConsumersIntent::SheetClosed => {
                if matches!(self.history.current(), AppRoute::ConsumerDetail { .. }) {
                    self.history.replace(AppRoute::Consumers);
                }
            }
        }
        cx.notify();
    }
}
