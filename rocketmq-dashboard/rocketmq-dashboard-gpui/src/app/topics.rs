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

//! Topic entity subscriptions and typed Sheet/history synchronization.

use gpui::{Context, Subscription, Window};

use super::{LegacyPageCache, RocketmqDashboard};
use crate::{features::topics::TopicsIntent, route::AppRoute};

impl RocketmqDashboard {
    pub(super) fn topic_subscriptions(
        pages: &LegacyPageCache,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> [Subscription; 1] {
        [
            cx.subscribe_in(&pages.topics, window, |this, _, event: &TopicsIntent, window, cx| {
                this.handle_topics_intent(event.clone(), window, cx);
            }),
        ]
    }

    pub(super) fn sync_topic_route(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let AppRoute::TopicDetail { topic, tab } = self.history.current().clone() {
            self.legacy_pages
                .topics
                .update(cx, |topics, cx| topics.open_route(topic.as_str(), tab, window, cx));
        } else if matches!(self.history.current(), AppRoute::Topics) {
            self.legacy_pages.topics.update(cx, |topics, cx| {
                topics.close_detail(window, cx);
                topics.ensure_loaded(window, cx);
            });
        } else {
            self.legacy_pages
                .topics
                .update(cx, |topics, cx| topics.close_detail(window, cx));
        }
    }

    fn handle_topics_intent(&mut self, event: TopicsIntent, window: &mut Window, cx: &mut Context<Self>) {
        match event {
            TopicsIntent::Navigate(route @ AppRoute::TopicDetail { .. }) => {
                // The row already opened the Sheet; retain it while only publishing history.
                self.history.navigate(route);
            }
            TopicsIntent::Navigate(route) => self.navigate(route, window, cx),
            TopicsIntent::ReplaceRoute(route) => self.history.replace(route),
            TopicsIntent::SheetClosed => {
                if matches!(self.history.current(), AppRoute::TopicDetail { .. }) {
                    self.history.replace(AppRoute::Topics);
                }
            }
        }
        cx.notify();
    }
}
