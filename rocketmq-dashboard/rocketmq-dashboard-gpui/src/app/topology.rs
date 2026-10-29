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

use super::{LegacyPageCache, RocketmqDashboard};
use crate::{features::topology::TopologyIntent, route::AppRoute};
use gpui::{Context, Subscription, Window};

impl RocketmqDashboard {
    pub(super) fn topology_subscription(
        pages: &LegacyPageCache,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Subscription {
        cx.subscribe_in(&pages.topology, window, |this, _, event, window, cx| match event {
            TopologyIntent::Navigate(route) => this.navigate(route.clone(), window, cx),
            TopologyIntent::Producer(group) => {
                this.navigate(AppRoute::Producers, window, cx);
                this.legacy_pages
                    .producers
                    .update(cx, |view, cx| view.select_topology_group(group.clone(), window, cx));
            }
        })
    }
}
