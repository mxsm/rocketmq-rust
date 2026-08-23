// Copyright 2025 The RocketMQ Rust Authors
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

//! Typed navigation data for the official component Sidebar.

use gpui_component::IconName;

use crate::route::AppRoute;

/// A typed navigation item, avoiding label or path comparisons in UI event handling.
#[derive(Clone)]
pub struct SidebarItem {
    /// The route emitted when the item is activated.
    pub route: AppRoute,
    /// Accessible visual label.
    pub label: &'static str,
    /// Official bundled icon.
    pub icon: IconName,
}

/// A visual group in the navigation sidebar.
#[derive(Clone)]
pub struct SidebarGroup {
    /// Group heading.
    pub label: &'static str,
    /// Items in display order.
    pub items: Vec<SidebarItem>,
}

/// Returns the three fixed groups of navigable delivery-one routes.
pub fn navigation_groups() -> Vec<SidebarGroup> {
    vec![
        SidebarGroup {
            label: "Overview",
            items: vec![
                SidebarItem {
                    route: AppRoute::Dashboard,
                    label: "Dashboard",
                    icon: IconName::LayoutDashboard,
                },
                SidebarItem {
                    route: AppRoute::Brokers,
                    label: "Brokers",
                    icon: IconName::Building2,
                },
            ],
        },
        SidebarGroup {
            label: "Messaging",
            items: vec![
                SidebarItem {
                    route: AppRoute::Topics,
                    label: "Topics",
                    icon: IconName::Inbox,
                },
                SidebarItem {
                    route: AppRoute::Consumers,
                    label: "Consumers",
                    icon: IconName::CircleUser,
                },
                SidebarItem {
                    route: AppRoute::Producers,
                    label: "Producers",
                    icon: IconName::User,
                },
                SidebarItem {
                    route: AppRoute::Messages,
                    label: "Messages",
                    icon: IconName::Search,
                },
            ],
        },
        SidebarGroup {
            label: "Operations",
            items: vec![
                SidebarItem {
                    route: AppRoute::DlqMessages,
                    label: "DLQ Messages",
                    icon: IconName::TriangleAlert,
                },
                SidebarItem {
                    route: AppRoute::MessageTrace,
                    label: "Message Trace",
                    icon: IconName::Map,
                },
                SidebarItem {
                    route: AppRoute::Acl,
                    label: "Access Control",
                    icon: IconName::User,
                },
                SidebarItem {
                    route: AppRoute::Monitors,
                    label: "Monitors",
                    icon: IconName::Bell,
                },
                SidebarItem {
                    route: AppRoute::OpsSettings,
                    label: "Operations Settings",
                    icon: IconName::Settings,
                },
                SidebarItem {
                    route: AppRoute::Proxy,
                    label: "Proxy",
                    icon: IconName::PanelRight,
                },
            ],
        },
    ]
}

/// Checks active navigation using route variants, including details that belong to a list page.
pub const fn is_active(item: &SidebarItem, current: &AppRoute) -> bool {
    matches!(
        (&item.route, current),
        (AppRoute::Dashboard, AppRoute::Dashboard)
            | (AppRoute::Brokers, AppRoute::Brokers | AppRoute::BrokerDetail { .. })
            | (AppRoute::Topics, AppRoute::Topics | AppRoute::TopicDetail { .. })
            | (
                AppRoute::Consumers,
                AppRoute::Consumers | AppRoute::ConsumerDetail { .. }
            )
            | (AppRoute::Producers, AppRoute::Producers)
            | (AppRoute::Messages, AppRoute::Messages)
            | (AppRoute::DlqMessages, AppRoute::DlqMessages)
            | (AppRoute::MessageTrace, AppRoute::MessageTrace)
            | (AppRoute::Acl, AppRoute::Acl)
            | (AppRoute::Monitors, AppRoute::Monitors)
            | (AppRoute::OpsSettings, AppRoute::OpsSettings)
            | (AppRoute::Proxy, AppRoute::Proxy)
    )
}

#[cfg(test)]
mod tests {
    use super::{is_active, navigation_groups};
    use crate::route::{AppRoute, BrokerTab, RouteKey};

    #[test]
    fn broker_detail_selects_the_typed_broker_navigation_item() {
        let broker = navigation_groups()
            .into_iter()
            .flat_map(|group| group.items)
            .find(|item| item.route == AppRoute::Brokers)
            .expect("Broker navigation item must exist");
        let current = AppRoute::BrokerDetail {
            broker: RouteKey::parse("broker-a").expect("valid broker key"),
            tab: BrokerTab::Overview,
        };

        assert!(is_active(&broker, &current));
    }
}
