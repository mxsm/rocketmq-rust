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

//! Typed navigation for the dashboard shell.

use std::{fmt, str::FromStr};

/// A validated path parameter used by detail routes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RouteKey(String);

impl RouteKey {
    /// Validates a route key without accepting a value that could change URL structure.
    pub fn parse(value: impl Into<String>) -> Result<Self, RouteParseError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 256
            || value
                .chars()
                .any(|character| matches!(character, '/' | '?' | '#' | '\\') || character.is_control())
        {
            return Err(RouteParseError::InvalidParameter);
        }

        Ok(Self(value))
    }

    /// Returns the encoded-as-validated path value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The selected Broker detail section.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BrokerTab {
    /// Broker summary and reachability state.
    Overview,
    /// Broker runtime values.
    Runtime,
    /// Broker configuration values.
    Configuration,
}

/// The selected Topic detail section.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TopicTab {
    /// Topic summary.
    Overview,
    /// Queue details.
    Queues,
    /// Permission details.
    Permissions,
}

/// The selected Consumer detail section.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConsumerTab {
    /// Consumer group summary.
    Overview,
    /// Subscription details.
    Subscriptions,
    /// Client connections.
    Connections,
    /// Offset details.
    Offsets,
    /// Monitoring details.
    Monitoring,
}

/// Every route exposed by Delivery 01.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum AppRoute {
    /// Authentication form.
    Login,
    /// Dashboard landing page.
    Dashboard,
    /// Broker list.
    Brokers,
    /// A broker detail page.
    BrokerDetail { broker: RouteKey, tab: BrokerTab },
    /// Topic list.
    Topics,
    /// A topic detail page.
    TopicDetail { topic: RouteKey, tab: TopicTab },
    /// Consumer list.
    Consumers,
    /// A consumer detail page.
    ConsumerDetail { group: RouteKey, tab: ConsumerTab },
    /// Producer list.
    Producers,
    /// Message search.
    Messages,
    /// Dead-letter message search.
    DlqMessages,
    /// Message trace lookup.
    MessageTrace,
    /// Access-control management.
    Acl,
    /// Monitoring rules.
    Monitors,
    /// Operations settings.
    OpsSettings,
    /// Proxy settings.
    Proxy,
}

impl AppRoute {
    /// Parses a path. Compatibility aliases are accepted only at this boundary.
    pub fn parse(path: &str) -> Result<Self, RouteParseError> {
        let path = path.split_once('?').map_or(path, |(path, _)| path);
        let segments: Vec<_> = path.split('/').filter(|segment| !segment.is_empty()).collect();

        match segments.as_slice() {
            [] => Ok(Self::Dashboard),
            ["login"] => Ok(Self::Login),
            ["dashboard"] => Ok(Self::Dashboard),
            ["nameservers"] => Ok(Self::OpsSettings),
            ["brokers"] => Ok(Self::Brokers),
            ["brokers", broker, tab] => Ok(Self::BrokerDetail {
                broker: RouteKey::parse(*broker)?,
                tab: parse_broker_tab(tab)?,
            }),
            ["topics"] => Ok(Self::Topics),
            ["topics", topic, tab] => Ok(Self::TopicDetail {
                topic: RouteKey::parse(*topic)?,
                tab: parse_topic_tab(tab)?,
            }),
            ["consumers"] => Ok(Self::Consumers),
            ["consumers", group, tab] => Ok(Self::ConsumerDetail {
                group: RouteKey::parse(*group)?,
                tab: parse_consumer_tab(tab)?,
            }),
            ["producers"] => Ok(Self::Producers),
            ["messages"] => Ok(Self::Messages),
            ["dlq-messages"] => Ok(Self::DlqMessages),
            ["message-trace"] => Ok(Self::MessageTrace),
            ["acl"] => Ok(Self::Acl),
            ["monitors"] => Ok(Self::Monitors),
            ["ops-settings"] => Ok(Self::OpsSettings),
            ["proxy"] => Ok(Self::Proxy),
            // Legacy input compatibility is intentionally confined to parsing.
            ["ops"] => Ok(Self::OpsSettings),
            ["cluster"] => Ok(Self::Brokers),
            ["dlq"] => Ok(Self::DlqMessages),
            _ => Err(RouteParseError::UnknownPath),
        }
    }

    /// Formats the canonical path. Aliases are never emitted.
    pub fn format_path(&self) -> String {
        match self {
            Self::Login => "/login".to_owned(),
            Self::Dashboard => "/dashboard".to_owned(),
            Self::Brokers => "/brokers".to_owned(),
            Self::BrokerDetail { broker, tab } => {
                format!("/brokers/{}/{tab}", broker.as_str())
            }
            Self::Topics => "/topics".to_owned(),
            Self::TopicDetail { topic, tab } => format!("/topics/{}/{tab}", topic.as_str()),
            Self::Consumers => "/consumers".to_owned(),
            Self::ConsumerDetail { group, tab } => format!("/consumers/{}/{tab}", group.as_str()),
            Self::Producers => "/producers".to_owned(),
            Self::Messages => "/messages".to_owned(),
            Self::DlqMessages => "/dlq-messages".to_owned(),
            Self::MessageTrace => "/message-trace".to_owned(),
            Self::Acl => "/acl".to_owned(),
            Self::Monitors => "/monitors".to_owned(),
            Self::OpsSettings => "/ops-settings".to_owned(),
            Self::Proxy => "/proxy".to_owned(),
        }
    }

    /// Returns a user-facing title without asking a page to inspect a route string.
    pub const fn title(&self) -> &'static str {
        match self {
            Self::Login => "Sign in",
            Self::Dashboard => "Dashboard",
            Self::Brokers | Self::BrokerDetail { .. } => "Brokers",
            Self::Topics | Self::TopicDetail { .. } => "Topics",
            Self::Consumers | Self::ConsumerDetail { .. } => "Consumers",
            Self::Producers => "Producers",
            Self::Messages => "Messages",
            Self::DlqMessages => "DLQ Messages",
            Self::MessageTrace => "Message Trace",
            Self::Acl => "Access Control",
            Self::Monitors => "Monitors",
            Self::OpsSettings => "Operations Settings",
            Self::Proxy => "Proxy",
        }
    }
}

impl fmt::Display for AppRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.format_path())
    }
}

impl FromStr for AppRoute {
    type Err = RouteParseError;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        Self::parse(path)
    }
}

impl fmt::Display for BrokerTab {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Overview => "overview",
            Self::Runtime => "runtime",
            Self::Configuration => "configuration",
        })
    }
}

impl fmt::Display for TopicTab {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Overview => "overview",
            Self::Queues => "queues",
            Self::Permissions => "permissions",
        })
    }
}

impl fmt::Display for ConsumerTab {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Overview => "overview",
            Self::Subscriptions => "subscriptions",
            Self::Connections => "connections",
            Self::Offsets => "offsets",
            Self::Monitoring => "monitoring",
        })
    }
}

/// A parse failure that contains no source value, preserving safe diagnostics for malformed links.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RouteParseError {
    /// The path is not a supported dashboard route.
    #[error("unknown dashboard route")]
    UnknownPath,
    /// A detail path does not have a valid parameter or tab.
    #[error("invalid dashboard route parameter")]
    InvalidParameter,
}

/// Browser-like in-memory history for typed routes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NavigationHistory {
    back: Vec<AppRoute>,
    current: AppRoute,
    forward: Vec<AppRoute>,
}

impl NavigationHistory {
    /// Creates a history with one current route.
    pub fn new(current: AppRoute) -> Self {
        Self {
            back: Vec::new(),
            current,
            forward: Vec::new(),
        }
    }

    /// Returns the visible route.
    pub const fn current(&self) -> &AppRoute {
        &self.current
    }

    /// Returns the route that Back would select without mutating history.
    pub fn back_target(&self) -> Option<&AppRoute> {
        self.back.last()
    }

    /// Returns the route that Forward would select without mutating history.
    pub fn forward_target(&self) -> Option<&AppRoute> {
        self.forward.last()
    }

    /// Navigates normally, preserving the old route for Back and discarding Forward.
    pub fn navigate(&mut self, route: AppRoute) {
        if self.current != route {
            self.back.push(std::mem::replace(&mut self.current, route));
            self.forward.clear();
        }
    }

    /// Replaces the visible route without retaining it in Back history.
    pub fn replace(&mut self, route: AppRoute) {
        self.current = route;
        self.forward.clear();
    }

    /// Replaces the route and removes every prior navigation entry.
    ///
    /// This is used at a session boundary so Back and Forward cannot reveal a page entity from
    /// the previous session.
    pub fn reset(&mut self, route: AppRoute) {
        self.back.clear();
        self.current = route;
        self.forward.clear();
    }

    /// Returns to the preceding route when present.
    pub fn back(&mut self) -> Option<&AppRoute> {
        let previous = self.back.pop()?;
        self.forward.push(std::mem::replace(&mut self.current, previous));
        Some(&self.current)
    }

    /// Returns to the next route when present.
    pub fn forward(&mut self) -> Option<&AppRoute> {
        let next = self.forward.pop()?;
        self.back.push(std::mem::replace(&mut self.current, next));
        Some(&self.current)
    }
}

fn parse_broker_tab(value: &str) -> Result<BrokerTab, RouteParseError> {
    match value {
        "overview" => Ok(BrokerTab::Overview),
        "runtime" => Ok(BrokerTab::Runtime),
        "configuration" => Ok(BrokerTab::Configuration),
        _ => Err(RouteParseError::InvalidParameter),
    }
}

fn parse_topic_tab(value: &str) -> Result<TopicTab, RouteParseError> {
    match value {
        "overview" => Ok(TopicTab::Overview),
        "queues" => Ok(TopicTab::Queues),
        "permissions" => Ok(TopicTab::Permissions),
        _ => Err(RouteParseError::InvalidParameter),
    }
}

fn parse_consumer_tab(value: &str) -> Result<ConsumerTab, RouteParseError> {
    match value {
        "overview" => Ok(ConsumerTab::Overview),
        "subscriptions" => Ok(ConsumerTab::Subscriptions),
        "connections" => Ok(ConsumerTab::Connections),
        "offsets" => Ok(ConsumerTab::Offsets),
        "monitoring" => Ok(ConsumerTab::Monitoring),
        _ => Err(RouteParseError::InvalidParameter),
    }
}

#[cfg(test)]
mod tests {
    use super::{AppRoute, BrokerTab, ConsumerTab, NavigationHistory, RouteKey, TopicTab};

    #[test]
    fn canonical_routes_round_trip_without_losing_detail_tabs() {
        let routes = [
            AppRoute::Login,
            AppRoute::Dashboard,
            AppRoute::Brokers,
            AppRoute::BrokerDetail {
                broker: RouteKey::parse("broker-a").expect("valid broker key"),
                tab: BrokerTab::Runtime,
            },
            AppRoute::Topics,
            AppRoute::TopicDetail {
                topic: RouteKey::parse("orders-v1").expect("valid topic key"),
                tab: TopicTab::Permissions,
            },
            AppRoute::Consumers,
            AppRoute::ConsumerDetail {
                group: RouteKey::parse("payments").expect("valid group key"),
                tab: ConsumerTab::Offsets,
            },
            AppRoute::Producers,
            AppRoute::Messages,
            AppRoute::DlqMessages,
            AppRoute::MessageTrace,
            AppRoute::Acl,
            AppRoute::Monitors,
            AppRoute::OpsSettings,
            AppRoute::Proxy,
        ];

        for route in routes {
            assert_eq!(AppRoute::parse(&route.format_path()), Ok(route));
        }
    }

    #[test]
    fn aliases_are_parse_only_compatibility() {
        assert_eq!(AppRoute::parse("/"), Ok(AppRoute::Dashboard));
        assert_eq!(AppRoute::parse("/ops"), Ok(AppRoute::OpsSettings));
        assert_eq!(AppRoute::parse("/nameservers"), Ok(AppRoute::OpsSettings));
        assert_eq!(AppRoute::parse("/cluster"), Ok(AppRoute::Brokers));
        assert_eq!(AppRoute::parse("/dlq"), Ok(AppRoute::DlqMessages));
        assert_eq!(AppRoute::OpsSettings.format_path(), "/ops-settings");
    }

    #[test]
    fn invalid_detail_parameters_and_tabs_are_rejected() {
        assert!(AppRoute::parse("/brokers//runtime").is_err());
        assert!(AppRoute::parse("/topics/orders/unknown").is_err());
        assert!(RouteKey::parse("a/b").is_err());
        assert!(RouteKey::parse("").is_err());
    }

    #[test]
    fn history_obeys_navigate_replace_back_and_forward_semantics() {
        let mut history = NavigationHistory::new(AppRoute::Login);
        history.replace(AppRoute::Dashboard);
        assert_eq!(history.back(), None);

        history.navigate(AppRoute::Brokers);
        history.navigate(AppRoute::Topics);
        assert_eq!(history.back(), Some(&AppRoute::Brokers));
        assert_eq!(history.forward(), Some(&AppRoute::Topics));
        assert_eq!(history.back(), Some(&AppRoute::Brokers));

        history.navigate(AppRoute::Consumers);
        assert_eq!(history.forward(), None);
        assert_eq!(history.current(), &AppRoute::Consumers);
    }

    #[test]
    fn reset_removes_back_and_forward_history_at_a_session_boundary() {
        let mut history = NavigationHistory::new(AppRoute::Dashboard);
        history.navigate(AppRoute::Brokers);
        history.navigate(AppRoute::Topics);
        assert_eq!(history.back(), Some(&AppRoute::Brokers));

        history.reset(AppRoute::Login);

        assert_eq!(history.current(), &AppRoute::Login);
        assert_eq!(history.back(), None);
        assert_eq!(history.forward(), None);
    }
}
