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

//! Protocol-independent Producer inspection models.

use std::fmt;

use crate::{
    CapabilityAvailability, ConnectionScope, ConsumerClientIdentity, ConsumerClientObservation, ConsumerObservation,
    ConsumerObservationState, ConsumerTargetFailure,
};

pub const PRODUCER_PAGE_SIZE: usize = 10;

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProducerIdentity(String);

impl ProducerIdentity {
    pub fn parse(value: impl Into<String>) -> Result<Self, ProducerValidationError> {
        let value = value.into().trim().to_string();
        if value.is_empty()
            || value.len() > 255
            || value
                .chars()
                .any(|character| character.is_control() || matches!(character, '/' | '?' | '#' | '\\'))
        {
            return Err(ProducerValidationError::InvalidGroup);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ProducerIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ProducerIdentity").finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProducerCapabilities {
    pub inventory: CapabilityAvailability,
    pub connections: CapabilityAvailability,
    pub client_detail: CapabilityAvailability,
}

impl ProducerCapabilities {
    #[must_use]
    pub const fn for_scope(scope: ConnectionScope) -> Self {
        match scope {
            ConnectionScope::NameServer => Self {
                inventory: CapabilityAvailability::Available,
                connections: CapabilityAvailability::Available,
                client_detail: CapabilityAvailability::Available,
            },
            ConnectionScope::Proxy => Self {
                inventory: CapabilityAvailability::Unavailable,
                connections: CapabilityAvailability::Unavailable,
                client_detail: CapabilityAvailability::Unavailable,
            },
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProducerGroupObservation {
    pub identity: ProducerIdentity,
    pub client_count: ConsumerObservation<usize>,
}

impl fmt::Debug for ProducerGroupObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerGroupObservation")
            .field("client_count", &self.client_count.state())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProducerInventory {
    pub groups: Vec<ProducerGroupObservation>,
    pub observation: ConsumerObservationState,
    pub failures: Vec<ConsumerTargetFailure>,
    pub capabilities: ProducerCapabilities,
}

impl fmt::Debug for ProducerInventory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerInventory")
            .field("group_count", &self.groups.len())
            .field("observation", &self.observation)
            .field("failure_count", &self.failures.len())
            .field("capabilities", &self.capabilities)
            .finish()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProducerFilterDraft {
    pub keyword: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProducerPage {
    pub items: Vec<ProducerGroupObservation>,
    pub page: usize,
    pub page_count: usize,
    pub total: usize,
}

#[must_use]
pub fn filter_page_producers(
    items: &[ProducerGroupObservation],
    filter: &ProducerFilterDraft,
    requested_page: usize,
) -> ProducerPage {
    let keyword = filter.keyword.trim().to_ascii_lowercase();
    let mut items = items
        .iter()
        .filter(|item| keyword.is_empty() || item.identity.as_str().to_ascii_lowercase().contains(&keyword))
        .cloned()
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left.identity.cmp(&right.identity));
    let total = items.len();
    let page_count = total.div_ceil(PRODUCER_PAGE_SIZE).max(1);
    let page = requested_page.clamp(1, page_count);
    let start = (page - 1) * PRODUCER_PAGE_SIZE;
    ProducerPage {
        items: items.into_iter().skip(start).take(PRODUCER_PAGE_SIZE).collect(),
        page,
        page_count,
        total,
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct ProducerConnectionQueryDraft {
    pub topic: String,
    pub group: String,
}

impl fmt::Debug for ProducerConnectionQueryDraft {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerConnectionQueryDraft")
            .field("topic_configured", &!self.topic.trim().is_empty())
            .field("group_configured", &!self.group.trim().is_empty())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProducerConnectionQuery {
    topic: String,
    group: ProducerIdentity,
}

impl ProducerConnectionQuery {
    pub fn try_from_draft(draft: &ProducerConnectionQueryDraft) -> Result<Self, ProducerValidationError> {
        let topic = draft.topic.trim().to_string();
        if topic.is_empty() || topic.len() > 127 || topic.chars().any(char::is_control) {
            return Err(ProducerValidationError::InvalidTopic);
        }
        Ok(Self {
            topic,
            group: ProducerIdentity::parse(draft.group.clone())?,
        })
    }

    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    #[must_use]
    pub fn group(&self) -> &ProducerIdentity {
        &self.group
    }
}

impl fmt::Debug for ProducerConnectionQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerConnectionQuery")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProducerConnections {
    pub query: ProducerConnectionQuery,
    pub clients: Vec<ConsumerClientObservation>,
}

impl ProducerConnections {
    #[must_use]
    pub fn find_client(&self, identity: &ConsumerClientIdentity) -> Option<&ConsumerClientObservation> {
        self.clients.iter().find(|client| &client.identity == identity)
    }
}

impl fmt::Debug for ProducerConnections {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerConnections")
            .field("client_count", &self.clients.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProducerValidationError {
    #[error("Producer group is invalid.")]
    InvalidGroup,
    #[error("Producer connection query requires a valid Topic.")]
    InvalidTopic,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConsumerUnknownReason;

    fn group(index: usize) -> ProducerGroupObservation {
        ProducerGroupObservation {
            identity: ProducerIdentity::parse(format!("producer-{index:02}")).expect("group"),
            client_count: ConsumerObservation::Complete(index),
        }
    }

    #[test]
    fn producer_inventory_never_infers_status_from_discovery() {
        let observation = ProducerGroupObservation {
            identity: ProducerIdentity::parse("orders-producer").expect("group"),
            client_count: ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::Unavailable,
            },
        };
        assert_eq!(observation.client_count.value(), None);
        assert!(!format!("{observation:?}").contains("Active"));
    }

    #[test]
    fn keyword_and_page_are_deterministic_and_clamped() {
        let items = (0..12).map(group).collect::<Vec<_>>();
        let page = filter_page_producers(
            &items,
            &ProducerFilterDraft {
                keyword: "producer".into(),
            },
            99,
        );
        assert_eq!(page.page, 2);
        assert_eq!(page.page_count, 2);
        assert_eq!(page.items.len(), 2);
    }

    #[test]
    fn topic_and_group_must_both_be_applied_before_query() {
        assert!(ProducerConnectionQuery::try_from_draft(&ProducerConnectionQueryDraft {
            topic: "orders".into(),
            group: String::new(),
        })
        .is_err());
        assert!(ProducerConnectionQuery::try_from_draft(&ProducerConnectionQueryDraft {
            topic: String::new(),
            group: "orders-producer".into(),
        })
        .is_err());
        let query = ProducerConnectionQuery::try_from_draft(&ProducerConnectionQueryDraft {
            topic: "orders".into(),
            group: "orders-producer".into(),
        })
        .expect("query");
        assert_eq!(query.topic(), "orders");
        assert_eq!(query.group().as_str(), "orders-producer");
    }

    #[test]
    fn producer_capabilities_disable_proxy_without_a_real_forward_path() {
        let direct = ProducerCapabilities::for_scope(ConnectionScope::NameServer);
        assert_eq!(direct.inventory, CapabilityAvailability::Available);
        assert_eq!(direct.connections, CapabilityAvailability::Available);
        assert_eq!(direct.client_detail, CapabilityAvailability::Available);

        let proxy = ProducerCapabilities::for_scope(ConnectionScope::Proxy);
        assert_eq!(proxy.inventory, CapabilityAvailability::Unavailable);
        assert_eq!(proxy.connections, CapabilityAvailability::Unavailable);
        assert_eq!(proxy.client_detail, CapabilityAvailability::Unavailable);
    }
}
