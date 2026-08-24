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

//! Protocol-independent Topic models used by native Dashboard features.

use std::collections::BTreeSet;
use std::fmt;

pub const TOPIC_PAGE_SIZE: usize = 10;

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicIdentity(String);

impl TopicIdentity {
    pub fn parse(value: impl Into<String>) -> Result<Self, TopicValidationError> {
        let value = value.into().trim().to_string();
        if value.is_empty() || value.len() > 127 || value.chars().any(char::is_control) {
            return Err(TopicValidationError::InvalidTopicName);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for TopicIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("TopicIdentity").finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TopicTargetIdentity {
    cluster_name: String,
    broker_name: String,
    broker_address: String,
}

impl TopicTargetIdentity {
    pub fn parse(
        cluster_name: impl Into<String>,
        broker_name: impl Into<String>,
        broker_address: impl Into<String>,
    ) -> Result<Self, TopicValidationError> {
        Ok(Self {
            cluster_name: non_empty("cluster", cluster_name)?,
            broker_name: non_empty("broker", broker_name)?,
            broker_address: non_empty("broker address", broker_address)?,
        })
    }

    #[must_use]
    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    #[must_use]
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    #[must_use]
    pub fn broker_address(&self) -> &str {
        &self.broker_address
    }
}

impl fmt::Debug for TopicTargetIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("TopicTargetIdentity").finish_non_exhaustive()
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct TopicSelection {
    clusters: BTreeSet<String>,
    brokers: BTreeSet<String>,
}

impl TopicSelection {
    pub fn try_new(
        clusters: impl IntoIterator<Item = String>,
        brokers: impl IntoIterator<Item = String>,
    ) -> Result<Self, TopicValidationError> {
        let selection = Self {
            clusters: canonical_names(clusters),
            brokers: canonical_names(brokers),
        };
        if selection.is_empty() {
            Err(TopicValidationError::MissingTarget)
        } else {
            Ok(selection)
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.clusters.is_empty() && self.brokers.is_empty()
    }

    pub fn clusters(&self) -> impl Iterator<Item = &str> {
        self.clusters.iter().map(String::as_str)
    }

    pub fn brokers(&self) -> impl Iterator<Item = &str> {
        self.brokers.iter().map(String::as_str)
    }
}

impl fmt::Debug for TopicSelection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicSelection")
            .field("cluster_count", &self.clusters.len())
            .field("broker_count", &self.brokers.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum TopicCategory {
    Application,
    Retry,
    Dlq,
    System,
    #[default]
    Unknown,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum TopicMessageType {
    Normal,
    Delay,
    Fifo,
    Transaction,
    Retry,
    Dlq,
    System,
    Unspecified,
    #[default]
    Unknown,
}

impl TopicMessageType {
    #[must_use]
    pub fn parse(value: Option<&str>) -> Self {
        match value.map(str::trim).map(str::to_ascii_uppercase).as_deref() {
            Some("NORMAL") => Self::Normal,
            Some("DELAY") => Self::Delay,
            Some("FIFO") => Self::Fifo,
            Some("TRANSACTION") => Self::Transaction,
            Some("RETRY") => Self::Retry,
            Some("DLQ") => Self::Dlq,
            Some("SYSTEM") => Self::System,
            Some("UNSPECIFIED") => Self::Unspecified,
            _ => Self::Unknown,
        }
    }
}

impl TopicCategory {
    #[must_use]
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_uppercase().as_str() {
            "APPLICATION" | "NORMAL" | "DELAY" | "FIFO" | "TRANSACTION" => Self::Application,
            "RETRY" => Self::Retry,
            "DLQ" => Self::Dlq,
            "SYSTEM" => Self::System,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopicPermission(u8);

impl TopicPermission {
    pub fn parse(bits: i32) -> Result<Self, TopicValidationError> {
        let bits = u8::try_from(bits).map_err(|_| TopicValidationError::InvalidPermission)?;
        if !(1..=7).contains(&bits) || bits & 0b110 == 0 {
            Err(TopicValidationError::InvalidPermission)
        } else {
            Ok(Self(bits))
        }
    }

    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }

    #[must_use]
    pub const fn can_read(self) -> bool {
        self.0 & 0b100 != 0
    }

    #[must_use]
    pub const fn can_write(self) -> bool {
        self.0 & 0b010 != 0
    }

    #[must_use]
    pub const fn inherits(self) -> bool {
        self.0 & 0b001 != 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicCompleteness {
    Complete,
    Partial {
        successful_target_count: usize,
        failed_target_count: usize,
    },
}

impl TopicCompleteness {
    #[must_use]
    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicFailureStage {
    CatalogConfig,
    CatalogRoute,
    Stats,
    Configuration,
    Consumer,
    Mutation,
    Reload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicFailureCode {
    NotFound,
    InvalidData,
    Unavailable,
    Conflict,
}

#[derive(Clone, PartialEq, Eq)]
pub struct TopicTargetFailure {
    pub target: String,
    pub stage: TopicFailureStage,
    pub code: TopicFailureCode,
    pub retryable: bool,
}

impl fmt::Debug for TopicTargetFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicTargetFailure")
            .field("stage", &self.stage)
            .field("code", &self.code)
            .field("retryable", &self.retryable)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TopicInventoryItem {
    pub identity: TopicIdentity,
    pub category: TopicCategory,
    pub message_type: TopicMessageType,
    pub clusters: Vec<String>,
    pub brokers: Vec<String>,
    pub read_queue_count: Option<u32>,
    pub write_queue_count: Option<u32>,
    pub permission: Option<TopicPermission>,
    pub ordered: Option<bool>,
}

impl TopicInventoryItem {
    #[must_use]
    pub fn is_mutable(&self) -> bool {
        self.category == TopicCategory::Application && self.is_complete()
            || matches!(self.category, TopicCategory::Retry | TopicCategory::Dlq) && self.is_complete()
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.category != TopicCategory::Unknown
            && self.message_type != TopicMessageType::Unknown
            && self.read_queue_count.is_some()
            && self.write_queue_count.is_some()
            && self.permission.is_some()
            && self.ordered.is_some()
            && !self.brokers.is_empty()
            && !self.clusters.is_empty()
    }
}

impl fmt::Debug for TopicInventoryItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicInventoryItem")
            .field("category", &self.category)
            .field("message_type", &self.message_type)
            .field("cluster_count", &self.clusters.len())
            .field("broker_count", &self.brokers.len())
            .field("complete", &self.is_complete())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TopicInventory {
    pub items: Vec<TopicInventoryItem>,
    pub targets: Vec<TopicTargetIdentity>,
    pub completeness: TopicCompleteness,
    pub failures: Vec<TopicTargetFailure>,
}

impl fmt::Debug for TopicInventory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicInventory")
            .field("item_count", &self.items.len())
            .field("target_count", &self.targets.len())
            .field("completeness", &self.completeness)
            .field("failure_count", &self.failures.len())
            .finish()
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct TopicFilterDraft {
    pub keyword: String,
    pub message_type: Option<TopicMessageType>,
    pub category: Option<TopicCategory>,
    pub cluster: Option<String>,
    pub broker: Option<String>,
}

impl TopicFilterDraft {
    #[must_use]
    pub fn normalized(mut self) -> Self {
        self.keyword = self.keyword.trim().to_ascii_lowercase();
        self.cluster = normalized_optional(self.cluster);
        self.broker = normalized_optional(self.broker);
        self
    }

    #[must_use]
    pub fn matches(&self, item: &TopicInventoryItem) -> bool {
        let keyword_matches = self.keyword.is_empty()
            || item
                .identity
                .as_str()
                .to_ascii_lowercase()
                .contains(&self.keyword.to_ascii_lowercase());
        keyword_matches
            && self
                .message_type
                .is_none_or(|message_type| item.message_type == message_type)
            && self.category.is_none_or(|category| item.category == category)
            && self
                .cluster
                .as_deref()
                .is_none_or(|cluster| item.clusters.iter().any(|value| value == cluster))
            && self
                .broker
                .as_deref()
                .is_none_or(|broker| item.brokers.iter().any(|value| value == broker))
    }
}

impl fmt::Debug for TopicFilterDraft {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicFilterDraft")
            .field("has_keyword", &!self.keyword.is_empty())
            .field("message_type", &self.message_type)
            .field("category", &self.category)
            .field("has_cluster", &self.cluster.is_some())
            .field("has_broker", &self.broker.is_some())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TopicSortKey {
    #[default]
    Name,
    Category,
    MessageType,
    ReadQueues,
    WriteQueues,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SortDirection {
    #[default]
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TopicSort {
    pub key: TopicSortKey,
    pub direction: SortDirection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicPage {
    pub items: Vec<TopicInventoryItem>,
    pub page: usize,
    pub page_count: usize,
    pub total: usize,
}

#[must_use]
pub fn filter_sort_page_topics(
    items: &[TopicInventoryItem],
    filter: &TopicFilterDraft,
    sort: TopicSort,
    requested_page: usize,
) -> TopicPage {
    let filter = filter.clone().normalized();
    let mut items = items
        .iter()
        .filter(|item| filter.matches(item))
        .cloned()
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        let ordering = match sort.key {
            TopicSortKey::Name => left.identity.as_str().cmp(right.identity.as_str()),
            TopicSortKey::Category => left.category.cmp(&right.category),
            TopicSortKey::MessageType => left.message_type.cmp(&right.message_type),
            TopicSortKey::ReadQueues => left.read_queue_count.cmp(&right.read_queue_count),
            TopicSortKey::WriteQueues => left.write_queue_count.cmp(&right.write_queue_count),
        }
        .then_with(|| left.identity.as_str().cmp(right.identity.as_str()));
        match sort.direction {
            SortDirection::Ascending => ordering,
            SortDirection::Descending => ordering.reverse(),
        }
    });
    let total = items.len();
    let page_count = total.div_ceil(TOPIC_PAGE_SIZE).max(1);
    let page = requested_page.clamp(1, page_count);
    let start = (page - 1) * TOPIC_PAGE_SIZE;
    let items = items.into_iter().skip(start).take(TOPIC_PAGE_SIZE).collect();
    TopicPage {
        items,
        page,
        page_count,
        total,
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TopicQueueOffsetView {
    pub broker_name: String,
    pub queue_id: i32,
    pub min_offset: i64,
    pub max_offset: i64,
    pub last_update_timestamp: i64,
}

impl TopicQueueOffsetView {
    #[must_use]
    pub fn message_count(&self) -> i64 {
        (self.max_offset - self.min_offset).max(0)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TopicStatsView {
    pub topic: TopicIdentity,
    pub total_message_count: i64,
    pub offsets: Vec<TopicQueueOffsetView>,
    pub completeness: TopicCompleteness,
    pub failures: Vec<TopicTargetFailure>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct TopicRouteBrokerView {
    pub cluster_name: String,
    pub broker_name: String,
    pub address_count: usize,
    pub zone_name: Option<String>,
    pub acting_master: bool,
}

#[derive(Clone, PartialEq, Eq)]
pub struct TopicRouteQueueView {
    pub broker_name: String,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub permission: Option<TopicPermission>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct TopicRouteView {
    pub topic: TopicIdentity,
    pub brokers: Vec<TopicRouteBrokerView>,
    pub queues: Vec<TopicRouteQueueView>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct TopicConfigTargetView {
    pub target: TopicTargetIdentity,
    pub version: u64,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub permission: Option<TopicPermission>,
    pub ordered: bool,
    pub message_type: TopicMessageType,
}

#[derive(Clone, PartialEq, Eq)]
pub struct TopicConfigView {
    pub topic: TopicIdentity,
    pub targets: Vec<TopicConfigTargetView>,
    pub inconsistent_fields: Vec<TopicConfigField>,
    pub completeness: TopicCompleteness,
    pub failures: Vec<TopicTargetFailure>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicConfigField {
    ReadQueues,
    WriteQueues,
    Permission,
    Ordered,
    MessageType,
}

#[derive(Clone, PartialEq)]
pub struct TopicConsumerView {
    pub consumer_group: String,
    pub total_diff: i64,
    pub inflight_diff: i64,
    pub consume_tps: f64,
}

#[derive(Clone, PartialEq)]
pub struct TopicConsumersView {
    pub topic: TopicIdentity,
    pub items: Vec<TopicConsumerView>,
    pub completeness: TopicCompleteness,
    pub failures: Vec<TopicTargetFailure>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicMutationKind {
    Create,
    Edit,
    DeleteTopic,
    DeleteBroker,
    Send,
    ResetOffset,
    SkipBacklog,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicMutationGuarantee {
    VersionCas,
    PreflightBestEffort,
}

#[derive(Clone, PartialEq, Eq)]
pub struct TopicTargetOutcome {
    pub target: String,
    pub stage: TopicFailureStage,
    pub applied: bool,
    pub failure: Option<TopicFailureCode>,
    pub retryable: bool,
}

impl fmt::Debug for TopicTargetOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicTargetOutcome")
            .field("stage", &self.stage)
            .field("applied", &self.applied)
            .field("failure", &self.failure)
            .field("retryable", &self.retryable)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TopicPartialOutcome {
    pub topic: TopicIdentity,
    pub kind: TopicMutationKind,
    pub guarantee: TopicMutationGuarantee,
    pub targets: Vec<TopicTargetOutcome>,
    pub reload_failed: bool,
}

impl TopicPartialOutcome {
    #[must_use]
    pub fn applied_count(&self) -> usize {
        self.targets.iter().filter(|target| target.applied).count()
    }

    #[must_use]
    pub fn failed_count(&self) -> usize {
        self.targets.len().saturating_sub(self.applied_count())
    }

    #[must_use]
    pub fn is_complete_success(&self) -> bool {
        !self.targets.is_empty() && self.failed_count() == 0 && !self.reload_failed
    }
}

impl fmt::Debug for TopicPartialOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicPartialOutcome")
            .field("kind", &self.kind)
            .field("guarantee", &self.guarantee)
            .field("target_count", &self.targets.len())
            .field("applied_count", &self.applied_count())
            .field("reload_failed", &self.reload_failed)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TopicValidationError {
    #[error("Topic name is invalid.")]
    InvalidTopicName,
    #[error("At least one authoritative target is required.")]
    MissingTarget,
    #[error("Queue counts must be between 1 and 128.")]
    InvalidQueueCount,
    #[error("Topic permission must include read or write access.")]
    InvalidPermission,
    #[error("A required Topic identity field is empty.")]
    EmptyIdentity,
}

pub fn validate_queue_count(value: u32) -> Result<u32, TopicValidationError> {
    if (1..=128).contains(&value) {
        Ok(value)
    } else {
        Err(TopicValidationError::InvalidQueueCount)
    }
}

fn canonical_names(values: impl IntoIterator<Item = String>) -> BTreeSet<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect()
}

fn normalized_optional(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn non_empty(_field: &'static str, value: impl Into<String>) -> Result<String, TopicValidationError> {
    let value = value.into().trim().to_string();
    if value.is_empty() || value.chars().any(char::is_control) {
        Err(TopicValidationError::EmptyIdentity)
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(name: &str, category: TopicCategory, message_type: TopicMessageType) -> TopicInventoryItem {
        TopicInventoryItem {
            identity: TopicIdentity::parse(name).expect("valid Topic"),
            category,
            message_type,
            clusters: vec!["cluster-a".into()],
            brokers: vec!["broker-a".into()],
            read_queue_count: Some(8),
            write_queue_count: Some(8),
            permission: Some(TopicPermission::parse(6).expect("permission")),
            ordered: Some(false),
        }
    }

    #[test]
    fn legacy_wire_shapes_remain_outside_the_domain_model() {
        assert!(!std::any::type_name::<TopicIdentity>().contains("serde"));
        assert!(!format!("{:?}", TopicIdentity::parse("private-orders").expect("identity")).contains("private-orders"));
    }

    #[test]
    fn classification_filter_sort_and_page_are_deterministic() {
        let mut items = (0..12)
            .map(|index| {
                item(
                    &format!("orders-{index:02}"),
                    TopicCategory::Application,
                    TopicMessageType::Normal,
                )
            })
            .collect::<Vec<_>>();
        items.push(item("retry-orders", TopicCategory::Retry, TopicMessageType::Retry));
        let filter = TopicFilterDraft {
            keyword: "orders".into(),
            category: Some(TopicCategory::Application),
            ..Default::default()
        };
        let page = filter_sort_page_topics(&items, &filter, TopicSort::default(), 99);
        assert_eq!(page.page, 2);
        assert_eq!(page.page_count, 2);
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].identity.as_str(), "orders-10");
    }

    #[test]
    fn incomplete_or_system_topics_fail_closed_for_mutation() {
        let mut incomplete = item("orders", TopicCategory::Application, TopicMessageType::Normal);
        incomplete.write_queue_count = None;
        assert!(!incomplete.is_mutable());
        assert!(!item("RMQ_SYS_TRACE_TOPIC", TopicCategory::System, TopicMessageType::System).is_mutable());
    }

    #[test]
    fn partial_outcome_never_claims_atomic_success() {
        let outcome = TopicPartialOutcome {
            topic: TopicIdentity::parse("orders").expect("identity"),
            kind: TopicMutationKind::Create,
            guarantee: TopicMutationGuarantee::PreflightBestEffort,
            targets: vec![
                TopicTargetOutcome {
                    target: "broker-a".into(),
                    stage: TopicFailureStage::Mutation,
                    applied: true,
                    failure: None,
                    retryable: false,
                },
                TopicTargetOutcome {
                    target: "broker-b".into(),
                    stage: TopicFailureStage::Mutation,
                    applied: false,
                    failure: Some(TopicFailureCode::Unavailable),
                    retryable: true,
                },
            ],
            reload_failed: false,
        };
        assert!(!outcome.is_complete_success());
        assert_eq!(outcome.applied_count(), 1);
    }
}
