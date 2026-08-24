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

//! Protocol-independent Consumer models used by native Dashboard features.

use std::cmp::Ordering;
use std::fmt;

use crate::ConnectionScope;

pub const CONSUMER_PAGE_SIZE: usize = 10;
pub const CONSUMER_DIAGNOSTIC_MAX_BYTES: usize = 256 * 1024;

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConsumerIdentity(String);

impl ConsumerIdentity {
    pub fn parse(value: impl Into<String>) -> Result<Self, ConsumerValidationError> {
        let value = value.into().trim().to_string();
        if value.is_empty()
            || value.len() > 255
            || value
                .chars()
                .any(|character| character.is_control() || matches!(character, '/' | '?' | '#' | '\\'))
        {
            return Err(ConsumerValidationError::InvalidGroup);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ConsumerIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ConsumerIdentity").finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConsumerClientIdentity(String);

impl ConsumerClientIdentity {
    pub fn parse(value: impl Into<String>) -> Result<Self, ConsumerValidationError> {
        let value = non_empty(value)?;
        if value.len() > 1024 {
            return Err(ConsumerValidationError::InvalidClient);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ConsumerClientIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ConsumerClientIdentity").finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerTargetIdentity {
    cluster_name: String,
    broker_name: String,
    broker_address: String,
}

impl ConsumerTargetIdentity {
    pub fn parse(
        cluster_name: impl Into<String>,
        broker_name: impl Into<String>,
        broker_address: impl Into<String>,
    ) -> Result<Self, ConsumerValidationError> {
        Ok(Self {
            cluster_name: non_empty(cluster_name)?,
            broker_name: non_empty(broker_name)?,
            broker_address: non_empty(broker_address)?,
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

impl fmt::Debug for ConsumerTargetIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerTargetIdentity")
            .field("cluster_configured", &!self.cluster_name.is_empty())
            .field("broker_configured", &!self.broker_name.is_empty())
            .field("address_configured", &!self.broker_address.is_empty())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerObservationState {
    Complete,
    Partial,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerUnknownReason {
    NotRequested,
    Unsupported,
    Unavailable,
    InvalidResponse,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerFailureStage {
    Inventory,
    Clients,
    Progress,
    Configuration,
    ConnectionObservation,
    PreflightAborted,
    Mutation,
    Cleanup,
    Reload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerFailureCode {
    NotFound,
    Unavailable,
    Unsupported,
    Conflict,
    InvalidData,
    Authorization,
    NotApplied,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerTargetFailure {
    pub target: String,
    pub stage: ConsumerFailureStage,
    pub code: ConsumerFailureCode,
    pub retryable: bool,
}

impl fmt::Debug for ConsumerTargetFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerTargetFailure")
            .field("stage", &self.stage)
            .field("code", &self.code)
            .field("retryable", &self.retryable)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum ConsumerObservation<T> {
    Complete(T),
    Partial {
        value: T,
        successful_target_count: usize,
        failures: Vec<ConsumerTargetFailure>,
    },
    Unknown {
        reason: ConsumerUnknownReason,
    },
}

impl<T> ConsumerObservation<T> {
    #[must_use]
    pub const fn state(&self) -> ConsumerObservationState {
        match self {
            Self::Complete(_) => ConsumerObservationState::Complete,
            Self::Partial { .. } => ConsumerObservationState::Partial,
            Self::Unknown { .. } => ConsumerObservationState::Unknown,
        }
    }

    #[must_use]
    pub const fn value(&self) -> Option<&T> {
        match self {
            Self::Complete(value) | Self::Partial { value, .. } => Some(value),
            Self::Unknown { .. } => None,
        }
    }

    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self, Self::Complete(_))
    }
}

impl<T: fmt::Debug> fmt::Debug for ConsumerObservation<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Complete(value) => formatter.debug_tuple("Complete").field(value).finish(),
            Self::Partial {
                successful_target_count,
                failures,
                ..
            } => formatter
                .debug_struct("Partial")
                .field("successful_target_count", successful_target_count)
                .field("failure_count", &failures.len())
                .finish_non_exhaustive(),
            Self::Unknown { reason } => formatter.debug_struct("Unknown").field("reason", reason).finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerConnectionState {
    Connected,
    Disconnected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerCategory {
    Application,
    System,
    Unknown,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerGroupObservation {
    pub identity: ConsumerIdentity,
    pub category: ConsumerCategory,
    pub connection_state: ConsumerObservation<ConsumerConnectionState>,
    pub client_count: ConsumerObservation<usize>,
    pub lag: ConsumerObservation<i64>,
    pub consume_type: ConsumerObservation<String>,
    pub message_model: ConsumerObservation<String>,
    pub targets: Vec<ConsumerTargetIdentity>,
}

impl fmt::Debug for ConsumerGroupObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerGroupObservation")
            .field("category", &self.category)
            .field("connection_state", &self.connection_state.state())
            .field("client_count", &self.client_count.state())
            .field("lag", &self.lag.state())
            .field("target_count", &self.targets.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilityAvailability {
    Available,
    Unavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerCapabilities {
    pub inventory: CapabilityAvailability,
    pub clients: CapabilityAvailability,
    pub progress: CapabilityAvailability,
    pub configuration: CapabilityAvailability,
    pub diagnostics: CapabilityAvailability,
    pub create: CapabilityAvailability,
    pub edit: CapabilityAvailability,
    pub delete: CapabilityAvailability,
    pub offset_actions: CapabilityAvailability,
}

impl ConsumerCapabilities {
    #[must_use]
    pub const fn for_scope(scope: ConnectionScope) -> Self {
        match scope {
            ConnectionScope::NameServer => Self {
                inventory: CapabilityAvailability::Available,
                clients: CapabilityAvailability::Available,
                progress: CapabilityAvailability::Available,
                configuration: CapabilityAvailability::Available,
                diagnostics: CapabilityAvailability::Available,
                create: CapabilityAvailability::Available,
                edit: CapabilityAvailability::Available,
                delete: CapabilityAvailability::Available,
                offset_actions: CapabilityAvailability::Available,
            },
            ConnectionScope::Proxy => Self {
                inventory: CapabilityAvailability::Available,
                clients: CapabilityAvailability::Available,
                progress: CapabilityAvailability::Available,
                configuration: CapabilityAvailability::Unavailable,
                diagnostics: CapabilityAvailability::Unavailable,
                create: CapabilityAvailability::Unavailable,
                edit: CapabilityAvailability::Unavailable,
                delete: CapabilityAvailability::Unavailable,
                offset_actions: CapabilityAvailability::Unavailable,
            },
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerInventory {
    pub groups: Vec<ConsumerGroupObservation>,
    pub targets: Vec<ConsumerTargetIdentity>,
    pub observation: ConsumerObservationState,
    pub failures: Vec<ConsumerTargetFailure>,
    pub capabilities: ConsumerCapabilities,
}

impl fmt::Debug for ConsumerInventory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerInventory")
            .field("group_count", &self.groups.len())
            .field("target_count", &self.targets.len())
            .field("observation", &self.observation)
            .field("failure_count", &self.failures.len())
            .field("capabilities", &self.capabilities)
            .finish()
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct ConsumerFilterDraft {
    pub keyword: String,
    pub connection: Option<ConsumerConnectionFilter>,
    pub consume_type: Option<String>,
}

impl ConsumerFilterDraft {
    #[must_use]
    pub fn normalized(mut self) -> Self {
        self.keyword = self.keyword.trim().to_ascii_lowercase();
        self.consume_type = self
            .consume_type
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty());
        self
    }

    #[must_use]
    pub fn matches(&self, item: &ConsumerGroupObservation) -> bool {
        let keyword_matches = self.keyword.is_empty()
            || item
                .identity
                .as_str()
                .to_ascii_lowercase()
                .contains(&self.keyword.to_ascii_lowercase());
        let connection_matches = self.connection.is_none_or(|filter| match filter {
            ConsumerConnectionFilter::Connected => matches!(
                item.connection_state,
                ConsumerObservation::Complete(ConsumerConnectionState::Connected)
            ),
            ConsumerConnectionFilter::Disconnected => matches!(
                item.connection_state,
                ConsumerObservation::Complete(ConsumerConnectionState::Disconnected)
            ),
            ConsumerConnectionFilter::Unknown => {
                matches!(item.connection_state, ConsumerObservation::Unknown { .. })
            }
        });
        let consume_type_matches = self.consume_type.as_deref().is_none_or(|expected| {
            item.consume_type
                .value()
                .is_some_and(|value| value.eq_ignore_ascii_case(expected))
        });
        keyword_matches && connection_matches && consume_type_matches
    }
}

impl fmt::Debug for ConsumerFilterDraft {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerFilterDraft")
            .field("has_keyword", &!self.keyword.is_empty())
            .field("connection", &self.connection)
            .field("has_consume_type", &self.consume_type.is_some())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerConnectionFilter {
    Connected,
    Disconnected,
    Unknown,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ConsumerSortKey {
    #[default]
    Group,
    Clients,
    Lag,
    ConsumeType,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ConsumerSortDirection {
    #[default]
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConsumerSort {
    pub key: ConsumerSortKey,
    pub direction: ConsumerSortDirection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerPage {
    pub items: Vec<ConsumerGroupObservation>,
    pub page: usize,
    pub page_count: usize,
    pub total: usize,
}

#[must_use]
pub fn filter_sort_page_consumers(
    items: &[ConsumerGroupObservation],
    filter: &ConsumerFilterDraft,
    sort: ConsumerSort,
    requested_page: usize,
) -> ConsumerPage {
    let filter = filter.clone().normalized();
    let mut items = items
        .iter()
        .filter(|item| filter.matches(item))
        .cloned()
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        let ordering = match sort.key {
            ConsumerSortKey::Group => left.identity.cmp(&right.identity),
            ConsumerSortKey::Clients => observation_cmp(&left.client_count, &right.client_count),
            ConsumerSortKey::Lag => observation_cmp(&left.lag, &right.lag),
            ConsumerSortKey::ConsumeType => observation_cmp(&left.consume_type, &right.consume_type),
        }
        .then_with(|| left.identity.cmp(&right.identity));
        match sort.direction {
            ConsumerSortDirection::Ascending => ordering,
            ConsumerSortDirection::Descending => ordering.reverse(),
        }
    });
    let total = items.len();
    let page_count = total.div_ceil(CONSUMER_PAGE_SIZE).max(1);
    let page = requested_page.clamp(1, page_count);
    let start = (page - 1) * CONSUMER_PAGE_SIZE;
    ConsumerPage {
        items: items.into_iter().skip(start).take(CONSUMER_PAGE_SIZE).collect(),
        page,
        page_count,
        total,
    }
}

fn observation_cmp<T: Ord>(left: &ConsumerObservation<T>, right: &ConsumerObservation<T>) -> Ordering {
    match (left.value(), right.value()) {
        (Some(left), Some(right)) => left.cmp(right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => left.state().cmp(&right.state()),
    }
}

impl Ord for ConsumerObservationState {
    fn cmp(&self, other: &Self) -> Ordering {
        observation_state_rank(*self).cmp(&observation_state_rank(*other))
    }
}

impl PartialOrd for ConsumerObservationState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

const fn observation_state_rank(state: ConsumerObservationState) -> u8 {
    match state {
        ConsumerObservationState::Complete => 0,
        ConsumerObservationState::Partial => 1,
        ConsumerObservationState::Unknown => 2,
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerClientObservation {
    pub identity: ConsumerClientIdentity,
    pub address: String,
    pub language: String,
    pub version: i32,
    pub version_description: String,
}

impl fmt::Debug for ConsumerClientObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerClientObservation")
            .field("language", &self.language)
            .field("version", &self.version)
            .field("address_configured", &!self.address.is_empty())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerClients {
    pub group: ConsumerIdentity,
    pub clients: Vec<ConsumerClientObservation>,
    pub consume_type: ConsumerObservation<String>,
    pub message_model: ConsumerObservation<String>,
    pub subscriptions: Vec<ConsumerSubscription>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerSubscription {
    pub topic: String,
    pub expression: String,
    pub expression_type: String,
}

impl fmt::Debug for ConsumerSubscription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerSubscription")
            .field("topic_configured", &!self.topic.is_empty())
            .field("expression_type", &self.expression_type)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerProgressRow {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub delta: i64,
    pub last_timestamp: i64,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerProgress {
    pub group: ConsumerIdentity,
    pub rows: Vec<ConsumerProgressRow>,
    pub total_delta: i64,
}

impl ConsumerProgress {
    #[must_use]
    pub fn from_rows(group: ConsumerIdentity, mut rows: Vec<ConsumerProgressRow>) -> Self {
        rows.sort_by(|left, right| {
            left.topic
                .cmp(&right.topic)
                .then(left.broker_name.cmp(&right.broker_name))
                .then(left.queue_id.cmp(&right.queue_id))
        });
        let total_delta = rows.iter().map(|row| row.delta).sum();
        Self {
            group,
            rows,
            total_delta,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerConfigIdentity {
    pub group: ConsumerIdentity,
    pub target: ConsumerTargetIdentity,
}

impl fmt::Debug for ConsumerConfigIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ConsumerConfigIdentity").finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerConfigEntries {
    pub retry_max_times: u32,
    pub retry_queue_nums: u32,
    pub consume_timeout_minutes: u32,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerConfigSnapshot {
    pub identity: ConsumerConfigIdentity,
    pub generation: u64,
    pub entries: ConsumerConfigEntries,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerConfiguration {
    pub group: ConsumerIdentity,
    pub snapshots: Vec<ConsumerConfigSnapshot>,
    pub observation: ConsumerObservationState,
    pub failures: Vec<ConsumerTargetFailure>,
}

impl fmt::Debug for ConsumerConfiguration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerConfiguration")
            .field("snapshot_count", &self.snapshots.len())
            .field("observation", &self.observation)
            .field("failure_count", &self.failures.len())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for ConsumerConfigSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerConfigSnapshot")
            .field("generation", &self.generation)
            .field("entries", &self.entries)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConsumerConfigPatch {
    pub retry_max_times: Option<u32>,
    pub retry_queue_nums: Option<u32>,
    pub consume_timeout_minutes: Option<u32>,
}

impl ConsumerConfigPatch {
    pub fn validate(self) -> Result<Self, ConsumerValidationError> {
        if self.retry_max_times.is_none() && self.retry_queue_nums.is_none() && self.consume_timeout_minutes.is_none() {
            return Err(ConsumerValidationError::EmptyPatch);
        }
        if self.retry_max_times.is_some_and(|value| !(1..=16).contains(&value))
            || self.retry_queue_nums.is_some_and(|value| !(1..=8).contains(&value))
            || self
                .consume_timeout_minutes
                .is_some_and(|value| !(1..=1_440).contains(&value))
        {
            return Err(ConsumerValidationError::InvalidConfigValue);
        }
        Ok(self)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerConfigPatchCommand {
    pub snapshot: ConsumerConfigSnapshot,
    pub patch: ConsumerConfigPatch,
    pub authorization: ConsumerAclClassification,
}

impl ConsumerConfigPatchCommand {
    pub fn validate(mut self) -> Result<Self, ConsumerValidationError> {
        require_authorized(self.authorization)?;
        self.patch = self.patch.validate()?;
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerConfigPatchOutcome {
    Applied {
        previous_generation: u64,
        generation: u64,
    },
    GenerationConflict {
        expected_generation: u64,
        actual_generation: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerMutationKind {
    Create,
    Edit,
    Delete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerMutationGuarantee {
    VersionCas,
    PreflightBestEffort,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerTargetOutcome {
    pub target: String,
    pub stage: ConsumerFailureStage,
    pub applied: bool,
    pub failure: Option<ConsumerFailureCode>,
    pub retryable: bool,
}

impl fmt::Debug for ConsumerTargetOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerTargetOutcome")
            .field("stage", &self.stage)
            .field("applied", &self.applied)
            .field("failure", &self.failure)
            .field("retryable", &self.retryable)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerPartialOutcome {
    pub group: ConsumerIdentity,
    pub kind: ConsumerMutationKind,
    pub guarantee: ConsumerMutationGuarantee,
    pub targets: Vec<ConsumerTargetOutcome>,
    pub reload_failed: bool,
}

impl ConsumerPartialOutcome {
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

impl fmt::Debug for ConsumerPartialOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerPartialOutcome")
            .field("kind", &self.kind)
            .field("guarantee", &self.guarantee)
            .field("target_count", &self.targets.len())
            .field("applied_count", &self.applied_count())
            .field("reload_failed", &self.reload_failed)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerAclClassification {
    Authorized,
    Denied,
    Unknown,
}

impl ConsumerAclClassification {
    #[must_use]
    pub const fn permits_mutation(self) -> bool {
        matches!(self, Self::Authorized)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerCreateCommand {
    pub group: ConsumerIdentity,
    pub targets: Vec<ConsumerTargetIdentity>,
    pub entries: ConsumerConfigEntries,
    pub authorization: ConsumerAclClassification,
}

impl ConsumerCreateCommand {
    pub fn validate(mut self) -> Result<Self, ConsumerValidationError> {
        require_authorized(self.authorization)?;
        validate_entries(self.entries)?;
        self.targets = canonical_targets(self.targets);
        if self.targets.is_empty() {
            return Err(ConsumerValidationError::EmptyTargets);
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerDeleteCommand {
    pub group: ConsumerIdentity,
    pub selected_targets: Vec<ConsumerTargetIdentity>,
    pub authoritative_targets: Vec<ConsumerTargetIdentity>,
    pub authorization: ConsumerAclClassification,
}

impl ConsumerDeleteCommand {
    pub fn validate(mut self) -> Result<Self, ConsumerValidationError> {
        require_authorized(self.authorization)?;
        self.selected_targets = canonical_targets(self.selected_targets);
        self.authoritative_targets = canonical_targets(self.authoritative_targets);
        if self.selected_targets.is_empty() || self.authoritative_targets.is_empty() {
            return Err(ConsumerValidationError::EmptyTargets);
        }
        if self
            .selected_targets
            .iter()
            .any(|target| !self.authoritative_targets.contains(target))
        {
            return Err(ConsumerValidationError::TargetsMismatch);
        }
        Ok(self)
    }
}

fn require_authorized(authorization: ConsumerAclClassification) -> Result<(), ConsumerValidationError> {
    if authorization.permits_mutation() {
        Ok(())
    } else {
        Err(ConsumerValidationError::Unauthorized)
    }
}

fn validate_entries(entries: ConsumerConfigEntries) -> Result<(), ConsumerValidationError> {
    if !(1..=16).contains(&entries.retry_max_times)
        || !(1..=8).contains(&entries.retry_queue_nums)
        || !(1..=1_440).contains(&entries.consume_timeout_minutes)
    {
        Err(ConsumerValidationError::InvalidConfigValue)
    } else {
        Ok(())
    }
}

fn canonical_targets(mut targets: Vec<ConsumerTargetIdentity>) -> Vec<ConsumerTargetIdentity> {
    targets.sort_by(|left, right| {
        left.cluster_name
            .cmp(&right.cluster_name)
            .then(left.broker_name.cmp(&right.broker_name))
            .then(left.broker_address.cmp(&right.broker_address))
    });
    targets.dedup();
    targets
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerDiagnosticKind {
    RunningInfo,
    Jstack,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerDiagnosticRequest {
    pub group: ConsumerIdentity,
    pub client: ConsumerClientIdentity,
    pub kind: ConsumerDiagnosticKind,
    pub max_output_bytes: usize,
}

impl ConsumerDiagnosticRequest {
    pub fn validate(self) -> Result<Self, ConsumerValidationError> {
        if !(1..=CONSUMER_DIAGNOSTIC_MAX_BYTES).contains(&self.max_output_bytes) {
            return Err(ConsumerValidationError::InvalidDiagnosticBudget);
        }
        Ok(self)
    }
}

impl fmt::Debug for ConsumerDiagnosticRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerDiagnosticRequest")
            .field("kind", &self.kind)
            .field("max_output_bytes", &self.max_output_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(PartialEq, Eq)]
pub struct ConsumerDiagnosticPayload {
    properties: Vec<(String, String)>,
    text: Option<String>,
    truncated: bool,
}

impl ConsumerDiagnosticPayload {
    #[must_use]
    pub fn new(properties: Vec<(String, String)>, text: Option<String>, truncated: bool) -> Self {
        Self {
            properties,
            text,
            truncated,
        }
    }

    #[must_use]
    pub fn properties(&self) -> &[(String, String)] {
        &self.properties
    }

    #[must_use]
    pub fn text(&self) -> Option<&str> {
        self.text.as_deref()
    }

    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }

    pub fn clear(&mut self) {
        for (key, value) in &mut self.properties {
            scrub_diagnostic_string(key);
            scrub_diagnostic_string(value);
        }
        self.properties.clear();
        if let Some(text) = &mut self.text {
            scrub_diagnostic_string(text);
        }
        self.text = None;
        self.truncated = false;
    }
}

fn scrub_diagnostic_string(value: &mut String) {
    let byte_len = value.len();
    if byte_len > 0 {
        value.replace_range(.., &"\0".repeat(byte_len));
        value.clear();
    }
}

impl fmt::Debug for ConsumerDiagnosticPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerDiagnosticPayload")
            .field("property_count", &self.properties.len())
            .field("text_loaded", &self.text.is_some())
            .field("truncated", &self.truncated)
            .finish()
    }
}

impl Drop for ConsumerDiagnosticPayload {
    fn drop(&mut self) {
        self.clear();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConsumerValidationError {
    #[error("Consumer group is invalid.")]
    InvalidGroup,
    #[error("Consumer client identity is invalid.")]
    InvalidClient,
    #[error("A required Consumer identity field is empty.")]
    EmptyIdentity,
    #[error("Consumer configuration patch is empty.")]
    EmptyPatch,
    #[error("Consumer configuration value is outside the supported range.")]
    InvalidConfigValue,
    #[error("Consumer diagnostic output budget must be between 1 byte and 256 KiB.")]
    InvalidDiagnosticBudget,
    #[error("At least one exact Consumer target is required.")]
    EmptyTargets,
    #[error("Selected Consumer targets are outside the authoritative target set.")]
    TargetsMismatch,
    #[error("Consumer mutation authorization is denied or unknown.")]
    Unauthorized,
}

fn non_empty(value: impl Into<String>) -> Result<String, ConsumerValidationError> {
    let value = value.into().trim().to_string();
    if value.is_empty() || value.chars().any(char::is_control) {
        Err(ConsumerValidationError::EmptyIdentity)
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(name: &str) -> ConsumerTargetIdentity {
        ConsumerTargetIdentity::parse("cluster-a", name, format!("{name}:10911")).expect("target")
    }

    fn group(
        index: usize,
        clients: ConsumerObservation<usize>,
        lag: ConsumerObservation<i64>,
    ) -> ConsumerGroupObservation {
        let connection_state = match clients.value() {
            Some(0) => ConsumerObservation::Complete(ConsumerConnectionState::Disconnected),
            Some(_) => ConsumerObservation::Complete(ConsumerConnectionState::Connected),
            None => ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::Unavailable,
            },
        };
        ConsumerGroupObservation {
            identity: ConsumerIdentity::parse(format!("orders-{index:02}")).expect("group"),
            category: ConsumerCategory::Application,
            connection_state,
            client_count: clients,
            lag,
            consume_type: ConsumerObservation::Complete("PUSH".into()),
            message_model: ConsumerObservation::Complete("CLUSTERING".into()),
            targets: vec![target("broker-a")],
        }
    }

    #[test]
    fn unknown_observation_is_not_zero_or_disconnected() {
        let item = group(
            1,
            ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::Unavailable,
            },
            ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::Unavailable,
            },
        );
        assert_eq!(item.client_count.value(), None);
        assert_eq!(item.lag.value(), None);
        assert!(matches!(item.connection_state, ConsumerObservation::Unknown { .. }));
    }

    #[test]
    fn filter_sort_and_page_preserve_negative_delta_and_clamp() {
        let mut items = (0..12)
            .map(|index| {
                group(
                    index,
                    ConsumerObservation::Complete(index),
                    ConsumerObservation::Complete(index as i64),
                )
            })
            .collect::<Vec<_>>();
        items[3].lag = ConsumerObservation::Complete(-7);
        let page = filter_sort_page_consumers(
            &items,
            &ConsumerFilterDraft {
                keyword: "orders".into(),
                connection: Some(ConsumerConnectionFilter::Connected),
                consume_type: Some("push".into()),
            },
            ConsumerSort {
                key: ConsumerSortKey::Lag,
                direction: ConsumerSortDirection::Ascending,
            },
            99,
        );
        assert_eq!(page.page, 2);
        assert_eq!(page.page_count, 2);
        assert_eq!(page.items.len(), 1);
        let all = filter_sort_page_consumers(
            &items,
            &ConsumerFilterDraft::default(),
            ConsumerSort {
                key: ConsumerSortKey::Lag,
                direction: ConsumerSortDirection::Ascending,
            },
            1,
        );
        assert_eq!(all.items[0].lag.value(), Some(&-7));
    }

    #[test]
    fn partial_observation_retains_failures_without_claiming_complete() {
        let observation = ConsumerObservation::Partial {
            value: 2usize,
            successful_target_count: 1,
            failures: vec![ConsumerTargetFailure {
                target: "broker-b".into(),
                stage: ConsumerFailureStage::Clients,
                code: ConsumerFailureCode::Unavailable,
                retryable: true,
            }],
        };
        assert_eq!(observation.state(), ConsumerObservationState::Partial);
        assert_eq!(observation.value(), Some(&2));
        assert!(!observation.is_complete());
    }

    #[test]
    fn proxy_capabilities_expose_only_truthful_forwarded_queries() {
        let capabilities = ConsumerCapabilities::for_scope(ConnectionScope::Proxy);
        assert_eq!(capabilities.inventory, CapabilityAvailability::Available);
        assert_eq!(capabilities.clients, CapabilityAvailability::Available);
        assert_eq!(capabilities.progress, CapabilityAvailability::Available);
        assert_eq!(capabilities.configuration, CapabilityAvailability::Unavailable);
        assert_eq!(capabilities.diagnostics, CapabilityAvailability::Unavailable);
        assert_eq!(capabilities.create, CapabilityAvailability::Unavailable);
        assert_eq!(capabilities.offset_actions, CapabilityAvailability::Unavailable);
    }

    #[test]
    fn cas_snapshot_carries_identity_generation_and_only_three_editable_entries() {
        let snapshot = ConsumerConfigSnapshot {
            identity: ConsumerConfigIdentity {
                group: ConsumerIdentity::parse("orders").expect("group"),
                target: target("broker-a"),
            },
            generation: 17,
            entries: ConsumerConfigEntries {
                retry_max_times: 16,
                retry_queue_nums: 1,
                consume_timeout_minutes: 15,
            },
        };
        let command = ConsumerConfigPatchCommand {
            snapshot,
            patch: ConsumerConfigPatch {
                retry_max_times: Some(8),
                retry_queue_nums: None,
                consume_timeout_minutes: Some(30),
            }
            .validate()
            .expect("patch"),
            authorization: ConsumerAclClassification::Authorized,
        }
        .validate()
        .expect("command");
        assert_eq!(command.snapshot.generation, 17);
        assert_eq!(command.patch.retry_max_times, Some(8));
    }

    #[test]
    fn diagnostics_are_bounded_redacted_in_debug_and_explicitly_clearable() {
        let request = ConsumerDiagnosticRequest {
            group: ConsumerIdentity::parse("orders").expect("group"),
            client: ConsumerClientIdentity::parse("10.0.0.1@client").expect("client"),
            kind: ConsumerDiagnosticKind::Jstack,
            max_output_bytes: CONSUMER_DIAGNOSTIC_MAX_BYTES,
        }
        .validate()
        .expect("request");
        assert!(ConsumerDiagnosticRequest {
            max_output_bytes: CONSUMER_DIAGNOSTIC_MAX_BYTES + 1,
            ..request
        }
        .validate()
        .is_err());

        let secret = "delivery-sensitive-stack";
        let mut payload =
            ConsumerDiagnosticPayload::new(vec![("consumerType".into(), "PUSH".into())], Some(secret.into()), true);
        assert!(!format!("{payload:?}").contains(secret));
        payload.clear();
        assert!(payload.properties().is_empty());
        assert_eq!(payload.text(), None);
        assert!(!payload.truncated());
    }

    #[test]
    fn unknown_acl_and_partial_mutation_fail_closed() {
        assert!(!ConsumerAclClassification::Unknown.permits_mutation());
        let outcome = ConsumerPartialOutcome {
            group: ConsumerIdentity::parse("orders").expect("group"),
            kind: ConsumerMutationKind::Delete,
            guarantee: ConsumerMutationGuarantee::PreflightBestEffort,
            targets: vec![
                ConsumerTargetOutcome {
                    target: "broker-a".into(),
                    stage: ConsumerFailureStage::Mutation,
                    applied: true,
                    failure: None,
                    retryable: false,
                },
                ConsumerTargetOutcome {
                    target: "broker-b".into(),
                    stage: ConsumerFailureStage::Mutation,
                    applied: false,
                    failure: Some(ConsumerFailureCode::Unavailable),
                    retryable: true,
                },
            ],
            reload_failed: false,
        };
        assert_eq!(outcome.applied_count(), 1);
        assert!(!outcome.is_complete_success());
    }
}
