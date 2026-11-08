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

//! Body-free dead-letter identities, current-page queries, and explicit recovery outcomes.

#[path = "dlq/csv.rs"]
mod csv;
pub use csv::*;

use crate::{
    ConsumerIdentity, MessageConsumeReceipt, MessageIdentity, MessageMetadataRow, MessagePageExtent,
    MessageSearchQuery, TopicIdentity,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

#[derive(Clone, PartialEq, Eq)]
pub struct DlqQuery {
    pub consumer_group: String,
    pub begin: i64,
    pub end: i64,
    pub message_id: Option<String>,
    pub page: u32,
    pub page_size: u32,
}
impl fmt::Debug for DlqQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DlqQuery")
            .field("page", &self.page)
            .finish_non_exhaustive()
    }
}
impl DlqQuery {
    pub fn topic(&self) -> String {
        format!("%DLQ%{}", self.consumer_group)
    }
    /// Checks the Group, inclusive UTC window, and real page request.
    ///
    /// # Errors
    /// Returns an error for invalid identity/window/page values or an exact-ID page after page one.
    pub fn validate(&self) -> Result<(), DlqIdentityError> {
        ConsumerIdentity::parse(self.consumer_group.clone()).map_err(|_| DlqIdentityError::Group)?;
        MessageSearchQuery::TopicRange {
            topic: self.topic(),
            begin: self.begin,
            end: self.end,
            page: self.page,
            page_size: self.page_size,
        }
        .validate()
        .map_err(|_| DlqIdentityError::Query)?;
        if let Some(id) = &self.message_id {
            MessageSearchQuery::MessageId {
                topic: self.topic(),
                message_id: id.clone(),
            }
            .validate()
            .map_err(|_| DlqIdentityError::Query)?;
            if self.page != 1 {
                return Err(DlqIdentityError::Query);
            }
        }
        Ok(())
    }
    /// Changes only the requested server page.
    ///
    /// # Errors
    /// Returns an error when the new page would violate [`Self::validate`].
    pub fn with_page(&self, page: u32) -> Result<Self, DlqIdentityError> {
        let query = Self { page, ..self.clone() };
        query.validate()?;
        Ok(query)
    }
}

/// An original tuple established from a message in one verified Group's DLQ.
/// Private fields prevent a UI from replacing a parsed original identity independently.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DlqResendTarget {
    source: MessageIdentity,
    original: MessageIdentity,
    consumer_group: String,
}
impl fmt::Debug for DlqResendTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DlqResendTarget { .. }")
    }
}
impl DlqResendTarget {
    /// Resolves only allowlisted original identity properties from a Group-verified DLQ message.
    ///
    /// The caller verifies the source Topic and physical ID against the actual lookup response.
    /// This function does not authorize contacting an address embedded in either identifier.
    ///
    /// # Errors
    /// Returns an error when the source Group or original identity is missing, invalid, or conflicting.
    pub fn resolve(
        group: &str,
        source: MessageIdentity,
        properties: &BTreeMap<String, String>,
    ) -> Result<Self, DlqIdentityError> {
        let group = ConsumerIdentity::parse(group).map_err(|_| DlqIdentityError::Group)?;
        if source.topic != format!("%DLQ%{}", group.as_str()) || source.message_id.trim().is_empty() {
            return Err(DlqIdentityError::Group);
        }
        if properties
            .get("ORIGIN_GROUP")
            .is_some_and(|value| !value.trim().is_empty() && value.trim() != group.as_str())
        {
            return Err(DlqIdentityError::Conflict);
        }
        if properties
            .get("STORE_MESSAGE_ID")
            .is_some_and(|value| !value.trim().is_empty() && !value.trim().eq_ignore_ascii_case(&source.message_id))
        {
            return Err(DlqIdentityError::Conflict);
        }
        let topic = properties
            .get("RETRY_TOPIC")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or(DlqIdentityError::MissingTopic)?;
        TopicIdentity::parse(topic).map_err(|_| DlqIdentityError::MissingTopic)?;
        if topic.starts_with("%DLQ%") || topic.starts_with("%RETRY%") {
            return Err(DlqIdentityError::Conflict);
        }
        let origin = properties
            .get("ORIGIN_MESSAGE_ID")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());
        let fallback = properties
            .get("DLQ_ORIGIN_MESSAGE_ID")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());
        if matches!((origin, fallback), (Some(a), Some(b)) if a != b) {
            return Err(DlqIdentityError::Conflict);
        }
        // Older Rust Brokers preserved the producer ID but did not serialize a physical origin ID.
        // Resolve that last resort only inside RETRY_TOPIC; explicit origin evidence stays primary.
        let id = origin
            .or(fallback)
            .or_else(|| properties.get("UNIQ_KEY").map(|s| s.trim()).filter(|s| !s.is_empty()))
            .ok_or(DlqIdentityError::MissingId)?;
        MessageSearchQuery::MessageId {
            topic: topic.into(),
            message_id: id.into(),
        }
        .validate()
        .map_err(|_| DlqIdentityError::MissingId)?;
        Ok(Self {
            source,
            original: MessageIdentity {
                topic: topic.into(),
                message_id: id.into(),
            },
            consumer_group: group.as_str().into(),
        })
    }
    pub fn source(&self) -> &MessageIdentity {
        &self.source
    }
    pub fn original(&self) -> &MessageIdentity {
        &self.original
    }
    pub fn consumer_group(&self) -> &str {
        &self.consumer_group
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DlqIdentityError {
    #[error("Select a valid Consumer Group and its DLQ.")]
    Group,
    #[error("Use a valid UTC range, Message ID and page size from 1 to 50.")]
    Query,
    #[error("Original Topic (RETRY_TOPIC) is missing or invalid.")]
    MissingTopic,
    #[error("Original Message ID is missing or invalid.")]
    MissingId,
    #[error("DLQ identity metadata conflicts; recovery is disabled.")]
    Conflict,
}
impl DlqIdentityError {
    pub const fn summary(self) -> &'static str {
        match self {
            Self::Group => "Select a valid Consumer Group and its DLQ.",
            Self::Query => "Use a valid UTC range, Message ID and page size from 1 to 50.",
            Self::MissingTopic => "Original Topic (RETRY_TOPIC) is missing or invalid.",
            Self::MissingId => "Original Message ID is missing or invalid.",
            Self::Conflict => "DLQ identity metadata conflicts; recovery is disabled.",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DlqRow {
    pub metadata: MessageMetadataRow,
    pub target: Result<DlqResendTarget, DlqIdentityError>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DlqPage {
    pub rows: Vec<DlqRow>,
    pub extent: MessagePageExtent,
}

/// Deduplicates only exact original Topic/Group/ID tuples, preserving page order.
pub fn dlq_selected_targets(rows: &[DlqRow], selected: &BTreeSet<MessageIdentity>) -> Vec<DlqResendTarget> {
    let mut tuples = BTreeSet::new();
    rows.iter()
        .filter(|row| selected.contains(&row.metadata.identity))
        .filter_map(|row| row.target.as_ref().ok())
        .filter(|target| tuples.insert((target.original.clone(), target.consumer_group.clone())))
        .cloned()
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DlqEntryState {
    Succeeded,
    BusinessFailure,
    /// The call failed before direct consumption was sent.
    CallFailed,
    /// Dispatch may have reached the Consumer. It must never be automatically replayed.
    Unknown,
    /// Local acceptance failed, so this entry was not attempted.
    Unattempted,
}
impl DlqEntryState {
    pub fn can_retry(self) -> bool {
        matches!(self, Self::BusinessFailure | Self::CallFailed)
    }
    pub const fn label(self) -> &'static str {
        match self {
            Self::Succeeded => "Succeeded",
            Self::BusinessFailure => "Consumer declined",
            Self::CallFailed => "Call failed before dispatch",
            Self::Unknown => "Unconfirmed — verify before any new attempt",
            Self::Unattempted => "Not attempted",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DlqAttempt {
    pub state: DlqEntryState,
    pub receipt: Option<MessageConsumeReceipt>,
}
impl DlqAttempt {
    pub fn from_receipt(receipt: MessageConsumeReceipt) -> Self {
        let state = if receipt.is_success() {
            DlqEntryState::Succeeded
        } else if matches!(
            receipt.code,
            crate::MessageConsumeCode::Unknown | crate::MessageConsumeCode::Success
        ) {
            DlqEntryState::Unknown
        } else {
            DlqEntryState::BusinessFailure
        };
        Self {
            state,
            receipt: Some(receipt),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DlqEntryResult {
    pub target: DlqResendTarget,
    pub attempt: DlqAttempt,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DlqBatchResult {
    pub entries: Vec<DlqEntryResult>,
}
impl DlqBatchResult {
    pub fn retryable_targets(&self) -> Vec<DlqResendTarget> {
        self.entries
            .iter()
            .filter(|entry| entry.attempt.state.can_retry())
            .map(|entry| entry.target.clone())
            .collect()
    }
}

#[cfg(test)]
#[path = "dlq/tests.rs"]
mod tests;
