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

//! P11 accepted and terminal audit transitions, shared without changing journal semantics.

use crate::StorageError;
use rocketmq_dashboard_common::{
    AuditAcceptedOperation, AuditCursor, AuditEvent, AuditFacts, AuditOutcome, AuditPage, AuditQuery,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_AUDIT_EVENTS: usize = 10_000;
pub const MAX_PENDING_AUDIT_OPERATIONS: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AuditJournal {
    next_event_id: u64,
    #[serde(default)]
    retention_generation: u64,
    pending: Vec<AuditAcceptedOperation>,
    events: Vec<AuditEvent>,
}

impl Default for AuditJournal {
    fn default() -> Self {
        Self {
            next_event_id: 1,
            retention_generation: 0,
            pending: Vec::new(),
            events: Vec::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum AuditJournalError {
    #[error("The local audit journal has too many operations awaiting verification.")]
    PendingCapacity,
    #[error("The audit request was already accepted; the business operation must not be replayed.")]
    AlreadyAccepted,
    #[error("The local audit journal is inconsistent.")]
    InvalidJournal,
    #[error("The audit operation does not match the accepted request.")]
    AttributionMismatch,
    #[error("Audit event identities are exhausted.")]
    IdentityExhausted,
    #[error("Audit retention generations are exhausted.")]
    RetentionExhausted,
    #[error("The audit page expired because its retained events were removed; restart the query.")]
    ExpiredCursor,
}

impl AuditJournal {
    pub fn pending(&self) -> &[AuditAcceptedOperation] {
        &self.pending
    }

    pub fn finish_local(
        &mut self,
        context: rocketmq_dashboard_common::OperationContext,
        action: rocketmq_dashboard_common::AuditAction,
        timestamp_epoch_ms: u64,
    ) -> Result<(), StorageError> {
        let accepted = self
            .pending
            .iter()
            .find(|operation| operation.context.request_id == context.request_id);
        if accepted.is_some_and(|operation| operation.context != context || operation.action != action) {
            return Err(StorageError::validation(AuditJournalError::AttributionMismatch));
        }
        let operation = accepted.cloned().unwrap_or(AuditAcceptedOperation {
            context,
            action,
            accepted_at_epoch_ms: timestamp_epoch_ms,
            topic_fields: Default::default(),
            consumer_fields: Default::default(),
        });
        self.finish(
            operation,
            AuditOutcome::Succeeded,
            AuditFacts::default(),
            timestamp_epoch_ms,
        )
    }

    pub fn validate(&self) -> Result<(), StorageError> {
        let mut requests = BTreeSet::new();
        let mut previous = 0;
        if self.pending.len() > MAX_PENDING_AUDIT_OPERATIONS
            || self.events.len() > MAX_AUDIT_EVENTS
            || self.next_event_id == 0
        {
            return Err(StorageError::validation(AuditJournalError::InvalidJournal));
        }
        for operation in &self.pending {
            operation.validate().map_err(StorageError::validation)?;
            if !requests.insert(&operation.context.request_id) {
                return Err(StorageError::validation(AuditJournalError::InvalidJournal));
            }
        }
        for event in &self.events {
            event.operation.validate().map_err(StorageError::validation)?;
            event.facts.validate().map_err(StorageError::validation)?;
            if !requests.insert(&event.operation.context.request_id)
                || event.event_id <= previous
                || event.event_id >= self.next_event_id
            {
                return Err(StorageError::validation(AuditJournalError::InvalidJournal));
            }
            previous = event.event_id;
        }
        Ok(())
    }

    pub fn accept(&mut self, operation: AuditAcceptedOperation) -> Result<(), StorageError> {
        operation.validate().map_err(StorageError::validation)?;
        let request_id = &operation.context.request_id;
        if self.pending.iter().any(|item| &item.context.request_id == request_id)
            || self
                .events
                .iter()
                .any(|item| &item.operation.context.request_id == request_id)
        {
            return Err(StorageError::validation(AuditJournalError::AlreadyAccepted));
        }
        if self.pending.len() >= MAX_PENDING_AUDIT_OPERATIONS {
            return Err(StorageError::validation(AuditJournalError::PendingCapacity));
        }
        self.pending.push(operation);
        Ok(())
    }

    pub fn finish(
        &mut self,
        operation: AuditAcceptedOperation,
        outcome: AuditOutcome,
        facts: AuditFacts,
        timestamp_epoch_ms: u64,
    ) -> Result<(), StorageError> {
        operation.validate().map_err(StorageError::validation)?;
        facts.validate().map_err(StorageError::validation)?;
        let matches = |item: &AuditAcceptedOperation| item.context.request_id == operation.context.request_id;
        if let Some(event) = self.events.iter().find(|event| matches(&event.operation)) {
            // Local commits already record their durable result before provider refresh completes.
            // A later observer or audit-only retry cannot replace that first terminal fact.
            return if event.operation.context == operation.context && event.operation.action == operation.action {
                Ok(())
            } else {
                Err(StorageError::validation(AuditJournalError::AttributionMismatch))
            };
        }
        if self
            .pending
            .iter()
            .find(|item| matches(item))
            .is_some_and(|accepted| accepted != &operation)
        {
            return Err(StorageError::validation(AuditJournalError::AttributionMismatch));
        }
        let next = self
            .next_event_id
            .checked_add(1)
            .ok_or_else(|| StorageError::validation(AuditJournalError::IdentityExhausted))?;
        self.pending.retain(|item| !matches(item));
        self.events.push(AuditEvent {
            event_id: self.next_event_id,
            operation,
            outcome,
            timestamp_epoch_ms,
            facts,
        });
        self.next_event_id = next;
        let before_retention = self.events.len();
        let cutoff = timestamp_epoch_ms.saturating_sub(rocketmq_dashboard_common::AuditFilter::MAX_WINDOW_MS);
        self.events.retain(|event| event.timestamp_epoch_ms >= cutoff);
        if self.events.len() > MAX_AUDIT_EVENTS {
            self.events.drain(..self.events.len() - MAX_AUDIT_EVENTS);
        }
        if self.events.len() < before_retention {
            self.retention_generation = self
                .retention_generation
                .checked_add(1)
                .ok_or_else(|| StorageError::validation(AuditJournalError::RetentionExhausted))?;
        }
        Ok(())
    }

    pub fn query(&self, query: AuditQuery) -> Result<AuditPage, StorageError> {
        query.validate().map_err(StorageError::validation)?;
        let high_watermark = query
            .cursor
            .as_ref()
            .map_or(self.next_event_id - 1, |cursor| cursor.high_watermark);
        let before = query.cursor.as_ref().map(|cursor| cursor.before_event_id);
        if query
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.retention_generation != self.retention_generation)
        {
            return Err(StorageError::validation(AuditJournalError::ExpiredCursor));
        }
        let mut matching = self.events.iter().rev().filter(|event| {
            event.event_id <= high_watermark
                && before.is_none_or(|before| event.event_id < before)
                && query.filter.matches(event)
        });
        let events: Vec<_> = matching.by_ref().take(query.limit).cloned().collect();
        let next_cursor = if matching.next().is_some() {
            events.last().map(|event| AuditCursor {
                filter: query.filter,
                high_watermark,
                before_event_id: event.event_id,
                retention_generation: self.retention_generation,
            })
        } else {
            None
        };
        Ok(AuditPage { events, next_cursor })
    }
}

#[cfg(test)]
#[path = "audit_tests.rs"]
mod tests;
