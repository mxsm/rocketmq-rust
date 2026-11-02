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

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{StorageError, StorageErrorKind};

const MAX_ACTIVE_PER_USER: usize = 128;
const MAX_RETAINED_SESSIONS: usize = 10_000;
const RETAIN_ENDED_MS: u64 = 30 * 24 * 60 * 60 * 1_000;

/// Random native session identity. Formatting never exposes the capability.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SessionId(String);

impl SessionId {
    pub fn generate() -> Self {
        Self(format!("session-{:032x}", rand::random::<u128>()))
    }

    /// Parses only the persisted opaque identity shape, without retaining rejected input.
    pub fn parse(value: String) -> Result<Self, StorageError> {
        if value.len() != 40
            || !value.starts_with("session-")
            || !value[8..].bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(StorageError::new(StorageErrorKind::Validation));
        }
        Ok(Self(value))
    }
}

impl fmt::Debug for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionId([REDACTED])")
    }
}

impl TryFrom<String> for SessionId {
    type Error = StorageError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<SessionId> for String {
    fn from(value: SessionId) -> Self {
        value.0
    }
}

/// Persistent metadata; no passwords, Admin credentials, or connection URLs are represented.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SessionRecord {
    pub id: SessionId,
    sequence: u64,
    pub username: String,
    pub created_at_epoch_ms: u64,
    pub expires_at_epoch_ms: u64,
    pub last_seen_at_epoch_ms: u64,
    pub revoked_at_epoch_ms: Option<u64>,
}

impl SessionRecord {
    pub fn new(username: String, created_at_epoch_ms: u64, expires_at_epoch_ms: u64) -> Result<Self, StorageError> {
        let record = Self {
            id: SessionId::generate(),
            sequence: 0,
            username,
            created_at_epoch_ms,
            expires_at_epoch_ms,
            last_seen_at_epoch_ms: created_at_epoch_ms,
            revoked_at_epoch_ms: None,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn is_active(&self, now_epoch_ms: u64) -> bool {
        self.revoked_at_epoch_ms.is_none() && self.expires_at_epoch_ms > now_epoch_ms
    }

    fn validate(&self) -> Result<(), StorageError> {
        validate_username(&self.username)?;
        if self.expires_at_epoch_ms <= self.created_at_epoch_ms
            || self.last_seen_at_epoch_ms < self.created_at_epoch_ms
            || self.last_seen_at_epoch_ms >= self.expires_at_epoch_ms
        {
            return Err(StorageError::new(StorageErrorKind::Validation));
        }
        Ok(())
    }
}

/// Cursor binds a stable creation boundary and the username filter to a query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionCursor {
    username: Option<String>,
    high_watermark: u64,
    before: u64,
}

#[derive(Clone, Debug)]
pub struct SessionQuery {
    pub username: Option<String>,
    pub cursor: Option<SessionCursor>,
    pub limit: usize,
}

impl Default for SessionQuery {
    fn default() -> Self {
        Self {
            username: None,
            cursor: None,
            limit: 50,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionPage {
    pub sessions: Vec<SessionRecord>,
    pub next_cursor: Option<SessionCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SessionDocument {
    pub(crate) schema_version: u32,
    next_sequence: u64,
    pub(crate) sessions: Vec<SessionRecord>,
}

impl SessionDocument {
    pub(crate) fn empty() -> Self {
        Self {
            schema_version: 1,
            next_sequence: 1,
            sessions: Vec::new(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        if self.schema_version != 1 {
            return Err(StorageError::unsupported_schema(self.schema_version, 1));
        }
        let mut identities = std::collections::BTreeSet::new();
        let mut sequences = std::collections::BTreeSet::new();
        if self.next_sequence == 0 || self.sessions.len() > MAX_RETAINED_SESSIONS {
            return Err(StorageError::new(StorageErrorKind::InvalidDocument));
        }
        for record in &self.sessions {
            record.validate()?;
            if !identities.insert(&record.id)
                || record.sequence == 0
                || record.sequence >= self.next_sequence
                || !sequences.insert(record.sequence)
            {
                return Err(StorageError::new(StorageErrorKind::InvalidDocument));
            }
        }
        Ok(())
    }

    pub(crate) fn create(&mut self, mut session: SessionRecord, now: u64) -> Result<(), StorageError> {
        session.validate()?;
        self.sessions.retain(|record| {
            record.revoked_at_epoch_ms.unwrap_or(record.expires_at_epoch_ms) >= now.saturating_sub(RETAIN_ENDED_MS)
        });
        if !session.is_active(now)
            || self.sessions.iter().any(|record| record.id == session.id)
            || self.sessions.len() >= MAX_RETAINED_SESSIONS
            || self
                .sessions
                .iter()
                .filter(|record| record.username == session.username && record.is_active(now))
                .count()
                >= MAX_ACTIVE_PER_USER
        {
            return Err(StorageError::conflict());
        }
        session.sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| StorageError::new(StorageErrorKind::RevisionExhausted))?;
        self.sessions.push(session);
        Ok(())
    }

    pub(crate) fn access(&mut self, id: &SessionId, now: u64) -> Option<SessionRecord> {
        let record = self
            .sessions
            .iter_mut()
            .find(|record| &record.id == id && record.is_active(now))?;
        record.last_seen_at_epoch_ms = record.last_seen_at_epoch_ms.max(now);
        Some(record.clone())
    }

    pub(crate) fn revoke(&mut self, id: &SessionId, now: u64) -> bool {
        if let Some(record) = self
            .sessions
            .iter_mut()
            .find(|record| &record.id == id && record.is_active(now))
        {
            record.revoked_at_epoch_ms = Some(now);
            true
        } else {
            false
        }
    }

    pub(crate) fn revoke_all(&mut self, username: &str, now: u64) -> Result<usize, StorageError> {
        validate_username(username)?;
        let mut count = 0;
        for record in &mut self.sessions {
            if record.username == username && record.is_active(now) {
                record.revoked_at_epoch_ms = Some(now);
                count += 1;
            }
        }
        Ok(count)
    }

    pub(crate) fn query(&self, query: SessionQuery) -> Result<SessionPage, StorageError> {
        if query.limit == 0
            || query.limit > 100
            || query
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.username != query.username)
        {
            return Err(StorageError::new(StorageErrorKind::Validation));
        }
        if let Some(username) = &query.username {
            validate_username(username)?;
        }
        let mut sessions: Vec<_> = self
            .sessions
            .iter()
            .filter(|record| {
                query
                    .username
                    .as_ref()
                    .is_none_or(|username| &record.username == username)
            })
            .cloned()
            .collect();
        sessions.sort_by_key(|record| std::cmp::Reverse(record.sequence));
        let high_watermark = query
            .cursor
            .as_ref()
            .map_or(self.next_sequence - 1, |cursor| cursor.high_watermark);
        if let Some(cursor) = query.cursor {
            sessions.retain(|record| record.sequence <= cursor.high_watermark && record.sequence < cursor.before);
        }
        let more = sessions.len() > query.limit;
        sessions.truncate(query.limit);
        let next_cursor = if more {
            sessions.last().map(|record| SessionCursor {
                username: query.username,
                high_watermark,
                before: record.sequence,
            })
        } else {
            None
        };
        Ok(SessionPage { sessions, next_cursor })
    }
}

fn validate_username(username: &str) -> Result<(), StorageError> {
    if username.trim().is_empty() || username.len() > 256 || username.chars().any(char::is_control) {
        return Err(StorageError::new(StorageErrorKind::Validation));
    }
    Ok(())
}
