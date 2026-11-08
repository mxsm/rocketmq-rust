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

//! Persistent environment identity, independent of endpoint and session revisions.

use serde::{Deserialize, Serialize};

use crate::ConnectionSnapshot;

/// An opaque, stable identifier assigned when a connection profile is created.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EnvironmentId(String);

impl EnvironmentId {
    /// Parses a persisted identity. Endpoint strings are never accepted as identities.
    ///
    /// # Errors
    /// Returns an error for empty, oversized, or non-identifier input.
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidEnvironmentId> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(InvalidEnvironmentId);
        }
        Ok(Self(value))
    }

    /// Returns the persisted opaque identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for EnvironmentId {
    type Error = InvalidEnvironmentId;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<EnvironmentId> for String {
    fn from(value: EnvironmentId) -> Self {
        value.0
    }
}

/// Invalid persistent environment identity, without retaining the rejected value.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("the environment identifier is invalid")]
pub struct InvalidEnvironmentId;

/// A connection generation together with its persistent data owner.
///
/// This wrapper preserves the original [`ConnectionSnapshot`] source and wire contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvironmentConnectionSnapshot {
    /// Persistent profile identity.
    pub environment_id: EnvironmentId,
    /// Immutable transport and endpoint generation.
    pub connection: ConnectionSnapshot,
}
