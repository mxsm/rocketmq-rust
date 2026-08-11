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

use serde::Deserialize;
use serde::Serialize;

/// Selects the default Store semantics applied to fields omitted from configuration files.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum StoreCompatibilityProfile {
    /// Preserves the historical rocketmq-rust defaults for existing deployments.
    #[default]
    #[serde(rename = "LEGACY_RUST")]
    LegacyRust,
    /// Applies the Apache RocketMQ Java 5.5 Store defaults.
    #[serde(rename = "JAVA_5_5")]
    Java55,
    /// Applies Java 5.5 defaults with synchronous flush and fail-closed replica ACKs.
    #[serde(rename = "DURABILITY_STRICT")]
    DurabilityStrict,
}

impl StoreCompatibilityProfile {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LegacyRust => "LEGACY_RUST",
            Self::Java55 => "JAVA_5_5",
            Self::DurabilityStrict => "DURABILITY_STRICT",
        }
    }

    #[must_use]
    pub const fn is_legacy(self) -> bool {
        matches!(self, Self::LegacyRust)
    }
}

impl fmt::Display for StoreCompatibilityProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_profile_names_round_trip() {
        for (profile, encoded) in [
            (StoreCompatibilityProfile::LegacyRust, "\"LEGACY_RUST\""),
            (StoreCompatibilityProfile::Java55, "\"JAVA_5_5\""),
            (StoreCompatibilityProfile::DurabilityStrict, "\"DURABILITY_STRICT\""),
        ] {
            assert_eq!(serde_json::to_string(&profile).expect("serialize profile"), encoded);
            assert_eq!(
                serde_json::from_str::<StoreCompatibilityProfile>(encoded).expect("deserialize profile"),
                profile
            );
        }
    }

    #[test]
    fn unknown_profile_is_rejected() {
        assert!(serde_json::from_str::<StoreCompatibilityProfile>("\"DLedger\"").is_err());
    }
}
