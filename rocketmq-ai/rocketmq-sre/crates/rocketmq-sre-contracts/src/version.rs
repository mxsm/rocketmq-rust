// Copyright 2023 The RocketMQ Rust Authors
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

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ContractError;

/// Version negotiation information for a serialized contract family.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct SchemaVersion {
    pub family: String,
    pub major: u16,
    pub minor: u16,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub required_features: BTreeSet<String>,
}

impl SchemaVersion {
    /// Creates a version with no required optional features.
    #[must_use]
    pub fn new(family: impl Into<String>, major: u16, minor: u16) -> Self {
        Self {
            family: family.into(),
            major,
            minor,
            required_features: BTreeSet::new(),
        }
    }

    /// Adds features that a consumer must understand before processing data.
    #[must_use]
    pub fn requiring(mut self, features: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.required_features.extend(features.into_iter().map(Into::into));
        self
    }

    /// Enforces fail-closed compatibility for family, major, and features.
    ///
    /// A newer minor remains compatible as long as every required feature is
    /// supported. Unknown majors and required features are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`ContractError::UnsupportedSchemaFamily`],
    /// [`ContractError::UnsupportedSchemaMajor`], or
    /// [`ContractError::MissingRequiredFeature`] on incompatibility.
    pub fn ensure_compatible(
        &self,
        supported_family: &str,
        supported_major: u16,
        supported_features: &BTreeSet<String>,
    ) -> Result<(), ContractError> {
        if self.family != supported_family {
            return Err(ContractError::UnsupportedSchemaFamily {
                actual: self.family.clone(),
                supported: supported_family.to_owned(),
            });
        }
        if self.major != supported_major {
            return Err(ContractError::UnsupportedSchemaMajor {
                family: self.family.clone(),
                actual: self.major,
                supported: supported_major,
            });
        }
        if let Some(feature) = self
            .required_features
            .iter()
            .find(|feature| !supported_features.contains(*feature))
        {
            return Err(ContractError::MissingRequiredFeature {
                feature: feature.clone(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn accepts_newer_minor_without_unknown_required_features() {
        let version = SchemaVersion::new("rocketmq-sre.evidence", 1, 99);

        assert!(
            version
                .ensure_compatible("rocketmq-sre.evidence", 1, &BTreeSet::new())
                .is_ok()
        );
    }

    #[test]
    fn rejects_unknown_major_and_required_feature() {
        let major_error = SchemaVersion::new("rocketmq-sre.evidence", 2, 0).ensure_compatible(
            "rocketmq-sre.evidence",
            1,
            &BTreeSet::new(),
        );
        assert!(matches!(major_error, Err(ContractError::UnsupportedSchemaMajor { .. })));

        let feature_error = SchemaVersion::new("rocketmq-sre.evidence", 1, 0)
            .requiring(["unknown"])
            .ensure_compatible("rocketmq-sre.evidence", 1, &BTreeSet::new());
        assert!(matches!(
            feature_error,
            Err(ContractError::MissingRequiredFeature { .. })
        ));
    }
}
