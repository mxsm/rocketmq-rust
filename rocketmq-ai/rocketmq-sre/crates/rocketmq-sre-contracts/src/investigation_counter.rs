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

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::InvestigationRunFailure;

// Decimal strings preserve u64 identity through JavaScript and JSON consumers.
macro_rules! investigation_counter {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Copy, Debug, Default, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(
            #[serde(with = "decimal_u64")]
            #[schemars(with = "String", regex(pattern = "^(0|[1-9][0-9]{0,19})$"))]
            u64,
        );

        impl $name {
            #[must_use]
            pub const fn from_u64(value: u64) -> Self {
                Self(value)
            }

            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }

            pub(crate) fn next(self) -> Result<Self, InvestigationRunFailure> {
                self.0
                    .checked_add(1)
                    .map(Self)
                    .ok_or(InvestigationRunFailure::CounterExhausted)
            }
        }
    };
}

investigation_counter!(
    InvestigationRevision,
    "Monotonic run revision, serialized as a lossless decimal string."
);
investigation_counter!(
    InvestigationControlEpoch,
    "Control generation, serialized as a lossless decimal string."
);

mod decimal_u64 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.is_empty()
            || value.len() > 20
            || (value.len() > 1 && value.starts_with('0'))
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(serde::de::Error::custom("invalid investigation counter"));
        }
        value
            .parse()
            .map_err(|_| serde::de::Error::custom("invalid investigation counter"))
    }
}
