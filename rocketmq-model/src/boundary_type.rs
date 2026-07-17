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

use std::fmt;
use std::str::FromStr;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

/// Selects the lower or upper offset when timestamps map to a range.
#[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
pub enum BoundaryType {
    #[default]
    Lower,
    Upper,
}

impl BoundaryType {
    /// Returns the lower-case boundary name used by RocketMQ APIs.
    pub fn get_name(&self) -> &'static str {
        match self {
            BoundaryType::Lower => "lower",
            BoundaryType::Upper => "upper",
        }
    }

    /// Parses a boundary name using the Java-compatible fallback behavior.
    ///
    /// Values other than `upper`, matched without case sensitivity, resolve to
    /// [`BoundaryType::Lower`].
    pub fn get_type(name: &str) -> BoundaryType {
        if name.eq_ignore_ascii_case("upper") {
            BoundaryType::Upper
        } else {
            BoundaryType::Lower
        }
    }
}

impl Serialize for BoundaryType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let name = match self {
            BoundaryType::Lower => "LOWER",
            BoundaryType::Upper => "UPPER",
        };
        serializer.serialize_str(name)
    }
}

impl<'de> Deserialize<'de> for BoundaryType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(BoundaryType::get_type(&value))
    }
}

impl fmt::Display for BoundaryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BoundaryType::Lower => write!(f, "LOWER"),
            BoundaryType::Upper => write!(f, "UPPER"),
        }
    }
}

impl FromStr for BoundaryType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(BoundaryType::get_type(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_parsing_preserve_java_compatibility() {
        assert_eq!(BoundaryType::Lower.get_name(), "lower");
        assert_eq!(BoundaryType::Upper.get_name(), "upper");
        assert_eq!(BoundaryType::get_type("UPPER"), BoundaryType::Upper);
        assert_eq!(BoundaryType::get_type("invalid"), BoundaryType::Lower);
        assert_eq!("upper".parse::<BoundaryType>().unwrap(), BoundaryType::Upper);
    }

    #[test]
    fn serde_and_display_use_uppercase_names() {
        let encoded = serde_json::to_string(&BoundaryType::Upper).unwrap();
        assert_eq!(encoded, r#""UPPER""#);
        assert_eq!(
            serde_json::from_str::<BoundaryType>(&encoded).unwrap(),
            BoundaryType::Upper
        );
        assert_eq!(format!("{}", BoundaryType::Lower), "LOWER");
    }

    #[test]
    fn invalid_serialized_values_fall_back_to_lower() {
        assert_eq!(
            serde_json::from_str::<BoundaryType>(r#""unknown""#).unwrap(),
            BoundaryType::Lower
        );
    }
}
