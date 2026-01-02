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

#[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
pub enum BoundaryType {
    #[default]
    Lower,
    Upper,
}

impl BoundaryType {
    /// Returns the name of the boundary type.
    ///
    /// # Returns
    /// * "lower" for `BoundaryType::Lower`
    /// * "upper" for `BoundaryType::Upper`
    pub fn get_name(&self) -> &'static str {
        match self {
            BoundaryType::Lower => "lower",
            BoundaryType::Upper => "upper",
        }
    }

    /// Parses a boundary type from a string name.
    ///
    /// Matches Java behavior: if the name equals "upper" (case-insensitive),
    /// returns `BoundaryType::Upper`; otherwise returns `BoundaryType::Lower` as default.
    ///
    /// # Arguments
    /// * `name` - The string name to parse (case-insensitive)
    ///
    /// # Returns
    /// * `BoundaryType::Upper` if name equals "upper" (case-insensitive)
    /// * `BoundaryType::Lower` otherwise (default fallback, matching Java behavior)
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
        // Matches Java enum serialization: uses uppercase enum name
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
        let s = String::deserialize(deserializer)?;
        // Matches Java behavior: returns Lower as default for any invalid value
        Ok(BoundaryType::get_type(&s))
    }
}

impl fmt::Display for BoundaryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display as uppercase to match Java enum.name()
        match self {
            BoundaryType::Lower => write!(f, "LOWER"),
            BoundaryType::Upper => write!(f, "UPPER"),
        }
    }
}

impl FromStr for BoundaryType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Always succeeds, returns Lower as default (matches Java behavior)
        Ok(BoundaryType::get_type(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundary_type_get_name_returns_correct_strings() {
        assert_eq!(BoundaryType::Lower.get_name(), "lower");
        assert_eq!(BoundaryType::Upper.get_name(), "upper");
    }

    #[test]
    fn boundary_type_get_type_parses_correctly() {
        assert_eq!(BoundaryType::get_type("lower"), BoundaryType::Lower);
        assert_eq!(BoundaryType::get_type("LOWER"), BoundaryType::Lower);
        assert_eq!(BoundaryType::get_type("upper"), BoundaryType::Upper);
        assert_eq!(BoundaryType::get_type("UPPER"), BoundaryType::Upper);
        assert_eq!(BoundaryType::get_type("Upper"), BoundaryType::Upper);
    }

    #[test]
    fn boundary_type_get_type_returns_lower_as_default() {
        // Matches Java behavior: invalid values default to Lower
        assert_eq!(BoundaryType::get_type("invalid"), BoundaryType::Lower);
        assert_eq!(BoundaryType::get_type(""), BoundaryType::Lower);
        assert_eq!(BoundaryType::get_type("random"), BoundaryType::Lower);
    }

    #[test]
    fn boundary_type_default_is_lower() {
        assert_eq!(BoundaryType::default(), BoundaryType::Lower);
    }

    #[test]
    fn boundary_type_serializes_to_string() {
        let lower = BoundaryType::Lower;
        let upper = BoundaryType::Upper;

        let lower_json = serde_json::to_string(&lower).unwrap();
        let upper_json = serde_json::to_string(&upper).unwrap();

        // Matches Java enum serialization: uppercase enum names
        assert_eq!(lower_json, r#""LOWER""#);
        assert_eq!(upper_json, r#""UPPER""#);
    }

    #[test]
    fn boundary_type_deserializes_from_string() {
        // Supports both uppercase (Java format) and lowercase
        let lower: BoundaryType = serde_json::from_str(r#""LOWER""#).unwrap();
        let upper: BoundaryType = serde_json::from_str(r#""UPPER""#).unwrap();
        let lower2: BoundaryType = serde_json::from_str(r#""lower""#).unwrap();
        let upper2: BoundaryType = serde_json::from_str(r#""upper""#).unwrap();

        assert_eq!(lower, BoundaryType::Lower);
        assert_eq!(upper, BoundaryType::Upper);
        assert_eq!(lower2, BoundaryType::Lower);
        assert_eq!(upper2, BoundaryType::Upper);
    }

    #[test]
    fn boundary_type_deserializes_case_insensitive() {
        // Case-insensitive deserialization
        let upper1: BoundaryType = serde_json::from_str(r#""Upper""#).unwrap();
        let upper2: BoundaryType = serde_json::from_str(r#""UpPeR""#).unwrap();

        assert_eq!(upper1, BoundaryType::Upper);
        assert_eq!(upper2, BoundaryType::Upper);
    }

    #[test]
    fn boundary_type_deserialization_defaults_to_lower_for_invalid_value() {
        // Matches Java behavior: invalid values default to Lower
        let result: BoundaryType = serde_json::from_str(r#""invalid""#).unwrap();
        assert_eq!(result, BoundaryType::Lower);

        let result2: BoundaryType = serde_json::from_str(r#""random""#).unwrap();
        assert_eq!(result2, BoundaryType::Lower);
    }

    #[test]
    fn boundary_type_roundtrip_serialization() {
        let original = BoundaryType::Upper;
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: BoundaryType = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn boundary_type_display_returns_uppercase() {
        assert_eq!(format!("{}", BoundaryType::Lower), "LOWER");
        assert_eq!(format!("{}", BoundaryType::Upper), "UPPER");
    }

    #[test]
    fn boundary_type_from_str_parses_correctly() {
        assert_eq!("UPPER".parse::<BoundaryType>().unwrap(), BoundaryType::Upper);
        assert_eq!("upper".parse::<BoundaryType>().unwrap(), BoundaryType::Upper);
        assert_eq!("LOWER".parse::<BoundaryType>().unwrap(), BoundaryType::Lower);
        assert_eq!("lower".parse::<BoundaryType>().unwrap(), BoundaryType::Lower);
    }

    #[test]
    fn boundary_type_from_str_defaults_to_lower_for_invalid() {
        assert_eq!("invalid".parse::<BoundaryType>().unwrap(), BoundaryType::Lower);
        assert_eq!("".parse::<BoundaryType>().unwrap(), BoundaryType::Lower);
    }
}
