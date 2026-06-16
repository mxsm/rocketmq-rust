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
use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

#[repr(i32)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceExporterType {
    #[default]
    Disable = 0,
    OtlpGrpc = 1,
    Log = 2,
}

impl TraceExporterType {
    #[inline]
    pub fn value(self) -> i32 {
        self as i32
    }

    #[inline]
    pub fn is_enable(self) -> bool {
        self as i32 > 0
    }

    #[inline]
    pub fn from_value(value: i32) -> Self {
        match value {
            1 => Self::OtlpGrpc,
            2 => Self::Log,
            _ => Self::Disable,
        }
    }
}

impl fmt::Display for TraceExporterType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Disable => "disable",
            Self::OtlpGrpc => "otlp_grpc",
            Self::Log => "log",
        };
        write!(f, "{s}")
    }
}

impl FromStr for TraceExporterType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "disable" | "off" | "none" => Ok(Self::Disable),
            "otlp_grpc" | "otlp" => Ok(Self::OtlpGrpc),
            "log" => Ok(Self::Log),
            _ => Err(()),
        }
    }
}

impl TryFrom<i32> for TraceExporterType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Disable),
            1 => Ok(Self::OtlpGrpc),
            2 => Ok(Self::Log),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn maps_values() {
        assert_eq!(TraceExporterType::from_value(0), TraceExporterType::Disable);
        assert_eq!(TraceExporterType::from_value(1), TraceExporterType::OtlpGrpc);
        assert_eq!(TraceExporterType::from_value(2), TraceExporterType::Log);
        assert_eq!(TraceExporterType::from_value(100), TraceExporterType::Disable);
    }

    #[test]
    fn reports_enabled_state() {
        assert!(!TraceExporterType::Disable.is_enable());
        assert!(TraceExporterType::OtlpGrpc.is_enable());
        assert!(TraceExporterType::Log.is_enable());
    }

    #[test]
    fn parses_aliases() {
        assert_eq!(
            TraceExporterType::from_str("disable").unwrap(),
            TraceExporterType::Disable
        );
        assert_eq!(
            TraceExporterType::from_str("otlp").unwrap(),
            TraceExporterType::OtlpGrpc
        );
        assert_eq!(TraceExporterType::from_str("log").unwrap(), TraceExporterType::Log);
        assert!(TraceExporterType::from_str("unknown").is_err());
    }

    #[test]
    fn serializes_as_snake_case() {
        let json = serde_json::to_string(&TraceExporterType::OtlpGrpc).unwrap();

        assert_eq!(json, "\"otlp_grpc\"");
        assert_eq!(
            serde_json::from_str::<TraceExporterType>(&json).unwrap(),
            TraceExporterType::OtlpGrpc
        );
    }
}
