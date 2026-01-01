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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricsExporterType {
    Disable = 0,
    OtlpGrpc = 1,
    Prom = 2,
    Log = 3,
}

impl MetricsExporterType {
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
            2 => Self::Prom,
            3 => Self::Log,
            _ => Self::Disable,
        }
    }
}

impl fmt::Display for MetricsExporterType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Disable => "disable",
            Self::OtlpGrpc => "otlp_grpc",
            Self::Prom => "prom",
            Self::Log => "log",
        };
        write!(f, "{s}")
    }
}

impl FromStr for MetricsExporterType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "disable" | "off" | "none" => Ok(Self::Disable),
            "otlp_grpc" | "otlp" => Ok(Self::OtlpGrpc),
            "prom" | "prometheus" => Ok(Self::Prom),
            "log" => Ok(Self::Log),
            _ => Err(()),
        }
    }
}

impl TryFrom<i32> for MetricsExporterType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Disable),
            1 => Ok(Self::OtlpGrpc),
            2 => Ok(Self::Prom),
            3 => Ok(Self::Log),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_from_value() {
        assert_eq!(MetricsExporterType::from_value(0), MetricsExporterType::Disable);
        assert_eq!(MetricsExporterType::from_value(1), MetricsExporterType::OtlpGrpc);
        assert_eq!(MetricsExporterType::from_value(2), MetricsExporterType::Prom);
        assert_eq!(MetricsExporterType::from_value(3), MetricsExporterType::Log);
        assert_eq!(MetricsExporterType::from_value(100), MetricsExporterType::Disable);
    }

    #[test]
    fn test_is_enable() {
        assert!(!MetricsExporterType::Disable.is_enable());
        assert!(MetricsExporterType::OtlpGrpc.is_enable());
        assert!(MetricsExporterType::Prom.is_enable());
        assert!(MetricsExporterType::Log.is_enable());
    }

    #[test]
    fn test_value() {
        assert_eq!(MetricsExporterType::Disable.value(), 0);
        assert_eq!(MetricsExporterType::OtlpGrpc.value(), 1);
        assert_eq!(MetricsExporterType::Prom.value(), 2);
        assert_eq!(MetricsExporterType::Log.value(), 3);
    }

    #[test]
    fn test_display() {
        assert_eq!(MetricsExporterType::Disable.to_string(), "disable");
        assert_eq!(MetricsExporterType::OtlpGrpc.to_string(), "otlp_grpc");
        assert_eq!(MetricsExporterType::Prom.to_string(), "prom");
        assert_eq!(MetricsExporterType::Log.to_string(), "log");
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            MetricsExporterType::from_str("disable").unwrap(),
            MetricsExporterType::Disable
        );
        assert_eq!(
            MetricsExporterType::from_str("otlp_grpc").unwrap(),
            MetricsExporterType::OtlpGrpc
        );
        assert_eq!(
            MetricsExporterType::from_str("prometheus").unwrap(),
            MetricsExporterType::Prom
        );
        assert_eq!(MetricsExporterType::from_str("log").unwrap(), MetricsExporterType::Log);

        assert!(MetricsExporterType::from_str("unknown").is_err());
    }

    #[test]
    fn test_serde_json() {
        let v = MetricsExporterType::OtlpGrpc;
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, "\"otlp_grpc\"");

        let parsed: MetricsExporterType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, MetricsExporterType::OtlpGrpc);
    }
}
