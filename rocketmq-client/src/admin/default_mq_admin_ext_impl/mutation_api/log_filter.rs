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

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;

const LOGGER_PREFIX: &str = "rocketmq_broker::";
const MIN_TTL_SECONDS: u32 = 60;
const MAX_TTL_SECONDS: u32 = 900;
const MAX_BROKER_ADDR_BYTES: usize = 512;
const MAX_LOGGER_BYTES: usize = 128;
const MAX_OPERATION_ID_BYTES: usize = 128;
const FORBIDDEN_LOGGER_SEGMENTS: &[&str] = &[
    "message", "body", "security", "auth", "acl", "secret", "payload", "store",
];

pub(super) fn set_properties(
    broker_addr: &CheetahString,
    logger: &CheetahString,
    level: &CheetahString,
    ttl_seconds: u32,
    operation_id: &CheetahString,
) -> Result<HashMap<CheetahString, CheetahString>, RocketMQError> {
    validate_broker_addr(broker_addr)?;
    validate_logger(logger)?;
    let filter_level = match level.as_str() {
        "INFO" => "info",
        "DEBUG" => "debug",
        _ => {
            return Err(RocketMQError::illegal_argument(
                "broker log-filter level must be INFO or DEBUG",
            ));
        }
    };
    if !(MIN_TTL_SECONDS..=MAX_TTL_SECONDS).contains(&ttl_seconds) {
        return Err(RocketMQError::illegal_argument(format!(
            "broker log-filter TTL must be between {MIN_TTL_SECONDS} and {MAX_TTL_SECONDS} seconds"
        )));
    }
    validate_operation_id(operation_id)?;
    Ok(properties([
        ("logFilter", format!("info,{logger}={filter_level}")),
        (
            "logFilterReason",
            "rocketmq-sre bounded incident diagnostics".to_owned(),
        ),
        ("logFilterTtlSeconds", ttl_seconds.to_string()),
        ("logFilterRequestId", operation_id.to_string()),
    ]))
}

pub(super) fn restore_properties(
    broker_addr: &CheetahString,
    operation_id: &CheetahString,
) -> Result<HashMap<CheetahString, CheetahString>, RocketMQError> {
    validate_broker_addr(broker_addr)?;
    validate_operation_id(operation_id)?;
    Ok(properties([
        ("logFilterRestore", "true".to_owned()),
        ("logFilterReason", "rocketmq-sre bounded logger restoration".to_owned()),
        ("logFilterTtlSeconds", MIN_TTL_SECONDS.to_string()),
        ("logFilterRequestId", operation_id.to_string()),
    ]))
}

fn validate_broker_addr(value: &CheetahString) -> Result<(), RocketMQError> {
    let value = value.as_str();
    if value.is_empty()
        || value.len() > MAX_BROKER_ADDR_BYTES
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(RocketMQError::illegal_argument(
            "broker address must be a bounded non-whitespace value",
        ));
    }
    Ok(())
}

fn validate_logger(value: &CheetahString) -> Result<(), RocketMQError> {
    let value = value.as_str();
    let valid_path = value.starts_with(LOGGER_PREFIX)
        && value.len() <= MAX_LOGGER_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b':'));
    if !valid_path || FORBIDDEN_LOGGER_SEGMENTS.iter().any(|segment| value.contains(segment)) {
        return Err(RocketMQError::illegal_argument(
            "broker logger must be one non-sensitive rocketmq_broker module path",
        ));
    }
    Ok(())
}

fn validate_operation_id(value: &CheetahString) -> Result<(), RocketMQError> {
    let value = value.as_str();
    if value.is_empty()
        || value.len() > MAX_OPERATION_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/'))
    {
        return Err(RocketMQError::illegal_argument(
            "broker log-filter operation id contains unsupported characters",
        ));
    }
    Ok(())
}

fn properties<const N: usize>(values: [(&str, String); N]) -> HashMap<CheetahString, CheetahString> {
    values
        .into_iter()
        .map(|(key, value)| (CheetahString::from(key), CheetahString::from(value)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_only_the_closed_broker_logger_contract() {
        let properties = set_properties(
            &"127.0.0.1:10911".into(),
            &"rocketmq_broker::processor".into(),
            &"DEBUG".into(),
            120,
            &"operation-1".into(),
        )
        .expect("bounded properties");
        assert_eq!(properties.len(), 4);
        assert_eq!(
            properties.get("logFilter").map(CheetahString::as_str),
            Some("info,rocketmq_broker::processor=debug")
        );
        assert_eq!(
            properties.get("logFilterTtlSeconds").map(CheetahString::as_str),
            Some("120")
        );
        assert!(!properties
            .keys()
            .any(|key| key.contains("message") || key.contains("body")));
    }

    #[test]
    fn rejects_filter_injection_sensitive_targets_and_unbounded_ttl() {
        for logger in [
            "rocketmq_broker::processor=trace",
            "rocketmq_store::commit_log",
            "rocketmq_broker::message",
        ] {
            assert!(set_properties(
                &"127.0.0.1:10911".into(),
                &logger.into(),
                &"DEBUG".into(),
                120,
                &"operation-1".into(),
            )
            .is_err());
        }
        assert!(set_properties(
            &"127.0.0.1:10911".into(),
            &"rocketmq_broker::processor".into(),
            &"TRACE".into(),
            120,
            &"operation-1".into(),
        )
        .is_err());
        assert!(set_properties(
            &"127.0.0.1:10911".into(),
            &"rocketmq_broker::processor".into(),
            &"DEBUG".into(),
            901,
            &"operation-1".into(),
        )
        .is_err());
    }

    #[test]
    fn restoration_keeps_the_same_bounded_operation_identity() {
        let properties =
            restore_properties(&"127.0.0.1:10911".into(), &"operation-1".into()).expect("restore properties");
        assert_eq!(
            properties.get("logFilterRestore").map(CheetahString::as_str),
            Some("true")
        );
        assert_eq!(
            properties.get("logFilterRequestId").map(CheetahString::as_str),
            Some("operation-1")
        );
    }
}
