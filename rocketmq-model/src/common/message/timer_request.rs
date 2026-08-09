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

use std::collections::HashMap;

use cheetah_string::CheetahString;
use thiserror::Error;

use crate::common::message::MessageConst;

/// Timer precisions supported by the Java-compatible timer engine.
pub const JAVA_COMPAT_TIMER_PRECISIONS_MS: [u64; 4] = [100, 200, 500, 1_000];

/// Immutable admission policy used while normalizing one timer request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerPolicySnapshot {
    precision_ms: u64,
    max_delay_ms: u64,
}

impl TimerPolicySnapshot {
    /// Creates a validated Java-compatible timer policy.
    ///
    /// # Errors
    ///
    /// Returns [`TimerNormalizeError::UnsupportedPrecision`] when the precision is not one of the
    /// values supported by the Java-compatible timer engine.
    pub fn try_new(precision_ms: u64, max_delay_ms: u64) -> Result<Self, TimerNormalizeError> {
        if !JAVA_COMPAT_TIMER_PRECISIONS_MS.contains(&precision_ms) {
            return Err(TimerNormalizeError::UnsupportedPrecision { precision_ms });
        }
        Ok(Self {
            precision_ms,
            max_delay_ms,
        })
    }

    #[inline]
    pub const fn precision_ms(self) -> u64 {
        self.precision_ms
    }

    #[inline]
    pub const fn max_delay_ms(self) -> u64 {
        self.max_delay_ms
    }
}

/// Property selected according to RocketMQ's timer-property precedence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerRequestKind {
    DelaySeconds,
    DelayMilliseconds,
    DeliverAtMilliseconds,
}

/// Canonical result shared by Broker, remoting, and Proxy admission adapters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NormalizedTimerRequest {
    pub kind: TimerRequestKind,
    pub original_deliver_ms: u64,
    pub timer_out_ms: u64,
}

/// A fallible timer request normalization error.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TimerNormalizeError {
    #[error("timer request does not contain a supported delivery property")]
    MissingDeliveryProperty,
    #[error("timer property {property} has invalid unsigned integer value `{value}`")]
    InvalidProperty { property: &'static str, value: String },
    #[error("timer precision {precision_ms}ms is unsupported; expected one of 100, 200, 500, 1000")]
    UnsupportedPrecision { precision_ms: u64 },
    #[error("timer delivery time arithmetic overflowed")]
    ArithmeticOverflow,
    #[error("timer delivery time {deliver_ms} is not later than current time {now_ms}")]
    NotInFuture { deliver_ms: u64, now_ms: u64 },
    #[error("timer delay {delay_ms}ms exceeds the configured maximum {max_delay_ms}ms")]
    ExceedsMaximum { delay_ms: u64, max_delay_ms: u64 },
}

/// Normalizes RocketMQ timer properties without reading the system clock.
///
/// Property precedence matches Java RocketMQ: `TIMER_DELAY_SEC`, then `TIMER_DELAY_MS`, then
/// `TIMER_DELIVER_MS`. The caller must sample `now_ms` once and pass that value to every adapter
/// involved in the same admission decision.
///
/// # Errors
///
/// Returns a structured error for malformed input, unsupported precision, arithmetic overflow,
/// past delivery times, and requests beyond the configured horizon.
pub fn normalize_timer_request(
    properties: &HashMap<CheetahString, CheetahString>,
    now_ms: u64,
    policy: TimerPolicySnapshot,
) -> Result<NormalizedTimerRequest, TimerNormalizeError> {
    normalize_timer_request_fields(
        properties
            .get(MessageConst::PROPERTY_TIMER_DELAY_SEC)
            .map(CheetahString::as_str),
        properties
            .get(MessageConst::PROPERTY_TIMER_DELAY_MS)
            .map(CheetahString::as_str),
        properties
            .get(MessageConst::PROPERTY_TIMER_DELIVER_MS)
            .map(CheetahString::as_str),
        now_ms,
        policy,
    )
}

/// Normalizes timer fields for adapters that do not use RocketMQ's internal property-map type.
///
/// # Errors
///
/// Has the same validation behavior as [`normalize_timer_request`].
pub fn normalize_timer_request_fields(
    delay_seconds: Option<&str>,
    delay_milliseconds: Option<&str>,
    deliver_at_milliseconds: Option<&str>,
    now_ms: u64,
    policy: TimerPolicySnapshot,
) -> Result<NormalizedTimerRequest, TimerNormalizeError> {
    let (kind, original_deliver_ms) = if let Some(value) = delay_seconds {
        let delay_seconds = parse_property(MessageConst::PROPERTY_TIMER_DELAY_SEC, value)?;
        let delay_ms = delay_seconds
            .checked_mul(1_000)
            .ok_or(TimerNormalizeError::ArithmeticOverflow)?;
        (
            TimerRequestKind::DelaySeconds,
            now_ms
                .checked_add(delay_ms)
                .ok_or(TimerNormalizeError::ArithmeticOverflow)?,
        )
    } else if let Some(value) = delay_milliseconds {
        let delay_ms = parse_property(MessageConst::PROPERTY_TIMER_DELAY_MS, value)?;
        (
            TimerRequestKind::DelayMilliseconds,
            now_ms
                .checked_add(delay_ms)
                .ok_or(TimerNormalizeError::ArithmeticOverflow)?,
        )
    } else if let Some(value) = deliver_at_milliseconds {
        (
            TimerRequestKind::DeliverAtMilliseconds,
            parse_property(MessageConst::PROPERTY_TIMER_DELIVER_MS, value)?,
        )
    } else {
        return Err(TimerNormalizeError::MissingDeliveryProperty);
    };

    let delay_ms = original_deliver_ms
        .checked_sub(now_ms)
        .ok_or(TimerNormalizeError::NotInFuture {
            deliver_ms: original_deliver_ms,
            now_ms,
        })?;
    if delay_ms == 0 {
        return Err(TimerNormalizeError::NotInFuture {
            deliver_ms: original_deliver_ms,
            now_ms,
        });
    }
    if delay_ms > policy.max_delay_ms() {
        return Err(TimerNormalizeError::ExceedsMaximum {
            delay_ms,
            max_delay_ms: policy.max_delay_ms(),
        });
    }

    let precision_ms = policy.precision_ms();
    let timer_out_ms = if original_deliver_ms.is_multiple_of(precision_ms) {
        original_deliver_ms
            .checked_sub(precision_ms)
            .ok_or(TimerNormalizeError::ArithmeticOverflow)?
    } else {
        original_deliver_ms / precision_ms * precision_ms
    };

    Ok(NormalizedTimerRequest {
        kind,
        original_deliver_ms,
        timer_out_ms,
    })
}

fn parse_property(property: &'static str, value: &str) -> Result<u64, TimerNormalizeError> {
    value.parse::<u64>().map_err(|_| TimerNormalizeError::InvalidProperty {
        property,
        value: value.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn properties(values: &[(&'static str, &'static str)]) -> HashMap<CheetahString, CheetahString> {
        values
            .iter()
            .map(|(key, value)| {
                (
                    CheetahString::from_static_str(key),
                    CheetahString::from_static_str(value),
                )
            })
            .collect()
    }

    #[test]
    fn timer_request_uses_java_property_precedence() {
        let input = properties(&[
            (MessageConst::PROPERTY_TIMER_DELIVER_MS, "999999"),
            (MessageConst::PROPERTY_TIMER_DELAY_MS, "2500"),
            (MessageConst::PROPERTY_TIMER_DELAY_SEC, "2"),
        ]);
        let policy = TimerPolicySnapshot::try_new(1_000, 10_000).unwrap();

        let normalized = normalize_timer_request(&input, 10_000, policy).unwrap();

        assert_eq!(normalized.kind, TimerRequestKind::DelaySeconds);
        assert_eq!(normalized.original_deliver_ms, 12_000);
        assert_eq!(normalized.timer_out_ms, 11_000);
    }

    #[test]
    fn timer_request_preserves_exact_target_minus_one_tick_encoding() {
        for precision_ms in JAVA_COMPAT_TIMER_PRECISIONS_MS {
            let policy = TimerPolicySnapshot::try_new(precision_ms, 10_000).unwrap();
            let exact = properties(&[(MessageConst::PROPERTY_TIMER_DELIVER_MS, "12000")]);
            let non_exact = properties(&[(MessageConst::PROPERTY_TIMER_DELIVER_MS, "12001")]);

            assert_eq!(
                normalize_timer_request(&exact, 10_000, policy).unwrap().timer_out_ms,
                12_000 - precision_ms
            );
            assert_eq!(
                normalize_timer_request(&non_exact, 10_000, policy)
                    .unwrap()
                    .timer_out_ms,
                12_001 / precision_ms * precision_ms
            );
        }
    }

    #[test]
    fn timer_request_rejects_invalid_configuration_and_arithmetic() {
        assert_eq!(
            TimerPolicySnapshot::try_new(0, 1_000),
            Err(TimerNormalizeError::UnsupportedPrecision { precision_ms: 0 })
        );
        assert_eq!(
            TimerPolicySnapshot::try_new(250, 1_000),
            Err(TimerNormalizeError::UnsupportedPrecision { precision_ms: 250 })
        );

        let policy = TimerPolicySnapshot::try_new(1_000, u64::MAX).unwrap();
        let overflow = properties(&[(MessageConst::PROPERTY_TIMER_DELAY_SEC, "18446744073709552")]);
        assert_eq!(
            normalize_timer_request(&overflow, 1, policy),
            Err(TimerNormalizeError::ArithmeticOverflow)
        );
    }

    #[test]
    fn timer_request_enforces_future_and_maximum_boundaries() {
        let policy = TimerPolicySnapshot::try_new(1_000, 3_000).unwrap();
        let at_max = properties(&[(MessageConst::PROPERTY_TIMER_DELIVER_MS, "13000")]);
        let beyond_max = properties(&[(MessageConst::PROPERTY_TIMER_DELIVER_MS, "13001")]);
        let past = properties(&[(MessageConst::PROPERTY_TIMER_DELIVER_MS, "9999")]);

        assert!(normalize_timer_request(&at_max, 10_000, policy).is_ok());
        assert!(matches!(
            normalize_timer_request(&beyond_max, 10_000, policy),
            Err(TimerNormalizeError::ExceedsMaximum { .. })
        ));
        assert!(matches!(
            normalize_timer_request(&past, 10_000, policy),
            Err(TimerNormalizeError::NotInFuture { .. })
        ));
    }
}
