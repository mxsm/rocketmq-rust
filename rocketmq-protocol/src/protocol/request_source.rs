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

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(i32)]
pub enum RequestSource {
    Unknown = -2,
    SDK = -1,
    ProxyForOrder = 0,
    ProxyForBroadcast = 1,
    ProxyForStream = 2,
}

impl RequestSource {
    pub const SYSTEM_PROPERTY_KEY: &'static str = "rocketmq.requestSource";

    pub fn get_value(&self) -> i32 {
        match self {
            RequestSource::SDK => -1,
            RequestSource::ProxyForOrder => 0,
            RequestSource::ProxyForBroadcast => 1,
            RequestSource::ProxyForStream => 2,
            _ => -2,
        }
    }

    pub fn is_valid(value: Option<i32>) -> bool {
        if let Some(v) = value {
            (-1..3).contains(&v)
        } else {
            false
        }
    }

    pub fn parse_integer(value: Option<i32>) -> RequestSource {
        if let Some(v) = value {
            if Self::is_valid(Some(v)) {
                return Self::from_value(v);
            }
        }
        RequestSource::Unknown
    }

    pub fn from_value(value: i32) -> RequestSource {
        match value {
            -1 => RequestSource::SDK,
            0 => RequestSource::ProxyForOrder,
            1 => RequestSource::ProxyForBroadcast,
            2 => RequestSource::ProxyForStream,
            _ => RequestSource::Unknown,
        }
    }
}

impl From<i32> for RequestSource {
    fn from(value: i32) -> Self {
        RequestSource::from_value(value)
    }
}

#[cfg(test)]
mod request_source_tests {
    use super::*;

    #[test]
    fn parse_integer_returns_unknown_for_out_of_range_positive_values() {
        assert_eq!(RequestSource::parse_integer(Some(3)), RequestSource::Unknown);
    }

    #[test]
    fn parse_integer_returns_unknown_for_out_of_range_negative_values() {
        assert_eq!(RequestSource::parse_integer(Some(-3)), RequestSource::Unknown);
    }

    #[test]
    fn from_value_returns_correct_variant_for_known_values() {
        assert_eq!(RequestSource::from_value(-1), RequestSource::SDK);
        assert_eq!(RequestSource::from_value(0), RequestSource::ProxyForOrder);
        assert_eq!(RequestSource::from_value(1), RequestSource::ProxyForBroadcast);
        assert_eq!(RequestSource::from_value(2), RequestSource::ProxyForStream);
    }

    #[test]
    fn from_value_returns_unknown_for_unknown_values() {
        assert_eq!(RequestSource::from_value(4), RequestSource::Unknown);
        assert_eq!(RequestSource::from_value(-3), RequestSource::Unknown);
    }

    #[test]
    fn is_valid_identifies_only_known_values_as_valid() {
        assert!(RequestSource::is_valid(Some(-1)));
        assert!(RequestSource::is_valid(Some(0)));
        assert!(RequestSource::is_valid(Some(1)));
        assert!(RequestSource::is_valid(Some(2)));
        assert!(!RequestSource::is_valid(Some(3)));
        assert!(!RequestSource::is_valid(Some(-3)));
    }

    #[test]
    fn get_value_returns_expected_integer_representation() {
        assert_eq!(RequestSource::SDK.get_value(), -1);
        assert_eq!(RequestSource::ProxyForOrder.get_value(), 0);
        assert_eq!(RequestSource::ProxyForBroadcast.get_value(), 1);
        assert_eq!(RequestSource::ProxyForStream.get_value(), 2);
        assert_eq!(RequestSource::Unknown.get_value(), -2);
    }
}
