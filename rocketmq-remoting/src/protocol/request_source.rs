/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum RequestSource {
    SDK,
    ProxyForOrder,
    ProxyForBroadcast,
    ProxyForStream,
}

impl RequestSource {
    pub const SYSTEM_PROPERTY_KEY: &'static str = "rocketmq.requestSource";

    pub fn get_value(&self) -> i32 {
        match self {
            RequestSource::SDK => -1,
            RequestSource::ProxyForOrder => 0,
            RequestSource::ProxyForBroadcast => 1,
            RequestSource::ProxyForStream => 2,
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
        RequestSource::SDK
    }

    fn from_value(value: i32) -> RequestSource {
        match value {
            -1 => RequestSource::SDK,
            0 => RequestSource::ProxyForOrder,
            1 => RequestSource::ProxyForBroadcast,
            2 => RequestSource::ProxyForStream,
            _ => RequestSource::SDK,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_value_returns_correct_value_for_each_variant() {
        assert_eq!(RequestSource::SDK.get_value(), -1);
        assert_eq!(RequestSource::ProxyForOrder.get_value(), 0);
        assert_eq!(RequestSource::ProxyForBroadcast.get_value(), 1);
        assert_eq!(RequestSource::ProxyForStream.get_value(), 2);
    }

    #[test]
    fn is_valid_returns_true_for_valid_values() {
        assert!(RequestSource::is_valid(Some(-1)));
        assert!(RequestSource::is_valid(Some(0)));
        assert!(RequestSource::is_valid(Some(1)));
        assert!(RequestSource::is_valid(Some(2)));
    }

    #[test]
    fn is_valid_returns_false_for_invalid_values() {
        assert!(!RequestSource::is_valid(Some(-2)));
        assert!(!RequestSource::is_valid(Some(4)));
        assert!(!RequestSource::is_valid(None));
    }

    #[test]
    fn parse_integer_returns_correct_variant_for_valid_values() {
        assert_eq!(RequestSource::parse_integer(Some(-1)), RequestSource::SDK);
        assert_eq!(
            RequestSource::parse_integer(Some(0)),
            RequestSource::ProxyForOrder
        );
        assert_eq!(
            RequestSource::parse_integer(Some(1)),
            RequestSource::ProxyForBroadcast
        );
        assert_eq!(
            RequestSource::parse_integer(Some(2)),
            RequestSource::ProxyForStream
        );
    }

    #[test]
    fn parse_integer_returns_sdk_for_invalid_values() {
        assert_eq!(RequestSource::parse_integer(Some(4)), RequestSource::SDK);
        assert_eq!(RequestSource::parse_integer(Some(-2)), RequestSource::SDK);
        assert_eq!(RequestSource::parse_integer(None), RequestSource::SDK);
    }
}
