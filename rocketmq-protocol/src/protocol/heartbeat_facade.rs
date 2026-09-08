// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::protocol::heartbeat::subscription_data::SubscriptionData;

pub fn default_subscription_data() -> SubscriptionData {
    SubscriptionData {
        sub_version: current_millis(),
        ..SubscriptionData::default()
    }
}

fn current_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be later than the unix epoch")
            .as_millis() as i64
    }

    #[test]
    fn default_subscription_data_uses_wall_clock_sub_version() {
        let before = now_millis();
        let data = default_subscription_data();

        assert!(data.sub_version > 0);
        assert!(data.sub_version >= before);
    }

    #[test]
    fn default_subscription_data_keeps_remaining_defaults() {
        let data = default_subscription_data();
        let expected = SubscriptionData::default();

        assert_eq!(data.class_filter_mode, expected.class_filter_mode);
        assert_eq!(data.topic, expected.topic);
        assert_eq!(data.sub_string, expected.sub_string);
        assert_eq!(data.tags_set, expected.tags_set);
        assert_eq!(data.code_set, expected.code_set);
        assert_eq!(data.expression_type, expected.expression_type);
        assert_eq!(data.filter_class_source, expected.filter_class_source);
    }
}
