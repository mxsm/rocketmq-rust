// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashSet;

use rocketmq_model::common::lite::LiteSubscription;

#[test]
fn lite_subscription_requires_explicit_clock_values() {
    let mut subscription = LiteSubscription::new("group".into(), "topic".into(), 100);
    let created = subscription.update_time();
    subscription.add_lite_topic_set(&HashSet::from(["lite".into()]), 101);
    assert!(subscription.update_time() > created);
    assert!(subscription.lite_topic_set().contains("lite"));
}
