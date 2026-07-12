// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashSet;

use rocketmq_common::common::lite::LiteSubscription;

#[test]
fn common_lite_subscription_keeps_clock_owned_legacy_api_and_converts() {
    let mut subscription = LiteSubscription::new("group".into(), "topic".into());
    let created = subscription.update_time();
    assert!(created > 0);
    std::thread::sleep(std::time::Duration::from_millis(2));
    subscription.add_lite_topic_set(&HashSet::from(["lite".into()]));
    assert!(subscription.update_time() > created);

    let canonical: rocketmq_protocol::common::lite::lite_subscription::LiteSubscription = subscription.clone().into();
    assert_eq!(LiteSubscription::from(canonical), subscription);
}
