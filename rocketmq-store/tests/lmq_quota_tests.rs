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

use rocketmq_store::MessageStoreConfig;

#[test]
fn java_lmq_quota_properties_preserve_default_and_explicit_zero() {
    let defaults: MessageStoreConfig = serde_json::from_str("{}").expect("default config");
    assert!(!defaults.enable_lmq_quota);
    assert_eq!(defaults.max_lmq_consume_queue_num, 20_000);

    let explicit_zero: MessageStoreConfig = serde_json::from_str(
        r#"{"enableLmq":true,"enableMultiDispatch":true,"enableLmqQuota":true,"maxLmqConsumeQueueNum":0}"#,
    )
    .expect("Java property aliases");
    assert!(explicit_zero.enable_lmq);
    assert!(explicit_zero.enable_multi_dispatch);
    assert!(explicit_zero.enable_lmq_quota);
    assert_eq!(explicit_zero.max_lmq_consume_queue_num, 0);
}
