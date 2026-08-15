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
use rocketmq_broker::send_message_constants::has_valid_compaction_key;
use rocketmq_model::common::message::MessageConst;
use serde::Deserialize;

#[derive(Deserialize)]
struct Contract {
    key_cases: Vec<KeyCase>,
}

#[derive(Deserialize)]
struct KeyCase {
    name: String,
    raw_key: String,
    accepted: bool,
    canonical_key: Option<String>,
}

#[test]
fn compaction_topic_key_admission_matches_the_java_55_fixture() {
    let contract: Contract =
        serde_json::from_str(include_str!("../../scripts/fixtures/java-5.5-compaction-contract.json"))
            .expect("valid Java 5.5 compaction fixture");

    for case in contract.key_cases {
        let properties = HashMap::from([(
            CheetahString::from_static_str(MessageConst::PROPERTY_KEYS),
            CheetahString::from_string(case.raw_key.clone()),
        )]);
        assert_eq!(has_valid_compaction_key(&properties), case.accepted, "{}", case.name);
        if case.accepted {
            assert_eq!(case.canonical_key.as_deref(), Some(case.raw_key.as_str()));
        } else {
            assert!(case.canonical_key.is_none());
        }
    }
}

#[test]
fn missing_compaction_key_is_rejected_before_store_projection() {
    assert!(!has_valid_compaction_key(&HashMap::new()));
}
