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

use std::collections::BTreeSet;

use rocketmq_sre_contracts::ActionDescriptor;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_core::ActionCatalog;
use rocketmq_sre_core::EMBEDDED_ACTION_DESCRIPTOR_YAMLS;
use serde_json::Value;

const IMPLEMENTATION_PLAN: &str = include_str!("../../../config/action-implementation/implementation-plan.v1.yaml");

#[test]
fn all_wave_two_and_wave_three_descriptors_form_one_closed_catalog() {
    let mut catalog = ActionCatalog::default();
    let mut ids = BTreeSet::new();
    let mut plan_only = BTreeSet::new();
    for yaml in EMBEDDED_ACTION_DESCRIPTOR_YAMLS {
        let descriptor: ActionDescriptor = serde_yaml::from_str(yaml).expect("valid action descriptor");
        assert!(ids.insert(descriptor.id.clone()), "duplicate {}", descriptor.id);
        if descriptor.plan_only {
            plan_only.insert(descriptor.id.clone());
        }
        catalog.register(descriptor).expect("registered descriptor");
    }

    assert_eq!(catalog.len(), ExecutionAction::ALL.len());
    assert_eq!(ids.len(), 25);
    assert_eq!(
        plan_only,
        ExecutionAction::WAVE3_PLAN_ONLY
            .into_iter()
            .map(|action| action.id().to_owned())
            .collect()
    );
    for action in ExecutionAction::WAVE3_PLAN_ONLY {
        assert!(catalog.executable_descriptor(action, "1.0.0").is_err());
    }
}

#[test]
fn implementation_plan_covers_wave_two_wave_three_and_permanent_r3() {
    let plan: Value = serde_yaml::from_str(IMPLEMENTATION_PLAN).expect("valid implementation plan");
    let waves = plan["waves"].as_array().expect("waves");
    let wave_two = waves[0]["actions"].as_array().expect("wave 2 actions");
    let wave_three = waves[1]["actions"].as_array().expect("wave 3 actions");
    assert_eq!(wave_two.len(), 13);
    assert_eq!(wave_three.len(), 7);
    assert_eq!(
        wave_two
            .iter()
            .filter(|entry| entry["representative"] == Value::Bool(true))
            .count(),
        3
    );
    assert!(
        wave_three
            .iter()
            .all(|entry| entry["handler_status"] == Value::String("forbidden".to_owned()))
    );

    let permanent_r3 = plan["permanent_r3"]["capability_ids"]
        .as_array()
        .expect("permanent R3 ids");
    assert!(!permanent_r3.is_empty());
    assert!(
        permanent_r3
            .iter()
            .all(|entry| { entry.as_str().is_some_and(|id| ExecutionAction::from_id(id).is_none()) })
    );
}
