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

use std::error::Error;
use std::str::FromStr;

use rocketmq_sre_eval::EvalError;
use rocketmq_sre_eval::assertions::ReplayAssertion;
use rocketmq_sre_eval::phase1_shadow::ProviderMode;
use rocketmq_sre_eval::phase1_shadow::ProviderModeRejection;

#[test]
fn eval_exports_one_operational_error_and_replay_assertions_are_outcomes() {
    fn requires_error<T: Error>() {}
    fn requires_closed_outcome<T>() {}

    requires_error::<EvalError>();
    requires_closed_outcome::<ReplayAssertion>();
    requires_closed_outcome::<ProviderModeRejection>();
}

#[test]
fn eval_error_redacts_the_public_and_private_shadow_layers() {
    let error =
        ProviderMode::from_str("credential-like-sensitive-value").expect_err("unknown provider mode must fail closed");

    assert_eq!(error.to_string(), "provider mode is unsupported");
    assert_eq!(format!("{error:?}"), "ProviderModeRejection");
}
