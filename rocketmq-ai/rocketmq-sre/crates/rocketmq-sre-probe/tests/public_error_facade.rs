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

use rocketmq_client_rust::ClientError;
use rocketmq_sre_probe::ProbeAclRejection;
use rocketmq_sre_probe::ProbeConfigRejection;
use rocketmq_sre_probe::ProbeIdentityRejection;
use rocketmq_sre_probe::scenario::ProbeBudgetRejection;
use rocketmq_sre_probe::scenario::ProbeDriverError;
use rocketmq_sre_probe::scenario::ProbeScenarioRejection;

#[test]
fn probe_keeps_only_the_driver_as_an_operational_error() {
    fn requires_error<T: Error>() {}
    fn requires_closed_outcome<T>() {}

    requires_error::<ProbeDriverError>();
    requires_closed_outcome::<ProbeAclRejection>();
    requires_closed_outcome::<ProbeConfigRejection>();
    requires_closed_outcome::<ProbeIdentityRejection>();
    requires_closed_outcome::<ProbeBudgetRejection>();
    requires_closed_outcome::<ProbeScenarioRejection>();
}

#[test]
fn driver_error_redacts_and_retains_the_typed_rocketmq_source() {
    let error = ProbeDriverError::producer_start_failed(ClientError::invariant_violated("sensitive internal detail"));

    assert_eq!(error.to_string(), "probe driver operation failed");
    assert_eq!(format!("{error:?}"), "probe driver operation failed");
    assert!(
        error
            .source()
            .is_some_and(|source| source.downcast_ref::<ClientError>().is_some())
    );
}
