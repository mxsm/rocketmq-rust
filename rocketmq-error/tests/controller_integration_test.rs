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

use std::error::Error as _;
use std::io;
use std::sync::Arc;

use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::RemotingResponseCode;
use rocketmq_error::RocketMQError;
use rocketmq_error::CONTROLLER_CONFIGURATION_INVALID;
use rocketmq_error::CONTROLLER_CONSENSUS_FAILED;
use rocketmq_error::CONTROLLER_CONSENSUS_TIMED_OUT;
use rocketmq_error::CONTROLLER_INTERNAL_FAILURE;
use rocketmq_error::CONTROLLER_LIFECYCLE_NOT_INITIALIZED;
use rocketmq_error::CONTROLLER_REQUEST_INVALID;

#[test]
fn canonical_controller_descriptors_keep_remoting_2015() {
    for descriptor in [
        &CONTROLLER_INTERNAL_FAILURE,
        &CONTROLLER_REQUEST_INVALID,
        &CONTROLLER_CONFIGURATION_INVALID,
        &CONTROLLER_LIFECYCLE_NOT_INITIALIZED,
        &CONTROLLER_CONSENSUS_FAILED,
        &CONTROLLER_CONSENSUS_TIMED_OUT,
    ] {
        assert_eq!(
            descriptor.projection().remoting().code,
            RemotingResponseCode::ControllerJraftInternalError
        );
        assert_eq!(descriptor.projection().remoting().code.as_i32(), 2015);
    }
}

#[test]
fn canonical_controller_error_retains_typed_source_and_fixed_public_message() {
    let error = Error::caused_by(&CONTROLLER_CONSENSUS_FAILED, io::Error::other("private quorum detail")).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, "append raft entry")
            .with_secret_presence(fields::SOURCE_PRESENT),
    );

    assert!(error
        .source()
        .and_then(|source| source.downcast_ref::<io::Error>())
        .is_some());
    assert_eq!(
        error.public_view().expect("schema-valid view").message(),
        "Controller consensus operation failed"
    );
    assert!(!error.to_string().contains("private quorum detail"));
}

#[test]
fn shared_facade_carrier_preserves_one_canonical_controller_allocation() {
    let canonical = Arc::new(
        Error::new(&CONTROLLER_CONSENSUS_TIMED_OUT).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, "change membership")
                .with_u64(fields::TIMEOUT_MS, 500),
        ),
    );
    let expected = Arc::clone(&canonical);
    let facade = RocketMQError::Shared(canonical);
    let RocketMQError::Shared(actual) = facade else {
        panic!("canonical Controller error must use the shared facade carrier");
    };

    assert!(Arc::ptr_eq(&expected, &actual));
    assert!(std::ptr::eq(actual.descriptor(), &CONTROLLER_CONSENSUS_TIMED_OUT));
}
