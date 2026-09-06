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

use std::error::Error as _;
use std::io;

use rocketmq_error::DomainError;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::RocketMQError;
use rocketmq_error::SharedError;
use rocketmq_error::CORE_CONFIGURATION_INVALID;
use rocketmq_error::TRANSPORT_CONNECTION_FAILED;

fn assert_shared_contract(canonical: Error, expected_cause: Option<&str>) {
    let shared: SharedError = std::sync::Arc::new(canonical);
    let wrapped = RocketMQError::Shared(std::sync::Arc::clone(&shared));

    assert_eq!(wrapped.descriptor(), shared.descriptor());
    assert_eq!(wrapped.context(), shared.context().clone());
    let boundary = wrapped.boundary_view();
    assert_eq!(boundary.code(), shared.code());
    assert_eq!(boundary.recovery_hint(), shared.recovery_hint());
    assert_eq!(boundary.severity(), shared.severity());
    assert_eq!(boundary.exposure(), shared.exposure());
    assert_eq!(wrapped.recovery_hint(), shared.recovery_hint());
    assert_eq!(wrapped.severity(), shared.severity());
    assert_eq!(wrapped.exposure(), shared.exposure());
    assert_eq!(wrapped.to_string(), shared.to_string());

    assert_eq!(wrapped.source().map(ToString::to_string).as_deref(), expected_cause);
    if expected_cause.is_some() {
        assert!(wrapped
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some());
    }

    let RocketMQError::Shared(retained) = wrapped else {
        panic!("expected the canonical shared carrier")
    };
    assert!(std::sync::Arc::ptr_eq(&shared, &retained));
}

#[test]
fn shared_error_clones_preserve_typed_metadata_and_source_chains() {
    assert_shared_contract(Error::new(&TRANSPORT_CONNECTION_FAILED), None);
    assert_shared_contract(
        Error::caused_by(
            &CORE_CONFIGURATION_INVALID,
            io::Error::new(io::ErrorKind::InvalidInput, "invalid timeout"),
        )
        .with_context(ErrorContext::new()),
        Some("invalid timeout"),
    );
}

#[test]
fn multiple_carriers_reuse_the_original_canonical_snapshot() {
    let shared: SharedError = std::sync::Arc::new(Error::new(&TRANSPORT_CONNECTION_FAILED));
    let first = RocketMQError::Shared(std::sync::Arc::clone(&shared));
    let second = RocketMQError::Shared(std::sync::Arc::clone(&shared));

    let RocketMQError::Shared(first) = first else {
        panic!("expected the first shared carrier")
    };
    let RocketMQError::Shared(second) = second else {
        panic!("expected the second shared carrier")
    };
    assert!(std::sync::Arc::ptr_eq(&first, &second));
    assert!(std::sync::Arc::ptr_eq(&shared, &first));
}
