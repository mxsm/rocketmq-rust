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
use rocketmq_error::FilterCompileError;
use rocketmq_error::FilterCompileErrorKind;
use rocketmq_error::FilterCompileSource;
use rocketmq_error::FilterCompileStage;
use rocketmq_error::RocketMQError;
use rocketmq_error::SharedRocketMQError;
use rocketmq_error::TRANSPORT_CONNECTION_FAILED;

fn assert_shared_contract(error: RocketMQError) {
    let expected_kind = error.kind();
    let expected_descriptor = error.descriptor();
    let expected_context = error.context();
    let expected_boundary = error.boundary_view();
    let expected_recovery_hint = error.recovery_hint();
    let expected_severity = error.severity();
    let expected_exposure = error.exposure();
    let expected_display = error.to_string();
    let expected_source = error.source().map(ToString::to_string);
    let expected_nested_source = error
        .source()
        .and_then(|source| source.source())
        .map(ToString::to_string);

    let shared = SharedRocketMQError::new(error);
    let cloned = shared.clone();

    for snapshot in [&shared, &cloned] {
        assert_eq!(snapshot.kind(), expected_kind);
        assert_eq!(snapshot.descriptor(), expected_descriptor);
        assert_eq!(snapshot.context(), expected_context);
        assert_eq!(snapshot.boundary_view(), expected_boundary);
        assert_eq!(snapshot.recovery_hint(), expected_recovery_hint);
        assert_eq!(snapshot.severity(), expected_severity);
        assert_eq!(snapshot.exposure(), expected_exposure);
        assert_eq!(snapshot.to_string(), expected_display);
    }
    assert!(std::ptr::eq(shared.as_error(), cloned.as_error()));

    let source = shared.source().expect("shared error source");
    let original = source
        .downcast_ref::<RocketMQError>()
        .expect("shared source must be the original RocketMQ error");
    assert!(std::ptr::eq(shared.as_error(), original));
    assert_eq!(source.to_string(), expected_display);
    assert_eq!(source.source().map(ToString::to_string), expected_source);
    assert_eq!(
        source
            .source()
            .and_then(|nested| nested.source())
            .map(ToString::to_string),
        expected_nested_source
    );

    let wrapped = cloned.into_error();
    assert_eq!(wrapped.kind(), expected_kind);
    assert_eq!(wrapped.context(), expected_context);
    assert_eq!(wrapped.boundary_view(), expected_boundary);
    assert_eq!(wrapped.to_string(), expected_display);
}

#[test]
fn shared_error_clones_preserve_typed_metadata_and_source_chains() {
    assert_shared_contract(RocketMQError::Network(std::sync::Arc::new(Error::new(
        &TRANSPORT_CONNECTION_FAILED,
    ))));
    let config_invalid_value = RocketMQError::ConfigInvalidValue {
        key: "connect.timeout",
        value: "invalid".to_owned(),
        reason: "must be positive".to_owned(),
    };
    assert_eq!(
        config_invalid_value.context().to_string(),
        "key=<redacted>, value=<redacted>, reason=<redacted>"
    );
    assert_shared_contract(config_invalid_value);
    assert_shared_contract(RocketMQError::ClientNotStarted);
    assert_shared_contract(RocketMQError::from(io::Error::new(
        io::ErrorKind::ConnectionRefused,
        io::Error::new(io::ErrorKind::TimedOut, "inner connect timeout"),
    )));
    assert_shared_contract(RocketMQError::from(FilterCompileError::new_with_source(
        FilterCompileErrorKind::UnexpectedToken,
        FilterCompileStage::Parse,
        Some(12),
        FilterCompileSource::Sql92,
    )));
}

#[test]
fn rewrapping_a_shared_error_reuses_the_original_snapshot() {
    let shared = SharedRocketMQError::new(RocketMQError::ClientNotStarted);
    let rewrapped = SharedRocketMQError::new(shared.clone().into_error());

    assert!(std::ptr::eq(shared.as_error(), rewrapped.as_error()));
    assert_eq!(shared.to_string(), rewrapped.to_string());
}
