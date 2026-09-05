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

use rocketmq_error::AuthError;
use rocketmq_error::ControllerError;
use rocketmq_error::DomainError;
use rocketmq_error::ErrorKind;
use rocketmq_error::RocketMQError;
use rocketmq_error::SerializationError;

fn assert_domain_contract(error: &dyn DomainError, expected_kind: ErrorKind) {
    assert_eq!(expected_kind, error.kind());
    assert_eq!(error.code(), error.descriptor().code());
    assert_eq!(error.code(), error.boundary_view().code());
    assert_eq!(error.recovery_hint(), error.boundary_view().recovery_hint());
    assert_eq!(error.severity(), error.boundary_view().severity());
    assert_eq!(error.exposure(), error.boundary_view().exposure());
}

#[test]
fn domain_errors_share_one_stable_metadata_contract() {
    let auth = AuthError::InvalidCredential("missing access key".to_owned());
    assert_domain_contract(&auth, ErrorKind::Authentication);

    let controller = RocketMQError::Controller(ControllerError::Raft("append failed".to_owned()));
    assert_domain_contract(&controller, ErrorKind::Controller);

    let serialization = SerializationError::source(
        "decode command",
        "JSON",
        io::Error::new(io::ErrorKind::InvalidData, "invalid token"),
    );
    assert_domain_contract(&serialization, ErrorKind::Serialization);

    let internal = RocketMQError::internal("join request worker", io::Error::other("worker cancelled"));
    assert_domain_contract(&internal, ErrorKind::Internal);
}

#[test]
fn source_preserving_variants_retain_the_original_error_chain() {
    let serialization = SerializationError::source(
        "decode command",
        "JSON",
        io::Error::new(io::ErrorKind::InvalidData, "invalid token"),
    );
    assert_eq!(
        Some("invalid token"),
        serialization.source().map(ToString::to_string).as_deref()
    );

    let controller = ControllerError::runtime_source("join controller task", io::Error::other("task cancelled"));
    assert_eq!(
        Some("task cancelled"),
        controller.source().map(ToString::to_string).as_deref()
    );

    let internal = RocketMQError::internal("join request worker", io::Error::other("worker cancelled"));
    assert_eq!(
        Some("worker cancelled"),
        internal.source().map(ToString::to_string).as_deref()
    );
    assert_eq!("core.internal.failure", internal.descriptor().code().as_str());
}

#[test]
fn invariant_failure_is_not_a_string_catch_all() {
    let error = RocketMQError::invariant_violated("request owner must outlive its worker");

    assert_eq!(ErrorKind::Internal, error.kind());
    assert!(error.source().is_none());
    let context = error.boundary_view().context().to_string();
    assert!(context.is_empty());
    assert!(!context.contains("request owner must outlive its worker"));
}
