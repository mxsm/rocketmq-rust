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

use rocketmq_error::CanonicalCondition;
use rocketmq_error::CliExitCode;
use rocketmq_error::GrpcStatusCode;
use rocketmq_error::HttpStatusCode;
use rocketmq_error::RemotingResponseCode;
use rocketmq_observability::ObservabilityFailureDetail;
use rocketmq_observability::ObservabilityOperation;

#[test]
fn observability_error_public_path_uses_canonical_configuration_policy() {
    const SENTINEL: &str = "private configuration detail";
    let error = rocketmq_observability::ObservabilityError::invalid_config(SENTINEL);
    let projection = error.descriptor().projection();

    assert_eq!(error.code(), rocketmq_error::OBSERVABILITY_CONFIGURATION_INVALID.code());
    assert_eq!(error.condition(), CanonicalCondition::InvalidArgument);
    assert_eq!(error.operation(), ObservabilityOperation::ValidateConfiguration);
    assert_eq!(projection.http().status, HttpStatusCode::BAD_REQUEST);
    assert_eq!(projection.grpc().status, GrpcStatusCode::InvalidArgument);
    assert_eq!(projection.cli().exit_code, CliExitCode::CONFIG);
    assert_eq!(projection.remoting().code, RemotingResponseCode::InvalidParameter);
    let detail = error
        .source()
        .and_then(|source| source.downcast_ref::<ObservabilityFailureDetail>())
        .expect("typed observability detail");
    assert_eq!(detail.detail(), SENTINEL);
    assert!(!error.to_string().contains(SENTINEL));
    assert!(!format!("{error:?}").contains(SENTINEL));
}

#[test]
fn cloned_observability_error_shares_canonical_source() {
    let error = rocketmq_observability::ObservabilityError::invalid_config("bad config");
    let cloned = error.clone();

    assert!(std::ptr::eq(
        error.source().expect("source"),
        cloned.source().expect("shared source")
    ));
    assert!(error.public_view().is_ok());
    assert!(error.diagnostic_view().is_ok());
}
