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

use rocketmq_error::CanonicalCondition;
use rocketmq_error::ErrorCode;
use rocketmq_error::RecoveryHint;

#[test]
fn canonical_error_codes_accept_lowercase_dotted_domain_semantics() {
    let cases = [
        "storage.commit_log.corrupt_record",
        "auth.credentials.expired",
        "route.v2.leader_not_found",
        "a.b.c",
    ];

    for value in cases {
        let code = ErrorCode::try_new(value).expect("valid canonical catalog code");
        assert_eq!(code.as_str(), value);
        assert_eq!(code.to_string(), value);
    }
}

#[test]
fn canonical_error_codes_reject_noncanonical_values() {
    let cases = [
        "",
        "storage",
        "storage.commit_log",
        "Storage.commit_log.corrupt_record",
        "storage.Commit_Log.corrupt_record",
        "storage.commit_log.CORRUPT_RECORD",
        "storage.commit log.corrupt_record",
        "storage..corrupt_record",
        ".storage.commit_log",
        "storage.commit_log.",
        "storage/commit_log/corrupt_record",
        "storage.commit-log.corrupt_record",
        "storage.commit_log.corrupt-record",
        "storage.commit_log.$corrupt_record",
        "storage.1commit_log.corrupt_record",
    ];

    for value in cases {
        assert!(
            ErrorCode::try_new(value).is_none(),
            "`{value}` must not be accepted as a canonical catalog code"
        );
    }
}

#[test]
fn canonical_conditions_have_exhaustive_stable_names() {
    fn accepts_only_canonical_conditions(condition: CanonicalCondition) {
        match condition {
            CanonicalCondition::InvalidArgument
            | CanonicalCondition::NotFound
            | CanonicalCondition::AlreadyExists
            | CanonicalCondition::Unauthenticated
            | CanonicalCondition::PermissionDenied
            | CanonicalCondition::ResourceExhausted
            | CanonicalCondition::FailedPrecondition
            | CanonicalCondition::Aborted
            | CanonicalCondition::Unavailable
            | CanonicalCondition::DeadlineExceeded
            | CanonicalCondition::DataLoss
            | CanonicalCondition::Cancelled
            | CanonicalCondition::Unimplemented
            | CanonicalCondition::Internal => {}
        }
    }

    let cases = [
        (CanonicalCondition::InvalidArgument, "invalid_argument"),
        (CanonicalCondition::NotFound, "not_found"),
        (CanonicalCondition::AlreadyExists, "already_exists"),
        (CanonicalCondition::Unauthenticated, "unauthenticated"),
        (CanonicalCondition::PermissionDenied, "permission_denied"),
        (CanonicalCondition::ResourceExhausted, "resource_exhausted"),
        (CanonicalCondition::FailedPrecondition, "failed_precondition"),
        (CanonicalCondition::Aborted, "aborted"),
        (CanonicalCondition::Unavailable, "unavailable"),
        (CanonicalCondition::DeadlineExceeded, "deadline_exceeded"),
        (CanonicalCondition::DataLoss, "data_loss"),
        (CanonicalCondition::Cancelled, "cancelled"),
        (CanonicalCondition::Unimplemented, "unimplemented"),
        (CanonicalCondition::Internal, "internal"),
    ];

    assert_eq!(cases.len(), 14);
    for (condition, name) in cases {
        accepts_only_canonical_conditions(condition);
        assert_eq!(condition.as_str(), name);
        assert_eq!(condition.to_string(), name);
    }
}

#[test]
fn recovery_hints_have_exhaustive_stable_names() {
    fn accepts_only_recovery_hints(hint: RecoveryHint) {
        match hint {
            RecoveryHint::Never
            | RecoveryHint::Backoff
            | RecoveryHint::RefreshRoute
            | RecoveryHint::RefreshLeader
            | RecoveryHint::SwitchBroker
            | RecoveryHint::RefreshCredentials
            | RecoveryHint::OperatorAction => {}
        }
    }

    let cases = [
        (RecoveryHint::Never, "never"),
        (RecoveryHint::Backoff, "backoff"),
        (RecoveryHint::RefreshRoute, "refresh_route"),
        (RecoveryHint::RefreshLeader, "refresh_leader"),
        (RecoveryHint::SwitchBroker, "switch_broker"),
        (RecoveryHint::RefreshCredentials, "refresh_credentials"),
        (RecoveryHint::OperatorAction, "operator_action"),
    ];

    assert_eq!(cases.len(), 7);
    for (hint, name) in cases {
        accepts_only_recovery_hints(hint);
        assert_eq!(hint.as_str(), name);
        assert_eq!(hint.to_string(), name);
    }
}
