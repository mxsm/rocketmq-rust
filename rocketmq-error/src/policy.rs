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

use crate::ErrorKind;

/// Retry or recovery classification for one error kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RetryClass {
    Never,
    Immediate,
    AfterBackoff,
    RefreshRoute,
    SwitchBroker,
    RefreshLeader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecoverySpec {
    pub retry: RetryClass,
}

impl RecoverySpec {
    #[inline]
    pub const fn new(retry: RetryClass) -> Self {
        Self { retry }
    }

    #[inline]
    pub const fn for_kind(kind: ErrorKind) -> Self {
        Self::new(match kind {
            ErrorKind::RouteNotFound
            | ErrorKind::RouteInconsistent
            | ErrorKind::RouteRegistrationConflict
            | ErrorKind::RouteVersionConflict => RetryClass::RefreshRoute,
            ErrorKind::BrokerOperationFailed
            | ErrorKind::BrokerNotFound
            | ErrorKind::BrokerRegistrationFailed
            | ErrorKind::QueueNotExist
            | ErrorKind::ProducerNotAvailable
            | ErrorKind::ConsumerNotAvailable => RetryClass::SwitchBroker,
            ErrorKind::NotMasterBroker | ErrorKind::ControllerNotLeader => RetryClass::RefreshLeader,
            ErrorKind::Network
            | ErrorKind::Rpc
            | ErrorKind::Timeout
            | ErrorKind::RetryLimitExceeded
            | ErrorKind::StorageLockFailed
            | ErrorKind::Tools => RetryClass::AfterBackoff,
            ErrorKind::MessageLookupFailed | ErrorKind::SubscriptionGroupNotExist => RetryClass::Immediate,
            _ => RetryClass::Never,
        })
    }
}

/// Default severity for logs, metrics, traces, and alert routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorSeverity {
    Debug,
    Info,
    Warn,
    Error,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObserveSpec {
    pub severity: ErrorSeverity,
    pub metric_label: &'static str,
}

impl ObserveSpec {
    #[inline]
    pub const fn new(severity: ErrorSeverity, metric_label: &'static str) -> Self {
        Self { severity, metric_label }
    }

    #[inline]
    pub const fn for_kind(kind: ErrorKind) -> Self {
        Self::new(observe_severity(kind), kind.code().as_str())
    }
}

#[inline]
const fn observe_severity(kind: ErrorKind) -> ErrorSeverity {
    match kind {
        ErrorKind::IllegalArgument
        | ErrorKind::InvalidProperty
        | ErrorKind::RequestBodyInvalid
        | ErrorKind::RequestHeaderError
        | ErrorKind::ResponseProcessFailed
        | ErrorKind::ConfigMissing
        | ErrorKind::ConfigInvalidValue
        | ErrorKind::MissingRequiredMessageProperty => ErrorSeverity::Info,
        ErrorKind::RouteNotFound
        | ErrorKind::TopicNotExist
        | ErrorKind::SubscriptionGroupNotExist
        | ErrorKind::QueueNotExist
        | ErrorKind::MessageLookupFailed
        | ErrorKind::Network
        | ErrorKind::Timeout
        | ErrorKind::RetryLimitExceeded
        | ErrorKind::NotMasterBroker
        | ErrorKind::ControllerNotLeader => ErrorSeverity::Warn,
        ErrorKind::StorageCorrupted | ErrorKind::StorageOutOfSpace => ErrorSeverity::Critical,
        ErrorKind::Legacy => ErrorSeverity::Debug,
        _ => ErrorSeverity::Error,
    }
}
