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

use crate::boundary::CliSpec;
use crate::boundary::GrpcSpec;
use crate::boundary::HttpSpec;
use crate::boundary::RemotingSpec;
use crate::kind::ErrorCode;
use crate::kind::ErrorKind;
use crate::kind::ErrorScope;
use crate::policy::ObserveSpec;
use crate::policy::RecoverySpec;

/// Static metadata for one [`ErrorKind`].
///
/// The registry is the single source for machine-readable error identity. Later
/// changes extend this struct with protocol, retry, redaction, and observability
/// fields without changing the lookup contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorSpec {
    pub kind: ErrorKind,
    pub code: ErrorCode,
    pub scope: ErrorScope,
    pub public_message: &'static str,
    pub remoting: RemotingSpec,
    pub grpc: GrpcSpec,
    pub http: HttpSpec,
    pub cli: CliSpec,
    pub recovery: RecoverySpec,
    pub observe: ObserveSpec,
}

impl ErrorSpec {
    #[inline]
    pub const fn new(kind: ErrorKind, public_message: &'static str) -> Self {
        Self {
            kind,
            code: kind.code(),
            scope: kind.scope(),
            public_message,
            remoting: RemotingSpec::for_kind(kind),
            grpc: GrpcSpec::for_kind(kind),
            http: HttpSpec::for_kind(kind),
            cli: CliSpec::for_kind(kind),
            recovery: RecoverySpec::for_kind(kind),
            observe: ObserveSpec::for_kind(kind),
        }
    }
}

macro_rules! spec {
    ($kind:ident, $message:literal) => {
        ErrorSpec::new(ErrorKind::$kind, $message)
    };
}

/// Static registry for all current error kinds.
pub const ALL_ERROR_SPECS: &[ErrorSpec] = &[
    spec!(Network, "Network operation failed"),
    spec!(Serialization, "Serialization failed"),
    spec!(Protocol, "Protocol error"),
    spec!(Rpc, "RPC operation failed"),
    spec!(Authentication, "Authentication failed"),
    spec!(Controller, "Controller operation failed"),
    spec!(InvalidProperty, "Message property is invalid"),
    spec!(BrokerNotFound, "Broker was not found"),
    spec!(BrokerRegistrationFailed, "Broker registration failed"),
    spec!(BrokerOperationFailed, "Broker operation failed"),
    spec!(TopicNotExist, "Topic does not exist"),
    spec!(QueueNotExist, "Queue does not exist"),
    spec!(SubscriptionGroupNotExist, "Subscription group does not exist"),
    spec!(QueueIdOutOfRange, "Queue id is out of range"),
    spec!(MessageTooLarge, "Message body is too large"),
    spec!(MessageValidationFailed, "Message validation failed"),
    spec!(RetryLimitExceeded, "Retry limit was exceeded"),
    spec!(TransactionRejected, "Transaction message was rejected"),
    spec!(BrokerPermissionDenied, "Broker permission was denied"),
    spec!(NotMasterBroker, "Broker is not the master"),
    spec!(MessageLookupFailed, "Message lookup failed"),
    spec!(TopicSendingForbidden, "Topic sending is forbidden"),
    spec!(BrokerAsyncTaskFailed, "Broker asynchronous operation failed"),
    spec!(RequestBodyInvalid, "Request body is invalid"),
    spec!(RequestHeaderError, "Request header is invalid"),
    spec!(ResponseProcessFailed, "Response processing failed"),
    spec!(RouteNotFound, "Route information was not found"),
    spec!(RouteInconsistent, "Route data is inconsistent"),
    spec!(RouteRegistrationConflict, "Route registration conflict"),
    spec!(RouteVersionConflict, "Route version conflict"),
    spec!(ClusterNotFound, "Cluster was not found"),
    spec!(ClientNotStarted, "Client is not started"),
    spec!(ClientAlreadyStarted, "Client is already started"),
    spec!(ClientShuttingDown, "Client is shutting down"),
    spec!(ClientInvalidState, "Client state is invalid"),
    spec!(ProducerNotAvailable, "Producer is not available"),
    spec!(ConsumerNotAvailable, "Consumer is not available"),
    spec!(Tools, "Tools operation failed"),
    spec!(Filter, "Filter operation failed"),
    spec!(StorageReadFailed, "Storage read failed"),
    spec!(StorageWriteFailed, "Storage write failed"),
    spec!(StorageCorrupted, "Storage data is corrupted"),
    spec!(StorageOutOfSpace, "Storage is out of space"),
    spec!(StorageLockFailed, "Storage lock failed"),
    spec!(ConfigParseFailed, "Configuration parsing failed"),
    spec!(ConfigMissing, "Required configuration is missing"),
    spec!(ConfigInvalidValue, "Configuration value is invalid"),
    spec!(AuthConfigInvalid, "Authentication configuration is invalid"),
    spec!(AuthHotReloadFailed, "Authentication hot reload failed"),
    spec!(ControllerNotLeader, "Controller is not the leader"),
    spec!(ControllerRaftError, "Controller raft operation failed"),
    spec!(ControllerConsensusTimeout, "Controller consensus operation timed out"),
    spec!(ControllerSnapshotFailed, "Controller snapshot operation failed"),
    spec!(Io, "I/O operation failed"),
    spec!(IllegalArgument, "Argument is illegal"),
    spec!(Timeout, "Operation timed out"),
    spec!(Internal, "Internal error"),
    spec!(Service, "Service lifecycle operation failed"),
    spec!(InvalidVersionOrdinal, "Version ordinal is invalid"),
    spec!(NotInitialized, "Component is not initialized"),
    spec!(MissingRequiredMessageProperty, "Message is missing a required property"),
];

/// Return the static metadata for an error kind.
#[inline]
pub fn error_spec(kind: ErrorKind) -> &'static ErrorSpec {
    ALL_ERROR_SPECS
        .iter()
        .find(|spec| spec.kind == kind)
        .expect("all ErrorKind variants must have an ErrorSpec")
}

impl ErrorKind {
    /// Return the static metadata for this error kind.
    #[inline]
    pub fn spec(self) -> &'static ErrorSpec {
        error_spec(self)
    }
}
