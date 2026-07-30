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

use crate::kind::ErrorKind;

use super::ErrorSpec;

macro_rules! spec {
    ($kind:ident, $message:literal) => {
        ErrorSpec::new(ErrorKind::$kind, $message)
    };
}

/// Static registry entries for all current error kinds.
///
/// Entries remain grouped by owning domain so changes to one boundary do not
/// turn the registry into an unstructured list. `ErrorKind::ALL` and the error
/// architecture guard verify that this stable index stays complete.
pub(super) const ERROR_SPECS: &[ErrorSpec] = &[
    // Transport, protocol, authentication, and controller boundaries.
    spec!(Network, "Network operation failed"),
    spec!(Serialization, "Serialization failed"),
    spec!(Protocol, "Protocol error"),
    spec!(Rpc, "RPC operation failed"),
    spec!(Authentication, "Authentication failed"),
    spec!(Controller, "Controller operation failed"),
    // Broker, request, and route boundaries.
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
    spec!(QueryNotFound, "Query result was not found"),
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
    // Client and tooling boundaries.
    spec!(ClientNotStarted, "Client is not started"),
    spec!(ClientAlreadyStarted, "Client is already started"),
    spec!(ClientShuttingDown, "Client is shutting down"),
    spec!(ClientInvalidState, "Client state is invalid"),
    spec!(ProducerNotAvailable, "Producer is not available"),
    spec!(ConsumerNotAvailable, "Consumer is not available"),
    spec!(Tools, "Tools operation failed"),
    spec!(Filter, "Filter operation failed"),
    // Observability boundary.
    spec!(ObservabilityFeatureDisabled, "Observability feature is disabled"),
    spec!(ObservabilityConfigInvalid, "Observability configuration is invalid"),
    spec!(
        ObservabilityMetricsInitFailed,
        "Observability metrics initialization failed"
    ),
    spec!(
        ObservabilityTracesInitFailed,
        "Observability traces initialization failed"
    ),
    spec!(ObservabilityLogsInitFailed, "Observability logs initialization failed"),
    spec!(
        ObservabilityLoggingInitFailed,
        "Observability logging initialization failed"
    ),
    spec!(ObservabilityLogFilterInvalid, "Observability log filter is invalid"),
    spec!(
        ObservabilitySubscriberInstallFailed,
        "Observability subscriber installation failed"
    ),
    spec!(
        ObservabilityMetricsShutdownFailed,
        "Observability metrics shutdown failed"
    ),
    spec!(
        ObservabilityTracesShutdownFailed,
        "Observability traces shutdown failed"
    ),
    spec!(ObservabilityLogsShutdownFailed, "Observability logs shutdown failed"),
    // Storage, configuration, and controller persistence boundaries.
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
    // System and compatibility boundaries.
    spec!(Io, "I/O operation failed"),
    spec!(IllegalArgument, "Argument is illegal"),
    spec!(Timeout, "Operation timed out"),
    spec!(Internal, "Internal error"),
    spec!(Service, "Service lifecycle operation failed"),
    spec!(InvalidVersionOrdinal, "Version ordinal is invalid"),
    spec!(NotInitialized, "Component is not initialized"),
    spec!(MissingRequiredMessageProperty, "Message is missing a required property"),
];
