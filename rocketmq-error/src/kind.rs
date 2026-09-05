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

/// Structural classification retained by the legacy typed error facade.
///
/// Stable identity and policy come from [`ErrorDescriptor`](crate::ErrorDescriptor),
/// not from this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorKind {
    /// Represents the network case.
    Network,
    /// Represents the serialization case.
    Serialization,
    /// Represents the protocol case.
    Protocol,
    /// Represents the rpc case.
    Rpc,
    /// Represents the authentication case.
    Authentication,
    /// Represents the controller case.
    Controller,
    /// Represents the invalid property case.
    InvalidProperty,
    /// Represents the broker not found case.
    BrokerNotFound,
    /// Represents the broker registration failed case.
    BrokerRegistrationFailed,
    /// Represents the broker operation failed case.
    BrokerOperationFailed,
    /// Represents the topic not exist case.
    TopicNotExist,
    /// Represents the queue not exist case.
    QueueNotExist,
    /// Represents the subscription group not exist case.
    SubscriptionGroupNotExist,
    /// Represents the queue id out of range case.
    QueueIdOutOfRange,
    /// Represents the message too large case.
    MessageTooLarge,
    /// Represents the message validation failed case.
    MessageValidationFailed,
    /// Represents the retry limit exceeded case.
    RetryLimitExceeded,
    /// Represents the transaction rejected case.
    TransactionRejected,
    /// Represents the broker permission denied case.
    BrokerPermissionDenied,
    /// Represents the not master broker case.
    NotMasterBroker,
    /// Represents the message lookup failed case.
    MessageLookupFailed,
    /// Represents the query not found case.
    QueryNotFound,
    /// Represents the topic sending forbidden case.
    TopicSendingForbidden,
    /// Represents the broker async task failed case.
    BrokerAsyncTaskFailed,
    /// Represents the request body invalid case.
    RequestBodyInvalid,
    /// Represents the request header error case.
    RequestHeaderError,
    /// Represents the response process failed case.
    ResponseProcessFailed,
    /// Represents the route not found case.
    RouteNotFound,
    /// Represents the route inconsistent case.
    RouteInconsistent,
    /// Represents the route registration conflict case.
    RouteRegistrationConflict,
    /// Represents the route version conflict case.
    RouteVersionConflict,
    /// Represents the cluster not found case.
    ClusterNotFound,
    /// Represents the client not started case.
    ClientNotStarted,
    /// Represents the client already started case.
    ClientAlreadyStarted,
    /// Represents the client shutting down case.
    ClientShuttingDown,
    /// Represents the client invalid state case.
    ClientInvalidState,
    /// Represents the producer not available case.
    ProducerNotAvailable,
    /// Represents the consumer not available case.
    ConsumerNotAvailable,
    /// Represents the tools case.
    Tools,
    /// Represents the filter case.
    Filter,
    /// Represents the observability feature disabled case.
    ObservabilityFeatureDisabled,
    /// Represents the observability config invalid case.
    ObservabilityConfigInvalid,
    /// Represents the observability metrics init failed case.
    ObservabilityMetricsInitFailed,
    /// Represents the observability traces init failed case.
    ObservabilityTracesInitFailed,
    /// Represents the observability logs init failed case.
    ObservabilityLogsInitFailed,
    /// Represents the observability logging init failed case.
    ObservabilityLoggingInitFailed,
    /// Represents the observability log filter invalid case.
    ObservabilityLogFilterInvalid,
    /// Represents the observability subscriber install failed case.
    ObservabilitySubscriberInstallFailed,
    /// Represents the observability metrics shutdown failed case.
    ObservabilityMetricsShutdownFailed,
    /// Represents the observability traces shutdown failed case.
    ObservabilityTracesShutdownFailed,
    /// Represents the observability logs shutdown failed case.
    ObservabilityLogsShutdownFailed,
    /// Represents the storage read failed case.
    StorageReadFailed,
    /// Represents the storage write failed case.
    StorageWriteFailed,
    /// Represents the storage corrupted case.
    StorageCorrupted,
    /// Represents the storage out of space case.
    StorageOutOfSpace,
    /// Represents the storage lock failed case.
    StorageLockFailed,
    /// Represents the config parse failed case.
    ConfigParseFailed,
    /// Represents the config missing case.
    ConfigMissing,
    /// Represents the config invalid value case.
    ConfigInvalidValue,
    /// Represents the auth config invalid case.
    AuthConfigInvalid,
    /// Represents the auth hot reload failed case.
    AuthHotReloadFailed,
    /// Represents the controller not leader case.
    ControllerNotLeader,
    /// Represents the controller raft error case.
    ControllerRaftError,
    /// Represents the controller consensus timeout case.
    ControllerConsensusTimeout,
    /// Represents the controller snapshot failed case.
    ControllerSnapshotFailed,
    /// Represents the io case.
    Io,
    /// Represents the illegal argument case.
    IllegalArgument,
    /// Represents the timeout case.
    Timeout,
    /// Represents the internal case.
    Internal,
    /// Represents the service case.
    Service,
    /// Represents the invalid version ordinal case.
    InvalidVersionOrdinal,
    /// Represents the not initialized case.
    NotInitialized,
}
