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

use std::fmt;

/// Stable machine-readable error code.
///
/// `ErrorCode` values are intentionally separate from display messages. Protocol
/// mapping, retry policy, and observability should use the code, not formatted
/// error text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ErrorCode(&'static str);

impl ErrorCode {
    /// Create a stable error code.
    #[inline]
    pub const fn new(value: &'static str) -> Self {
        Self(value)
    }

    /// Return the stable code string.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// Architectural boundary where an error belongs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorScope {
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
    /// Represents the broker case.
    Broker,
    /// Represents the request case.
    Request,
    /// Represents the route case.
    Route,
    /// Represents the client case.
    Client,
    /// Represents the tools case.
    Tools,
    /// Represents the filter case.
    Filter,
    /// Represents the observability case.
    Observability,
    /// Represents the storage case.
    Storage,
    /// Represents the configuration case.
    Configuration,
    /// Represents the system case.
    System,
    /// Represents the version case.
    Version,
}

/// Low-cardinality domain category for metrics, logs, and external adapters.
///
/// `ErrorScope` points at the architectural boundary that produced the error.
/// `ErrorCategory` is intentionally coarser and should remain stable enough for
/// dashboards and alert grouping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorCategory {
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
    /// Represents the broker case.
    Broker,
    /// Represents the route case.
    Route,
    /// Represents the client case.
    Client,
    /// Represents the tools case.
    Tools,
    /// Represents the filter case.
    Filter,
    /// Represents the observability case.
    Observability,
    /// Represents the storage case.
    Storage,
    /// Represents the configuration case.
    Configuration,
    /// Represents the system case.
    System,
    /// Represents the version case.
    Version,
}

impl ErrorCategory {
    /// Return the stable category name used by external adapters.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Network => "network",
            Self::Serialization => "serialization",
            Self::Protocol => "protocol",
            Self::Rpc => "rpc",
            Self::Authentication => "authentication",
            Self::Controller => "controller",
            Self::Broker => "broker",
            Self::Route => "route",
            Self::Client => "client",
            Self::Tools => "tools",
            Self::Filter => "filter",
            Self::Observability => "observability",
            Self::Storage => "storage",
            Self::Configuration => "configuration",
            Self::System => "system",
            Self::Version => "version",
        }
    }
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Stable logical error kind.
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
    /// Represents the missing required message property case.
    MissingRequiredMessageProperty,
}

impl ErrorKind {
    /// Every public error kind currently known to the error kernel.
    pub const ALL: &'static [Self] = &[
        Self::Network,
        Self::Serialization,
        Self::Protocol,
        Self::Rpc,
        Self::Authentication,
        Self::Controller,
        Self::InvalidProperty,
        Self::BrokerNotFound,
        Self::BrokerRegistrationFailed,
        Self::BrokerOperationFailed,
        Self::TopicNotExist,
        Self::QueueNotExist,
        Self::SubscriptionGroupNotExist,
        Self::QueueIdOutOfRange,
        Self::MessageTooLarge,
        Self::MessageValidationFailed,
        Self::RetryLimitExceeded,
        Self::TransactionRejected,
        Self::BrokerPermissionDenied,
        Self::NotMasterBroker,
        Self::MessageLookupFailed,
        Self::QueryNotFound,
        Self::TopicSendingForbidden,
        Self::BrokerAsyncTaskFailed,
        Self::RequestBodyInvalid,
        Self::RequestHeaderError,
        Self::ResponseProcessFailed,
        Self::RouteNotFound,
        Self::RouteInconsistent,
        Self::RouteRegistrationConflict,
        Self::RouteVersionConflict,
        Self::ClusterNotFound,
        Self::ClientNotStarted,
        Self::ClientAlreadyStarted,
        Self::ClientShuttingDown,
        Self::ClientInvalidState,
        Self::ProducerNotAvailable,
        Self::ConsumerNotAvailable,
        Self::Tools,
        Self::Filter,
        Self::ObservabilityFeatureDisabled,
        Self::ObservabilityConfigInvalid,
        Self::ObservabilityMetricsInitFailed,
        Self::ObservabilityTracesInitFailed,
        Self::ObservabilityLogsInitFailed,
        Self::ObservabilityLoggingInitFailed,
        Self::ObservabilityLogFilterInvalid,
        Self::ObservabilitySubscriberInstallFailed,
        Self::ObservabilityMetricsShutdownFailed,
        Self::ObservabilityTracesShutdownFailed,
        Self::ObservabilityLogsShutdownFailed,
        Self::StorageReadFailed,
        Self::StorageWriteFailed,
        Self::StorageCorrupted,
        Self::StorageOutOfSpace,
        Self::StorageLockFailed,
        Self::ConfigParseFailed,
        Self::ConfigMissing,
        Self::ConfigInvalidValue,
        Self::AuthConfigInvalid,
        Self::AuthHotReloadFailed,
        Self::ControllerNotLeader,
        Self::ControllerRaftError,
        Self::ControllerConsensusTimeout,
        Self::ControllerSnapshotFailed,
        Self::Io,
        Self::IllegalArgument,
        Self::Timeout,
        Self::Internal,
        Self::Service,
        Self::InvalidVersionOrdinal,
        Self::NotInitialized,
        Self::MissingRequiredMessageProperty,
    ];

    /// Stable machine-readable code for this kind.
    #[inline]
    pub const fn code(self) -> ErrorCode {
        ErrorCode::new(match self {
            Self::Network => "NETWORK_CONNECTION_FAILED",
            Self::Serialization => "SERIALIZATION_FAILED",
            Self::Protocol => "PROTOCOL_ERROR",
            Self::Rpc => "RPC_ERROR",
            Self::Authentication => "AUTHENTICATION_FAILED",
            Self::Controller => "CONTROLLER_ERROR",
            Self::InvalidProperty => "INVALID_PROPERTY",
            Self::BrokerNotFound => "BROKER_NOT_FOUND",
            Self::BrokerRegistrationFailed => "BROKER_REGISTRATION_FAILED",
            Self::BrokerOperationFailed => "BROKER_OPERATION_FAILED",
            Self::TopicNotExist => "TOPIC_NOT_EXIST",
            Self::QueueNotExist => "QUEUE_NOT_EXIST",
            Self::SubscriptionGroupNotExist => "SUBSCRIPTION_GROUP_NOT_EXIST",
            Self::QueueIdOutOfRange => "QUEUE_ID_OUT_OF_RANGE",
            Self::MessageTooLarge => "MESSAGE_TOO_LARGE",
            Self::MessageValidationFailed => "MESSAGE_VALIDATION_FAILED",
            Self::RetryLimitExceeded => "RETRY_LIMIT_EXCEEDED",
            Self::TransactionRejected => "TRANSACTION_REJECTED",
            Self::BrokerPermissionDenied => "BROKER_PERMISSION_DENIED",
            Self::NotMasterBroker => "NOT_MASTER_BROKER",
            Self::MessageLookupFailed => "MESSAGE_LOOKUP_FAILED",
            Self::QueryNotFound => "QUERY_NOT_FOUND",
            Self::TopicSendingForbidden => "TOPIC_SENDING_FORBIDDEN",
            Self::BrokerAsyncTaskFailed => "BROKER_ASYNC_TASK_FAILED",
            Self::RequestBodyInvalid => "REQUEST_BODY_INVALID",
            Self::RequestHeaderError => "REQUEST_HEADER_ERROR",
            Self::ResponseProcessFailed => "RESPONSE_PROCESS_FAILED",
            Self::RouteNotFound => "ROUTE_NOT_FOUND",
            Self::RouteInconsistent => "ROUTE_INCONSISTENT",
            Self::RouteRegistrationConflict => "ROUTE_REGISTRATION_CONFLICT",
            Self::RouteVersionConflict => "ROUTE_VERSION_CONFLICT",
            Self::ClusterNotFound => "CLUSTER_NOT_FOUND",
            Self::ClientNotStarted => "CLIENT_NOT_STARTED",
            Self::ClientAlreadyStarted => "CLIENT_ALREADY_STARTED",
            Self::ClientShuttingDown => "CLIENT_SHUTTING_DOWN",
            Self::ClientInvalidState => "CLIENT_INVALID_STATE",
            Self::ProducerNotAvailable => "PRODUCER_NOT_AVAILABLE",
            Self::ConsumerNotAvailable => "CONSUMER_NOT_AVAILABLE",
            Self::Tools => "TOOLS_ERROR",
            Self::Filter => "FILTER_ERROR",
            Self::ObservabilityFeatureDisabled => "OBSERVABILITY_FEATURE_DISABLED",
            Self::ObservabilityConfigInvalid => "OBSERVABILITY_CONFIG_INVALID",
            Self::ObservabilityMetricsInitFailed => "OBSERVABILITY_METRICS_INIT_FAILED",
            Self::ObservabilityTracesInitFailed => "OBSERVABILITY_TRACES_INIT_FAILED",
            Self::ObservabilityLogsInitFailed => "OBSERVABILITY_LOGS_INIT_FAILED",
            Self::ObservabilityLoggingInitFailed => "OBSERVABILITY_LOGGING_INIT_FAILED",
            Self::ObservabilityLogFilterInvalid => "OBSERVABILITY_LOG_FILTER_INVALID",
            Self::ObservabilitySubscriberInstallFailed => "OBSERVABILITY_SUBSCRIBER_INSTALL_FAILED",
            Self::ObservabilityMetricsShutdownFailed => "OBSERVABILITY_METRICS_SHUTDOWN_FAILED",
            Self::ObservabilityTracesShutdownFailed => "OBSERVABILITY_TRACES_SHUTDOWN_FAILED",
            Self::ObservabilityLogsShutdownFailed => "OBSERVABILITY_LOGS_SHUTDOWN_FAILED",
            Self::StorageReadFailed => "STORAGE_READ_FAILED",
            Self::StorageWriteFailed => "STORAGE_WRITE_FAILED",
            Self::StorageCorrupted => "STORAGE_CORRUPTED",
            Self::StorageOutOfSpace => "STORAGE_OUT_OF_SPACE",
            Self::StorageLockFailed => "STORAGE_LOCK_FAILED",
            Self::ConfigParseFailed => "CONFIG_PARSE_FAILED",
            Self::ConfigMissing => "CONFIG_MISSING",
            Self::ConfigInvalidValue => "CONFIG_INVALID_VALUE",
            Self::AuthConfigInvalid => "AUTH_CONFIG_INVALID",
            Self::AuthHotReloadFailed => "AUTH_HOT_RELOAD_FAILED",
            Self::ControllerNotLeader => "CONTROLLER_NOT_LEADER",
            Self::ControllerRaftError => "CONTROLLER_RAFT_ERROR",
            Self::ControllerConsensusTimeout => "CONTROLLER_CONSENSUS_TIMEOUT",
            Self::ControllerSnapshotFailed => "CONTROLLER_SNAPSHOT_FAILED",
            Self::Io => "IO_ERROR",
            Self::IllegalArgument => "ILLEGAL_ARGUMENT",
            Self::Timeout => "TIMEOUT",
            Self::Internal => "INTERNAL",
            Self::Service => "SERVICE_ERROR",
            Self::InvalidVersionOrdinal => "INVALID_VERSION_ORDINAL",
            Self::NotInitialized => "NOT_INITIALIZED",
            Self::MissingRequiredMessageProperty => "MISSING_REQUIRED_MESSAGE_PROPERTY",
        })
    }

    /// Architectural boundary for this kind.
    #[inline]
    pub const fn scope(self) -> ErrorScope {
        match self {
            Self::Network => ErrorScope::Network,
            Self::Serialization => ErrorScope::Serialization,
            Self::Protocol => ErrorScope::Protocol,
            Self::Rpc => ErrorScope::Rpc,
            Self::Authentication => ErrorScope::Authentication,
            Self::Controller
            | Self::ControllerNotLeader
            | Self::ControllerRaftError
            | Self::ControllerConsensusTimeout
            | Self::ControllerSnapshotFailed => ErrorScope::Controller,
            Self::InvalidProperty
            | Self::BrokerNotFound
            | Self::BrokerRegistrationFailed
            | Self::BrokerOperationFailed
            | Self::TopicNotExist
            | Self::QueueNotExist
            | Self::SubscriptionGroupNotExist
            | Self::QueueIdOutOfRange
            | Self::MessageTooLarge
            | Self::MessageValidationFailed
            | Self::RetryLimitExceeded
            | Self::TransactionRejected
            | Self::BrokerPermissionDenied
            | Self::NotMasterBroker
            | Self::MessageLookupFailed
            | Self::QueryNotFound
            | Self::TopicSendingForbidden
            | Self::BrokerAsyncTaskFailed => ErrorScope::Broker,
            Self::RequestBodyInvalid | Self::RequestHeaderError | Self::ResponseProcessFailed => ErrorScope::Request,
            Self::RouteNotFound
            | Self::RouteInconsistent
            | Self::RouteRegistrationConflict
            | Self::RouteVersionConflict
            | Self::ClusterNotFound => ErrorScope::Route,
            Self::ClientNotStarted
            | Self::ClientAlreadyStarted
            | Self::ClientShuttingDown
            | Self::ClientInvalidState
            | Self::ProducerNotAvailable
            | Self::ConsumerNotAvailable => ErrorScope::Client,
            Self::Tools => ErrorScope::Tools,
            Self::Filter => ErrorScope::Filter,
            Self::ObservabilityFeatureDisabled
            | Self::ObservabilityConfigInvalid
            | Self::ObservabilityMetricsInitFailed
            | Self::ObservabilityTracesInitFailed
            | Self::ObservabilityLogsInitFailed
            | Self::ObservabilityLoggingInitFailed
            | Self::ObservabilityLogFilterInvalid
            | Self::ObservabilitySubscriberInstallFailed
            | Self::ObservabilityMetricsShutdownFailed
            | Self::ObservabilityTracesShutdownFailed
            | Self::ObservabilityLogsShutdownFailed => ErrorScope::Observability,
            Self::StorageReadFailed
            | Self::StorageWriteFailed
            | Self::StorageCorrupted
            | Self::StorageOutOfSpace
            | Self::StorageLockFailed => ErrorScope::Storage,
            Self::ConfigParseFailed | Self::ConfigMissing | Self::ConfigInvalidValue | Self::AuthConfigInvalid => {
                ErrorScope::Configuration
            }
            Self::AuthHotReloadFailed => ErrorScope::Authentication,
            Self::Io
            | Self::IllegalArgument
            | Self::Timeout
            | Self::Internal
            | Self::Service
            | Self::NotInitialized => ErrorScope::System,
            Self::InvalidVersionOrdinal => ErrorScope::Version,
            Self::MissingRequiredMessageProperty => ErrorScope::Protocol,
        }
    }

    /// Low-cardinality domain category for this kind.
    #[inline]
    pub const fn category(self) -> ErrorCategory {
        match self {
            Self::Network => ErrorCategory::Network,
            Self::Serialization => ErrorCategory::Serialization,
            Self::Protocol
            | Self::RequestBodyInvalid
            | Self::RequestHeaderError
            | Self::ResponseProcessFailed
            | Self::MissingRequiredMessageProperty => ErrorCategory::Protocol,
            Self::Rpc => ErrorCategory::Rpc,
            Self::Authentication
            | Self::AuthHotReloadFailed
            | Self::BrokerPermissionDenied
            | Self::TopicSendingForbidden => ErrorCategory::Authentication,
            Self::Controller
            | Self::ControllerNotLeader
            | Self::ControllerRaftError
            | Self::ControllerConsensusTimeout
            | Self::ControllerSnapshotFailed => ErrorCategory::Controller,
            Self::InvalidProperty
            | Self::BrokerNotFound
            | Self::BrokerRegistrationFailed
            | Self::BrokerOperationFailed
            | Self::TopicNotExist
            | Self::QueueNotExist
            | Self::SubscriptionGroupNotExist
            | Self::QueueIdOutOfRange
            | Self::MessageTooLarge
            | Self::MessageValidationFailed
            | Self::RetryLimitExceeded
            | Self::TransactionRejected
            | Self::NotMasterBroker
            | Self::MessageLookupFailed
            | Self::QueryNotFound
            | Self::BrokerAsyncTaskFailed => ErrorCategory::Broker,
            Self::RouteNotFound
            | Self::RouteInconsistent
            | Self::RouteRegistrationConflict
            | Self::RouteVersionConflict
            | Self::ClusterNotFound => ErrorCategory::Route,
            Self::ClientNotStarted
            | Self::ClientAlreadyStarted
            | Self::ClientShuttingDown
            | Self::ClientInvalidState
            | Self::ProducerNotAvailable
            | Self::ConsumerNotAvailable => ErrorCategory::Client,
            Self::Tools => ErrorCategory::Tools,
            Self::Filter => ErrorCategory::Filter,
            Self::ObservabilityFeatureDisabled
            | Self::ObservabilityConfigInvalid
            | Self::ObservabilityMetricsInitFailed
            | Self::ObservabilityTracesInitFailed
            | Self::ObservabilityLogsInitFailed
            | Self::ObservabilityLoggingInitFailed
            | Self::ObservabilityLogFilterInvalid
            | Self::ObservabilitySubscriberInstallFailed
            | Self::ObservabilityMetricsShutdownFailed
            | Self::ObservabilityTracesShutdownFailed
            | Self::ObservabilityLogsShutdownFailed => ErrorCategory::Observability,
            Self::StorageReadFailed
            | Self::StorageWriteFailed
            | Self::StorageCorrupted
            | Self::StorageOutOfSpace
            | Self::StorageLockFailed => ErrorCategory::Storage,
            Self::ConfigParseFailed | Self::ConfigMissing | Self::ConfigInvalidValue | Self::AuthConfigInvalid => {
                ErrorCategory::Configuration
            }
            Self::Io
            | Self::IllegalArgument
            | Self::Timeout
            | Self::Internal
            | Self::Service
            | Self::NotInitialized => ErrorCategory::System,
            Self::InvalidVersionOrdinal => ErrorCategory::Version,
        }
    }

    /// Resolve an error kind from a stable code.
    #[inline]
    pub fn from_code(code: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|kind| kind.code().as_str() == code)
    }
}
