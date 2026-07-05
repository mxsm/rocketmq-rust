# Error Codes

The canonical registry lives in `rocketmq-error/src/spec.rs`.
Every public `ErrorKind` must have exactly one `ErrorSpec`, and the registry tests enforce this invariant.
Boundary adapters must use `RocketMQError::boundary_view()` to build a `BoundaryErrorView`, or use `ErrorSpec` protocol primitives directly, instead of parsing `Display` text.

Each `ErrorSpec` provides:

- stable machine-readable code from `ErrorKind::code()`;
- scope and low-cardinality category from `ErrorKind`;
- public message for protocol and CLI surfaces;
- remoting, gRPC, HTTP, and CLI mapping primitives;
- retry policy through `RecoverySpec`;
- severity and metric label through `ObserveSpec`;
- redaction policy through `RedactionPolicy`.

## Registry

| Error kind | Stable code | Public message |
| --- | --- | --- |
| `Network` | `NETWORK_CONNECTION_FAILED` | Network operation failed |
| `Serialization` | `SERIALIZATION_FAILED` | Serialization failed |
| `Protocol` | `PROTOCOL_ERROR` | Protocol error |
| `Rpc` | `RPC_ERROR` | RPC operation failed |
| `Authentication` | `AUTHENTICATION_FAILED` | Authentication failed |
| `Controller` | `CONTROLLER_ERROR` | Controller operation failed |
| `InvalidProperty` | `INVALID_PROPERTY` | Message property is invalid |
| `BrokerNotFound` | `BROKER_NOT_FOUND` | Broker was not found |
| `BrokerRegistrationFailed` | `BROKER_REGISTRATION_FAILED` | Broker registration failed |
| `BrokerOperationFailed` | `BROKER_OPERATION_FAILED` | Broker operation failed |
| `TopicNotExist` | `TOPIC_NOT_EXIST` | Topic does not exist |
| `QueueNotExist` | `QUEUE_NOT_EXIST` | Queue does not exist |
| `SubscriptionGroupNotExist` | `SUBSCRIPTION_GROUP_NOT_EXIST` | Subscription group does not exist |
| `QueueIdOutOfRange` | `QUEUE_ID_OUT_OF_RANGE` | Queue id is out of range |
| `MessageTooLarge` | `MESSAGE_TOO_LARGE` | Message body is too large |
| `MessageValidationFailed` | `MESSAGE_VALIDATION_FAILED` | Message validation failed |
| `RetryLimitExceeded` | `RETRY_LIMIT_EXCEEDED` | Retry limit was exceeded |
| `TransactionRejected` | `TRANSACTION_REJECTED` | Transaction message was rejected |
| `BrokerPermissionDenied` | `BROKER_PERMISSION_DENIED` | Broker permission was denied |
| `NotMasterBroker` | `NOT_MASTER_BROKER` | Broker is not the master |
| `MessageLookupFailed` | `MESSAGE_LOOKUP_FAILED` | Message lookup failed |
| `QueryNotFound` | `QUERY_NOT_FOUND` | Query result was not found |
| `TopicSendingForbidden` | `TOPIC_SENDING_FORBIDDEN` | Topic sending is forbidden |
| `BrokerAsyncTaskFailed` | `BROKER_ASYNC_TASK_FAILED` | Broker asynchronous operation failed |
| `RequestBodyInvalid` | `REQUEST_BODY_INVALID` | Request body is invalid |
| `RequestHeaderError` | `REQUEST_HEADER_ERROR` | Request header is invalid |
| `ResponseProcessFailed` | `RESPONSE_PROCESS_FAILED` | Response processing failed |
| `RouteNotFound` | `ROUTE_NOT_FOUND` | Route information was not found |
| `RouteInconsistent` | `ROUTE_INCONSISTENT` | Route data is inconsistent |
| `RouteRegistrationConflict` | `ROUTE_REGISTRATION_CONFLICT` | Route registration conflict |
| `RouteVersionConflict` | `ROUTE_VERSION_CONFLICT` | Route version conflict |
| `ClusterNotFound` | `CLUSTER_NOT_FOUND` | Cluster was not found |
| `ClientNotStarted` | `CLIENT_NOT_STARTED` | Client is not started |
| `ClientAlreadyStarted` | `CLIENT_ALREADY_STARTED` | Client is already started |
| `ClientShuttingDown` | `CLIENT_SHUTTING_DOWN` | Client is shutting down |
| `ClientInvalidState` | `CLIENT_INVALID_STATE` | Client state is invalid |
| `ProducerNotAvailable` | `PRODUCER_NOT_AVAILABLE` | Producer is not available |
| `ConsumerNotAvailable` | `CONSUMER_NOT_AVAILABLE` | Consumer is not available |
| `Tools` | `TOOLS_ERROR` | Tools operation failed |
| `Filter` | `FILTER_ERROR` | Filter operation failed |
| `StorageReadFailed` | `STORAGE_READ_FAILED` | Storage read failed |
| `StorageWriteFailed` | `STORAGE_WRITE_FAILED` | Storage write failed |
| `StorageCorrupted` | `STORAGE_CORRUPTED` | Storage data is corrupted |
| `StorageOutOfSpace` | `STORAGE_OUT_OF_SPACE` | Storage is out of space |
| `StorageLockFailed` | `STORAGE_LOCK_FAILED` | Storage lock failed |
| `ConfigParseFailed` | `CONFIG_PARSE_FAILED` | Configuration parsing failed |
| `ConfigMissing` | `CONFIG_MISSING` | Required configuration is missing |
| `ConfigInvalidValue` | `CONFIG_INVALID_VALUE` | Configuration value is invalid |
| `AuthConfigInvalid` | `AUTH_CONFIG_INVALID` | Authentication configuration is invalid |
| `AuthHotReloadFailed` | `AUTH_HOT_RELOAD_FAILED` | Authentication hot reload failed |
| `ControllerNotLeader` | `CONTROLLER_NOT_LEADER` | Controller is not the leader |
| `ControllerRaftError` | `CONTROLLER_RAFT_ERROR` | Controller raft operation failed |
| `ControllerConsensusTimeout` | `CONTROLLER_CONSENSUS_TIMEOUT` | Controller consensus operation timed out |
| `ControllerSnapshotFailed` | `CONTROLLER_SNAPSHOT_FAILED` | Controller snapshot operation failed |
| `Io` | `IO_ERROR` | I/O operation failed |
| `IllegalArgument` | `ILLEGAL_ARGUMENT` | Argument is illegal |
| `Timeout` | `TIMEOUT` | Operation timed out |
| `Internal` | `INTERNAL` | Internal error |
| `Service` | `SERVICE_ERROR` | Service lifecycle operation failed |
| `InvalidVersionOrdinal` | `INVALID_VERSION_ORDINAL` | Version ordinal is invalid |
| `NotInitialized` | `NOT_INITIALIZED` | Component is not initialized |
| `MissingRequiredMessageProperty` | `MISSING_REQUIRED_MESSAGE_PROPERTY` | Message is missing a required property |

## Update Checklist

- Add the new `ErrorKind` variant and stable code.
- Add one `ErrorSpec` entry with a public message.
- Confirm remoting, gRPC, HTTP, and CLI mappings are correct for the kind.
- Confirm retry class, severity, metric label, and redaction policy are correct.
- Add or update tests under `rocketmq-error/tests`.
- Run `python scripts/error_architecture_guard.py`.
