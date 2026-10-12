// Copyright 2026 The RocketMQ Rust Authors
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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::audit::{AuditEvent, AuditFuture, AuditMode, AuditRecord, AuditResult, AuditTrail};
use crate::error::{ControlError, ControlErrorCode, ERROR_SCHEMA_VERSION};
use crate::model::{ClusterName, ControlOperation, MUTATION_ARGUMENTS_SCHEMA_VERSION};
use crate::tool_runtime::{
    MutationToolRequest, MutationToolResponse, MutationToolSession, MutationToolSessionFactory, RuntimeFuture,
};
use crate::tools;

const SAFE_OPERATOR: &str = "operator@example.test";
const SAFE_REASON: &str = "approved matrix change";
const BACKEND_CREDENTIAL: &str = "access-secret-must-not-leak";
const BACKEND_ENDPOINT: &str = "broker-a:10911";
const RAW_BACKEND_ERROR: &str = "raw-backend-error-must-not-leak";
const MESSAGE_BODY: &str = "message-body-must-not-leak";
const CLIENT_IDENTITY: &str = "client-identity-must-not-leak";
/// The identifier of the drain or log filter that the matrix calls name. It is the caller's text
/// and belongs on no surface.
const OPERATION_ID: &str = "matrix-operation-1";
const SENSITIVE_BACKEND_FAILURE: &str = "access-secret-must-not-leak broker-a:10911 raw-backend-error-must-not-leak message-body-must-not-leak client-identity-must-not-leak";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolKind {
    Topic,
    ConsumerGroup,
    Offset,
    BrokerConfig,
    RequestMode,
    DrainBegin,
    DrainCancel,
    LogFilterSet,
    LogFilterRestore,
}

impl ToolKind {
    const ALL: [Self; 9] = [
        Self::Topic,
        Self::ConsumerGroup,
        Self::Offset,
        Self::BrokerConfig,
        Self::RequestMode,
        Self::DrainBegin,
        Self::DrainCancel,
        Self::LogFilterSet,
        Self::LogFilterRestore,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Topic => tools::UPSERT_TOPIC_TOOL,
            Self::ConsumerGroup => tools::UPSERT_CONSUMER_GROUP_TOOL,
            Self::Offset => tools::RESET_CONSUMER_OFFSET_TOOL,
            Self::BrokerConfig => tools::PATCH_BROKER_CONFIG_TOOL,
            Self::RequestMode => tools::SET_CONSUMER_REQUEST_MODE_TOOL,
            Self::DrainBegin => tools::BEGIN_PROXY_DRAIN_TOOL,
            Self::DrainCancel => tools::CANCEL_PROXY_DRAIN_TOOL,
            Self::LogFilterSet => tools::SET_BROKER_LOG_FILTER_TTL_TOOL,
            Self::LogFilterRestore => tools::RESTORE_BROKER_LOG_FILTER_TOOL,
        }
    }

    const fn operation(self) -> ControlOperation {
        match self {
            Self::Topic => ControlOperation::TopicUpsert,
            Self::ConsumerGroup => ControlOperation::ConsumerGroupUpsert,
            Self::Offset => ControlOperation::ConsumerOffsetReset,
            Self::BrokerConfig => ControlOperation::BrokerConfigPatch,
            Self::RequestMode => ControlOperation::ConsumerRequestMode,
            Self::DrainBegin => ControlOperation::ProxyDrainBegin,
            Self::DrainCancel => ControlOperation::ProxyDrainCancel,
            Self::LogFilterSet => ControlOperation::BrokerLogFilterSet,
            Self::LogFilterRestore => ControlOperation::BrokerLogFilterRestore,
        }
    }

    const fn claim(self) -> &'static str {
        self.operation().as_str()
    }

    const fn slug(self) -> &'static str {
        match self {
            Self::Topic => "topic",
            Self::ConsumerGroup => "group",
            Self::Offset => "offset",
            Self::BrokerConfig => "broker",
            Self::RequestMode => "mode",
            Self::DrainBegin => "drain-begin",
            Self::DrainCancel => "drain-cancel",
            Self::LogFilterSet => "log-filter-set",
            Self::LogFilterRestore => "log-filter-restore",
        }
    }

    const fn logical_targets(self) -> &'static [&'static str] {
        match self {
            Self::Topic => &["orders", "broker-a"],
            Self::ConsumerGroup => &["orders_consumers", "broker-a"],
            Self::Offset | Self::RequestMode => &["orders", "orders_consumers"],
            Self::BrokerConfig | Self::LogFilterSet | Self::LogFilterRestore => &["broker-a"],
            Self::DrainBegin | Self::DrainCancel => &["proxy-a"],
        }
    }

    /// For a Tool that changes exactly one target: the argument that names it, and the list
    /// argument that it must not accept.
    const fn single_target(self) -> Option<(&'static str, &'static str)> {
        match self {
            Self::BrokerConfig | Self::LogFilterSet | Self::LogFilterRestore => Some(("broker_name", "broker_names")),
            Self::DrainBegin | Self::DrainCancel => Some(("proxy_name", "proxy_names")),
            Self::Topic | Self::ConsumerGroup | Self::Offset | Self::RequestMode => None,
        }
    }

    /// Whether `arguments` decode as the input of this Tool.
    fn decodes(self, arguments: serde_json::Value) -> bool {
        match self {
            Self::Topic => serde_json::from_value::<tools::UpsertTopicArgs>(arguments).is_ok(),
            Self::ConsumerGroup => serde_json::from_value::<tools::UpsertConsumerGroupArgs>(arguments).is_ok(),
            Self::Offset => serde_json::from_value::<tools::ResetConsumerOffsetArgs>(arguments).is_ok(),
            Self::BrokerConfig => serde_json::from_value::<tools::PatchBrokerConfigArgs>(arguments).is_ok(),
            Self::RequestMode => serde_json::from_value::<tools::SetConsumerRequestModeArgs>(arguments).is_ok(),
            Self::DrainBegin | Self::DrainCancel => serde_json::from_value::<tools::ProxyDrainArgs>(arguments).is_ok(),
            Self::LogFilterSet => serde_json::from_value::<tools::SetBrokerLogFilterTtlArgs>(arguments).is_ok(),
            Self::LogFilterRestore => serde_json::from_value::<tools::RestoreBrokerLogFilterArgs>(arguments).is_ok(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    ValidDryRun,
    ValidExecute,
    ExecuteWithoutConfirm,
    ExecuteWithoutReason,
    MissingWriteScope,
    ClusterNotAllowed,
    OperationNotAllowed,
    ArgumentOutOfBounds,
    CasConflict,
    SingleTargetFailure,
    MultiTargetPartial,
    PostReadFailure,
    SessionShutdownFailure,
    AuditWriteFailure,
    SensitiveRuntimeFailure,
}

impl Scenario {
    const ALL: [Self; 14] = [
        Self::ValidDryRun,
        Self::ValidExecute,
        Self::ExecuteWithoutConfirm,
        Self::ExecuteWithoutReason,
        Self::MissingWriteScope,
        Self::ClusterNotAllowed,
        Self::OperationNotAllowed,
        Self::ArgumentOutOfBounds,
        Self::CasConflict,
        Self::SingleTargetFailure,
        Self::MultiTargetPartial,
        Self::PostReadFailure,
        Self::SessionShutdownFailure,
        Self::AuditWriteFailure,
    ];

    const fn slug(self) -> &'static str {
        match self {
            Self::ValidDryRun => "dry-run",
            Self::ValidExecute => "execute",
            Self::ExecuteWithoutConfirm => "no-confirm",
            Self::ExecuteWithoutReason => "no-reason",
            Self::MissingWriteScope => "no-scope",
            Self::ClusterNotAllowed => "cluster-denied",
            Self::OperationNotAllowed => "operation-denied",
            Self::ArgumentOutOfBounds => "bounds",
            Self::CasConflict => "conflict",
            Self::SingleTargetFailure => "single-failure",
            Self::MultiTargetPartial => "partial",
            Self::PostReadFailure => "postread",
            Self::SessionShutdownFailure => "shutdown",
            Self::AuditWriteFailure => "audit",
            Self::SensitiveRuntimeFailure => "sensitive-runtime-failure",
        }
    }

    const fn expected_status(self) -> Option<tools::MutationStatus> {
        match self {
            Self::ValidDryRun => Some(tools::MutationStatus::Planned),
            Self::ValidExecute | Self::SessionShutdownFailure => Some(tools::MutationStatus::Applied),
            Self::CasConflict => Some(tools::MutationStatus::Conflict),
            Self::SingleTargetFailure | Self::PostReadFailure => Some(tools::MutationStatus::Failed),
            Self::MultiTargetPartial => Some(tools::MutationStatus::Partial),
            Self::ExecuteWithoutConfirm
            | Self::ExecuteWithoutReason
            | Self::MissingWriteScope
            | Self::ClusterNotAllowed
            | Self::OperationNotAllowed
            | Self::ArgumentOutOfBounds
            | Self::AuditWriteFailure
            | Self::SensitiveRuntimeFailure => None,
        }
    }

    const fn expected_code(self) -> Option<ControlErrorCode> {
        match self {
            Self::ValidDryRun | Self::ValidExecute => None,
            Self::ExecuteWithoutConfirm => Some(ControlErrorCode::ConfirmationRequired),
            Self::ExecuteWithoutReason | Self::ArgumentOutOfBounds => Some(ControlErrorCode::InvalidArgument),
            Self::MissingWriteScope => Some(ControlErrorCode::PermissionDenied),
            Self::ClusterNotAllowed => Some(ControlErrorCode::ClusterNotAllowed),
            Self::OperationNotAllowed => Some(ControlErrorCode::OperationNotAllowed),
            Self::CasConflict => Some(ControlErrorCode::PreconditionConflict),
            Self::SingleTargetFailure => Some(ControlErrorCode::ExecutionFailed),
            Self::MultiTargetPartial => Some(ControlErrorCode::PartialApply),
            Self::PostReadFailure => Some(ControlErrorCode::VerificationFailed),
            Self::SessionShutdownFailure => Some(ControlErrorCode::ShutdownFailed),
            Self::AuditWriteFailure => Some(ControlErrorCode::AuditUnavailable),
            Self::SensitiveRuntimeFailure => Some(ControlErrorCode::ExecutionFailed),
        }
    }

    const fn reaches_runtime(self) -> bool {
        matches!(
            self,
            Self::ValidDryRun
                | Self::ValidExecute
                | Self::CasConflict
                | Self::SingleTargetFailure
                | Self::MultiTargetPartial
                | Self::PostReadFailure
                | Self::SessionShutdownFailure
                | Self::AuditWriteFailure
                | Self::SensitiveRuntimeFailure
        )
    }

    /// Sessions that two identical calls carrying one request key open.
    ///
    /// A settled outcome is replayed for the second call. A plan, a failure and an unverified
    /// write are not kept, so the second call runs again.
    const fn sessions_for_two_calls(self) -> usize {
        match self {
            Self::ValidExecute | Self::CasConflict | Self::MultiTargetPartial => 1,
            Self::ValidDryRun
            | Self::SingleTargetFailure
            | Self::PostReadFailure
            | Self::SessionShutdownFailure
            | Self::SensitiveRuntimeFailure => 2,
            Self::ExecuteWithoutConfirm
            | Self::ExecuteWithoutReason
            | Self::MissingWriteScope
            | Self::ClusterNotAllowed
            | Self::OperationNotAllowed
            | Self::ArgumentOutOfBounds
            | Self::AuditWriteFailure => 0,
        }
    }

    const fn expected_audit_result(self) -> Option<AuditResult> {
        match self {
            Self::ValidDryRun => Some(AuditResult::Planned),
            Self::ValidExecute => Some(AuditResult::Applied),
            Self::CasConflict => Some(AuditResult::Conflict),
            Self::SingleTargetFailure | Self::PostReadFailure | Self::SessionShutdownFailure => {
                Some(AuditResult::Failed)
            }
            Self::MultiTargetPartial => Some(AuditResult::Partial),
            Self::SensitiveRuntimeFailure => Some(AuditResult::Failed),
            Self::ExecuteWithoutConfirm
            | Self::ExecuteWithoutReason
            | Self::MissingWriteScope
            | Self::ClusterNotAllowed
            | Self::OperationNotAllowed
            | Self::ArgumentOutOfBounds
            | Self::AuditWriteFailure => None,
        }
    }
}

#[derive(Default)]
struct LifecycleCounters {
    starts: AtomicUsize,
    runs: AtomicUsize,
    preflight_reads: AtomicUsize,
    mutation_calls: AtomicUsize,
    verification_reads: AtomicUsize,
    shutdowns: AtomicUsize,
    sensitive_failures: AtomicUsize,
}

struct FakeMutationAdminFactory {
    scenario: Scenario,
    counters: Arc<LifecycleCounters>,
}

impl MutationToolSessionFactory for FakeMutationAdminFactory {
    fn open<'a>(
        &'a self,
        _cluster: &'a ClusterName,
    ) -> RuntimeFuture<'a, Result<Box<dyn MutationToolSession>, ControlError>> {
        Box::pin(async move {
            self.counters.starts.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(FakeMutationAdminSession {
                scenario: self.scenario,
                counters: Arc::clone(&self.counters),
            }) as Box<dyn MutationToolSession>)
        })
    }
}

struct FakeMutationAdminSession {
    scenario: Scenario,
    counters: Arc<LifecycleCounters>,
}

struct SensitiveOpenFailureFactory {
    opens: Arc<AtomicUsize>,
}

impl MutationToolSessionFactory for SensitiveOpenFailureFactory {
    fn open<'a>(
        &'a self,
        _cluster: &'a ClusterName,
    ) -> RuntimeFuture<'a, Result<Box<dyn MutationToolSession>, ControlError>> {
        Box::pin(async move {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Err(ControlError::new(
                ControlErrorCode::ExecutionFailed,
                SENSITIVE_BACKEND_FAILURE,
                true,
            ))
        })
    }
}

#[derive(Clone, Copy)]
enum HostileFailureStage {
    Open,
    Run,
}

struct HostileErrorFactory {
    code: ControlErrorCode,
    retryable: bool,
    stage: HostileFailureStage,
    gate: Arc<tokio::sync::Notify>,
    opens: Arc<AtomicUsize>,
    runs: Arc<AtomicUsize>,
    shutdowns: Arc<AtomicUsize>,
}

impl MutationToolSessionFactory for HostileErrorFactory {
    fn open<'a>(
        &'a self,
        _cluster: &'a ClusterName,
    ) -> RuntimeFuture<'a, Result<Box<dyn MutationToolSession>, ControlError>> {
        Box::pin(async move {
            self.opens.fetch_add(1, Ordering::SeqCst);
            if matches!(self.stage, HostileFailureStage::Open) {
                self.gate.notified().await;
                return Err(ControlError::new(self.code, SENSITIVE_BACKEND_FAILURE, self.retryable));
            }
            Ok(Box::new(HostileErrorSession {
                code: self.code,
                retryable: self.retryable,
                gate: Arc::clone(&self.gate),
                runs: Arc::clone(&self.runs),
                shutdowns: Arc::clone(&self.shutdowns),
            }) as Box<dyn MutationToolSession>)
        })
    }
}

struct HostileErrorSession {
    code: ControlErrorCode,
    retryable: bool,
    gate: Arc<tokio::sync::Notify>,
    runs: Arc<AtomicUsize>,
    shutdowns: Arc<AtomicUsize>,
}

impl MutationToolSession for HostileErrorSession {
    fn run<'a>(
        &'a mut self,
        _request: MutationToolRequest,
    ) -> RuntimeFuture<'a, Result<MutationToolResponse, ControlError>> {
        Box::pin(async move {
            self.runs.fetch_add(1, Ordering::SeqCst);
            self.gate.notified().await;
            Err(ControlError::new(self.code, SENSITIVE_BACKEND_FAILURE, self.retryable))
        })
    }

    fn shutdown(&mut self) -> RuntimeFuture<'_, Result<(), ControlError>> {
        Box::pin(async move {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

impl MutationToolSession for FakeMutationAdminSession {
    fn run<'a>(
        &'a mut self,
        request: MutationToolRequest,
    ) -> RuntimeFuture<'a, Result<MutationToolResponse, ControlError>> {
        Box::pin(async move {
            self.counters.runs.fetch_add(1, Ordering::SeqCst);
            self.counters.preflight_reads.fetch_add(1, Ordering::SeqCst);
            if self.scenario == Scenario::SensitiveRuntimeFailure {
                self.counters.sensitive_failures.fetch_add(1, Ordering::SeqCst);
                return Err(ControlError::new(
                    ControlErrorCode::ExecutionFailed,
                    SENSITIVE_BACKEND_FAILURE,
                    true,
                ));
            }
            if self.scenario != Scenario::ValidDryRun {
                self.counters.mutation_calls.fetch_add(1, Ordering::SeqCst);
            }
            if matches!(
                self.scenario,
                Scenario::ValidExecute
                    | Scenario::MultiTargetPartial
                    | Scenario::PostReadFailure
                    | Scenario::SessionShutdownFailure
            ) {
                self.counters.verification_reads.fetch_add(1, Ordering::SeqCst);
            }
            Ok(response_for(request, self.scenario))
        })
    }

    fn shutdown(&mut self) -> RuntimeFuture<'_, Result<(), ControlError>> {
        Box::pin(async move {
            self.counters.shutdowns.fetch_add(1, Ordering::SeqCst);
            if self.scenario == Scenario::SessionShutdownFailure {
                Err(ControlError::shutdown_failed())
            } else {
                Ok(())
            }
        })
    }
}

struct MatrixHarness {
    router: Router,
    counters: Arc<LifecycleCounters>,
    sink: Arc<MemoryAuditSink>,
}

async fn matrix_harness(scenario: Scenario) -> MatrixHarness {
    let counters = Arc::new(LifecycleCounters::default());
    let sink = Arc::new(if scenario == Scenario::AuditWriteFailure {
        MemoryAuditSink::failing(8, 4096)
    } else {
        MemoryAuditSink::new(8, 4096)
    });
    let router = matrix_router(
        Arc::new(FakeMutationAdminFactory {
            scenario,
            counters: Arc::clone(&counters),
        }),
        sink.clone(),
    )
    .await;
    MatrixHarness {
        router,
        counters,
        sink,
    }
}

async fn matrix_router(
    factory: Arc<dyn MutationToolSessionFactory>,
    sink: Arc<dyn ReliableAuditSink>,
) -> Router {
    let mut transport_config = config();
    let policy = MutationPolicyConfig {
        mutations_enabled: true,
        dry_run: true,
        allowed_operations: ToolKind::ALL.into_iter().map(ToolKind::operation).collect(),
        allowed_clusters: vec![ClusterName::try_new("cluster-a").unwrap()],
        operation_timeout_seconds: 2,
        require_plan_digest: false,
        emergency_stop_file: None,
    };
    transport_config.mutations = policy.clone();
    let runtime = rocketmq_runtime::RuntimeContext::from_current("control-acceptance-matrix");
    let owner = runtime
        .service_context("control-acceptance-matrix")
        .task_group()
        .clone();
    let server = ControlServer::with_test_factory(
        &policy,
        BTreeSet::from([ClusterName::try_new("cluster-a").unwrap()]),
        AuditTrail::new(sink),
        factory,
        owner,
    )
    .with_proxy_aliases(ClusterName::try_new("cluster-a").unwrap(), &["proxy-a"]);
    let auth = AuthState::from_source(
        &transport_config.oauth,
        resource_metadata_url(&transport_config),
        StaticSource,
    )
    .await
    .unwrap();
    build_router_with_auth(&transport_config, server, CancellationToken::new(), auth)
}

fn arguments(tool: ToolKind, scenario: Scenario) -> serde_json::Value {
    let multi = scenario == Scenario::MultiTargetPartial;
    let brokers = if multi {
        serde_json::json!(["broker-a", "broker-b"])
    } else {
        serde_json::json!(["broker-a"])
    };
    let mut value = match tool {
        ToolKind::Topic => serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "topic": "orders",
            "broker_names": brokers,
            "read_queue_nums": 8,
            "write_queue_nums": 8,
            "perm": 6,
            "order": false,
            "message_type": "NORMAL"
        }),
        ToolKind::ConsumerGroup => serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "consumer_group": "orders_consumers",
            "broker_names": brokers,
            "consume_enable": true,
            "consume_from_min_enable": false,
            "consume_broadcast_enable": false,
            "consume_message_orderly": false,
            "retry_queue_nums": 1,
            "retry_max_times": 16,
            "broker_id": 0,
            "which_broker_when_consume_slowly": 1,
            "notify_consumer_ids_changed_enable": true,
            "group_sys_flag": 0,
            "consume_timeout_minute": 15
        }),
        ToolKind::Offset => serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "topic": "orders",
            "consumer_group": "orders_consumers",
            "timestamp": "2026-08-30T00:00:00Z",
            "force": false
        }),
        ToolKind::BrokerConfig => serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "broker_name": "broker-a",
            "properties": {"traceTopicEnable": "true"}
        }),
        ToolKind::RequestMode => serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "topic": "orders",
            "consumer_group": "orders_consumers",
            "mode": "pop",
            "pop_share_queue_num": 4,
            "timeout_millis": 12000
        }),
        ToolKind::DrainBegin | ToolKind::DrainCancel => serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "proxy_name": "proxy-a",
            "operation_id": OPERATION_ID
        }),
        ToolKind::LogFilterSet => serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "broker_name": "broker-a",
            "logger": "rocketmq_broker::processor",
            "level": "DEBUG",
            "ttl_seconds": 300,
            "operation_id": OPERATION_ID
        }),
        ToolKind::LogFilterRestore => serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "broker_name": "broker-a",
            "operation_id": OPERATION_ID
        }),
    };
    let object = value.as_object_mut().unwrap();
    let dry_run = scenario == Scenario::ValidDryRun;
    object.insert("dry_run".to_owned(), serde_json::json!(dry_run));
    object.insert("confirm".to_owned(), serde_json::json!(!dry_run));
    object.insert("reason".to_owned(), serde_json::json!(SAFE_REASON));
    object.insert(
        "request_key".to_owned(),
        serde_json::json!(format!("matrix-{}-{}", tool.slug(), scenario.slug())),
    );
    match scenario {
        Scenario::ExecuteWithoutConfirm => {
            object.insert("confirm".to_owned(), serde_json::json!(false));
        }
        Scenario::ExecuteWithoutReason => {
            object.remove("reason");
        }
        Scenario::ArgumentOutOfBounds => match tool {
            ToolKind::Topic => {
                object.insert("read_queue_nums".to_owned(), serde_json::json!(0));
            }
            ToolKind::ConsumerGroup => {
                object.insert("retry_queue_nums".to_owned(), serde_json::json!(128));
            }
            ToolKind::Offset => {
                object.insert("timestamp".to_owned(), serde_json::json!("1969-12-31T23:59:59Z"));
            }
            ToolKind::BrokerConfig => {
                object.insert("properties".to_owned(), serde_json::json!({}));
            }
            ToolKind::RequestMode => {
                object.insert("timeout_millis".to_owned(), serde_json::json!(24_001));
            }
            ToolKind::DrainBegin | ToolKind::DrainCancel | ToolKind::LogFilterRestore => {
                object.insert("operation_id".to_owned(), serde_json::json!("short"));
            }
            ToolKind::LogFilterSet => {
                object.insert("ttl_seconds".to_owned(), serde_json::json!(901));
            }
        },
        Scenario::ValidDryRun
        | Scenario::ValidExecute
        | Scenario::MissingWriteScope
        | Scenario::ClusterNotAllowed
        | Scenario::OperationNotAllowed
        | Scenario::CasConflict
        | Scenario::SingleTargetFailure
        | Scenario::MultiTargetPartial
        | Scenario::PostReadFailure
        | Scenario::SessionShutdownFailure
        | Scenario::AuditWriteFailure
        | Scenario::SensitiveRuntimeFailure => {}
    }
    value
}

fn fake_runtime_status(scenario: Scenario) -> tools::MutationStatus {
    match scenario {
        Scenario::ValidDryRun => tools::MutationStatus::Planned,
        Scenario::ValidExecute | Scenario::SessionShutdownFailure => tools::MutationStatus::Applied,
        Scenario::CasConflict => tools::MutationStatus::Conflict,
        Scenario::SingleTargetFailure | Scenario::PostReadFailure => tools::MutationStatus::Failed,
        Scenario::MultiTargetPartial => tools::MutationStatus::Partial,
        Scenario::ExecuteWithoutConfirm
        | Scenario::ExecuteWithoutReason
        | Scenario::MissingWriteScope
        | Scenario::ClusterNotAllowed
        | Scenario::OperationNotAllowed
        | Scenario::ArgumentOutOfBounds
        | Scenario::AuditWriteFailure
        | Scenario::SensitiveRuntimeFailure => unreachable!("scenario has no typed fake response"),
    }
}

fn response_for(request: MutationToolRequest, scenario: Scenario) -> MutationToolResponse {
    match request {
        MutationToolRequest::Topic(args) => MutationToolResponse::Topic(upsert_response(
            &args.cluster,
            args.topic.clone(),
            args.broker_names,
            args.replacement,
            scenario,
            tools::TopicUpsertOperation::TopicUpsert,
        )),
        MutationToolRequest::ConsumerGroup(args) => MutationToolResponse::ConsumerGroup(upsert_response(
            &args.cluster,
            args.consumer_group.clone(),
            args.broker_names,
            args.replacement,
            scenario,
            tools::ConsumerGroupUpsertOperation::ConsumerGroupUpsert,
        )),
        MutationToolRequest::ConsumerOffset(args) => {
            MutationToolResponse::ConsumerOffset(offset_response(&args, scenario))
        }
        MutationToolRequest::BrokerConfig(args) => MutationToolResponse::BrokerConfig(broker_response(&args, scenario)),
        MutationToolRequest::ConsumerRequestMode(args) => {
            MutationToolResponse::ConsumerRequestMode(request_mode_response(&args, scenario))
        }
        MutationToolRequest::ProxyDrainBegin(args) => MutationToolResponse::ProxyDrain(proxy_drain_response(
            tools::ProxyDrainOperation::ProxyDrainBegin,
            &args,
            scenario,
        )),
        MutationToolRequest::ProxyDrainCancel(args) => MutationToolResponse::ProxyDrain(proxy_drain_response(
            tools::ProxyDrainOperation::ProxyDrainCancel,
            &args,
            scenario,
        )),
        MutationToolRequest::BrokerLogFilterSet(args) => MutationToolResponse::BrokerLogFilter(log_filter_response(
            tools::BrokerLogFilterOperation::BrokerLogFilterSet,
            &args.cluster,
            &args.broker_name,
            tools::BrokerLogFilterRequested::Set {
                logger: args.logger.clone(),
                level: args.level,
                ttl_seconds: args.ttl_seconds,
            },
            scenario,
        )),
        MutationToolRequest::BrokerLogFilterRestore(args) => {
            MutationToolResponse::BrokerLogFilter(log_filter_response(
                tools::BrokerLogFilterOperation::BrokerLogFilterRestore,
                &args.cluster,
                &args.broker_name,
                tools::BrokerLogFilterRequested::Restore,
                scenario,
            ))
        }
    }
}

trait UpsertOperation: Copy {
    type Resource;

    fn resource(self, name: String, brokers: Vec<String>) -> Self::Resource;
}

impl UpsertOperation for tools::TopicUpsertOperation {
    type Resource = tools::TopicMutationResource;

    fn resource(self, name: String, brokers: Vec<String>) -> Self::Resource {
        tools::TopicMutationResource { topic: name, brokers }
    }
}

impl UpsertOperation for tools::ConsumerGroupUpsertOperation {
    type Resource = tools::ConsumerGroupMutationResource;

    fn resource(self, name: String, brokers: Vec<String>) -> Self::Resource {
        tools::ConsumerGroupMutationResource {
            consumer_group: name,
            brokers,
        }
    }
}

fn upsert_response<T, O>(
    cluster: &str,
    resource_name: String,
    brokers: Vec<String>,
    replacement: T,
    scenario: Scenario,
    operation: O,
) -> tools::MutationToolResponse<O, O::Resource, T>
where
    T: Clone,
    O: UpsertOperation,
{
    let status = fake_runtime_status(scenario);
    let before = brokers
        .iter()
        .cloned()
        .map(|broker| (broker, tools::VisibleState::Absent))
        .collect::<BTreeMap<_, _>>();
    let targets = brokers
        .iter()
        .enumerate()
        .map(|(index, broker)| {
            let truth = target_truth(scenario, index);
            tools::MutationTarget {
                target: tools::LogicalMutationTarget {
                    broker_name: broker.clone(),
                },
                before: tools::VisibleState::Absent,
                requested: replacement.clone(),
                after: truth.after.then(|| tools::VisibleState::Present {
                    version: 1,
                    value: replacement.clone(),
                }),
                applied: truth.applied,
                changed: truth.applied,
                persistence: truth.persistence,
                verification: truth.verification,
                failure: truth.failure,
                retryable: truth.retryable,
            }
        })
        .collect::<Vec<_>>();
    let after = targets.iter().any(|target| target.after.is_some()).then(|| {
        targets
            .iter()
            .filter_map(|target| {
                target
                    .after
                    .clone()
                    .map(|state| (target.target.broker_name.clone(), state))
            })
            .collect()
    });
    let error_code = tools::response_error_code(status, targets.iter().map(|target| target.failure));
    tools::MutationToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation,
        cluster: cluster.to_owned(),
        mode: scenario_mode(scenario),
        status,
        error_code,
        target: operation.resource(resource_name, brokers),
        before,
        requested: replacement,
        after,
        targets,
        warnings: Vec::new(),
    }
}

#[derive(Clone, Copy)]
struct TargetTruth {
    applied: bool,
    after: bool,
    persistence: tools::PersistenceState,
    verification: tools::VerificationState,
    failure: Option<tools::FailureCode>,
    retryable: bool,
}

fn target_truth(scenario: Scenario, index: usize) -> TargetTruth {
    let success = TargetTruth {
        applied: true,
        after: true,
        persistence: tools::PersistenceState::Persisted,
        verification: tools::VerificationState::Verified,
        failure: None,
        retryable: false,
    };
    match scenario {
        Scenario::ValidDryRun => TargetTruth {
            applied: false,
            after: false,
            persistence: tools::PersistenceState::NotRequired,
            verification: tools::VerificationState::NotPerformed,
            failure: None,
            retryable: false,
        },
        Scenario::ValidExecute | Scenario::SessionShutdownFailure => success,
        Scenario::CasConflict => TargetTruth {
            applied: false,
            after: false,
            persistence: tools::PersistenceState::NotRequired,
            verification: tools::VerificationState::NotPerformed,
            failure: Some(tools::FailureCode::Conflict),
            retryable: false,
        },
        Scenario::SingleTargetFailure => TargetTruth {
            applied: false,
            after: false,
            persistence: tools::PersistenceState::NotRequired,
            verification: tools::VerificationState::NotPerformed,
            failure: Some(tools::FailureCode::Unavailable),
            retryable: true,
        },
        Scenario::MultiTargetPartial if index == 0 => success,
        Scenario::MultiTargetPartial => TargetTruth {
            applied: false,
            after: false,
            persistence: tools::PersistenceState::NotRequired,
            verification: tools::VerificationState::NotPerformed,
            failure: Some(tools::FailureCode::Unavailable),
            retryable: true,
        },
        Scenario::PostReadFailure => TargetTruth {
            applied: true,
            after: false,
            persistence: tools::PersistenceState::Persisted,
            verification: tools::VerificationState::Failed,
            failure: Some(tools::FailureCode::VerificationFailed),
            retryable: true,
        },
        Scenario::ExecuteWithoutConfirm
        | Scenario::ExecuteWithoutReason
        | Scenario::MissingWriteScope
        | Scenario::ClusterNotAllowed
        | Scenario::OperationNotAllowed
        | Scenario::ArgumentOutOfBounds
        | Scenario::AuditWriteFailure
        | Scenario::SensitiveRuntimeFailure => unreachable!("scenario cannot produce target truth"),
    }
}

fn offset_response(args: &tools::ResetConsumerOffsetArgs, scenario: Scenario) -> tools::OffsetMutationToolResponse {
    let status = fake_runtime_status(scenario);
    let brokers = target_brokers(scenario);
    let targets = brokers
        .iter()
        .enumerate()
        .map(|(index, broker)| {
            let truth = target_truth(scenario, index);
            tools::OffsetMutationTarget {
                broker_name: broker.clone(),
                queue_id: Some(index as i32),
                before: Some(9),
                planned: Some(4),
                delta: Some(-5),
                after: truth.after.then_some(4),
                applied: truth.applied,
                changed: truth.applied,
                failure: truth.failure,
                retryable: truth.retryable,
            }
        })
        .collect::<Vec<_>>();
    let before = targets
        .iter()
        .map(|target| tools::OffsetQueueState {
            broker_name: target.broker_name.clone(),
            queue_id: target.queue_id.unwrap(),
            offset: target.before.unwrap(),
        })
        .collect();
    let after = targets.iter().any(|target| target.after.is_some()).then(|| {
        targets
            .iter()
            .filter_map(|target| {
                Some(tools::OffsetQueueState {
                    broker_name: target.broker_name.clone(),
                    queue_id: target.queue_id?,
                    offset: target.after?,
                })
            })
            .collect()
    });
    tools::OffsetMutationToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation: tools::ConsumerOffsetResetOperation::ConsumerOffsetReset,
        cluster: args.cluster.clone(),
        mode: scenario_mode(scenario),
        status,
        error_code: tools::response_error_code(status, targets.iter().map(|target| target.failure)),
        target: tools::OffsetResetResource {
            topic: args.topic.clone(),
            consumer_group: args.consumer_group.clone(),
            brokers,
        },
        before,
        requested: tools::OffsetRequested {
            timestamp: args.timestamp.clone(),
            timestamp_millis: 1_788_048_000_000,
            force: args.force,
        },
        after,
        targets,
        warnings: Vec::new(),
    }
}

fn broker_response(args: &tools::PatchBrokerConfigArgs, scenario: Scenario) -> tools::BrokerConfigMutationToolResponse {
    let status = fake_runtime_status(scenario);
    let truth = target_truth(scenario, 0);
    let before_state = broker_state(false);
    let after_state = broker_state(true);
    let patch = tools::BrokerConfigPatch {
        trace_topic_enable: Some(true),
        ..tools::BrokerConfigPatch::default()
    };
    let target = tools::BrokerConfigMutationTarget {
        broker_name: args.broker_name.clone(),
        before: Some(before_state),
        requested: patch,
        after: truth.after.then_some(after_state),
        applied: truth.applied,
        changed: truth.applied,
        persistence: truth.persistence,
        verification: truth.verification,
        failure: truth.failure,
        retryable: truth.retryable,
    };
    tools::BrokerConfigMutationToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation: tools::BrokerConfigPatchOperation::BrokerConfigPatch,
        cluster: args.cluster.clone(),
        mode: scenario_mode(scenario),
        status,
        error_code: tools::response_error_code(status, [target.failure]),
        target: tools::BrokerConfigResource {
            broker_name: args.broker_name.clone(),
        },
        before: BTreeMap::from([(args.broker_name.clone(), before_state)]),
        requested: patch,
        after: truth
            .after
            .then(|| BTreeMap::from([(args.broker_name.clone(), after_state)])),
        targets: vec![target],
        warnings: Vec::new(),
    }
}

fn request_mode_response(
    args: &tools::SetConsumerRequestModeArgs,
    scenario: Scenario,
) -> tools::RequestModeMutationToolResponse {
    let status = fake_runtime_status(scenario);
    let brokers = target_brokers(scenario);
    let before_value = tools::RequestModeValue {
        mode: tools::ConsumerRequestMode::Pull,
        pop_share_queue_num: 0,
    };
    let requested = tools::RequestModeValue {
        mode: args.mode,
        pop_share_queue_num: args.pop_share_queue_num,
    };
    let targets = brokers
        .iter()
        .enumerate()
        .map(|(index, broker)| {
            let truth = target_truth(scenario, index);
            tools::RequestModeMutationTarget {
                broker_name: broker.clone(),
                before: Some(before_value),
                requested,
                after: truth.after.then_some(requested),
                applied: truth.applied,
                changed: truth.applied,
                persistence: truth.persistence,
                verification: truth.verification,
                failure: truth.failure,
                retryable: truth.retryable,
            }
        })
        .collect::<Vec<_>>();
    let before = brokers
        .iter()
        .cloned()
        .map(|broker| (broker, Some(before_value)))
        .collect();
    let after = targets.iter().any(|target| target.after.is_some()).then(|| {
        targets
            .iter()
            .map(|target| (target.broker_name.clone(), target.after))
            .collect()
    });
    tools::RequestModeMutationToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation: tools::ConsumerRequestModeOperation::ConsumerRequestMode,
        cluster: args.cluster.clone(),
        mode: scenario_mode(scenario),
        status,
        error_code: tools::response_error_code(status, targets.iter().map(|target| target.failure)),
        target: tools::RequestModeResource {
            topic: args.topic.clone(),
            consumer_group: args.consumer_group.clone(),
            brokers,
        },
        before,
        requested: tools::RequestModeRequested {
            mode: args.mode,
            pop_share_queue_num: args.pop_share_queue_num,
            timeout_millis: args.timeout_millis,
        },
        after,
        targets,
        warnings: Vec::new(),
    }
}

/// Who holds the target before a call: nobody before a begin or a set, the call's own operation
/// before a cancel or a restore, and another operation when the call ends in a conflict.
const fn holder_before(starts: bool, scenario: Scenario) -> tools::OperationHolder {
    if matches!(scenario, Scenario::CasConflict) {
        tools::OperationHolder::OtherOperation
    } else if starts {
        tools::OperationHolder::None
    } else {
        tools::OperationHolder::ThisOperation
    }
}

/// Who holds the target once a call has done what it asked.
const fn holder_after(starts: bool) -> tools::OperationHolder {
    if starts {
        tools::OperationHolder::ThisOperation
    } else {
        tools::OperationHolder::None
    }
}

/// A begin finds the Proxy accepting and leaves it draining; a cancel does the reverse.
fn proxy_drain_response(
    operation: tools::ProxyDrainOperation,
    args: &tools::ProxyDrainArgs,
    scenario: Scenario,
) -> tools::ProxyDrainToolResponse {
    let status = fake_runtime_status(scenario);
    let truth = target_truth(scenario, 0);
    let begin = operation == tools::ProxyDrainOperation::ProxyDrainBegin;
    let requested = tools::ProxyDrainRequested {
        action: if begin {
            tools::ProxyDrainAction::Begin
        } else {
            tools::ProxyDrainAction::Cancel
        },
    };
    let before_state = drain_state(holder_before(begin, scenario));
    let after_state = drain_state(holder_after(begin));
    let target = tools::ProxyDrainTarget {
        proxy_name: args.proxy_name.clone(),
        before: Some(before_state),
        requested,
        after: truth.after.then_some(after_state),
        applied: truth.applied,
        changed: truth.applied,
        verification: truth.verification,
        failure: truth.failure,
        retryable: truth.retryable,
    };
    tools::ProxyDrainToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation,
        cluster: args.cluster.clone(),
        mode: scenario_mode(scenario),
        status,
        error_code: tools::response_error_code(status, [target.failure]),
        target: tools::ProxyDrainResource {
            proxy_name: args.proxy_name.clone(),
        },
        before: BTreeMap::from([(args.proxy_name.clone(), before_state)]),
        requested,
        after: truth
            .after
            .then(|| BTreeMap::from([(args.proxy_name.clone(), after_state)])),
        targets: vec![target],
        warnings: Vec::new(),
    }
}

const fn drain_state(held_by: tools::OperationHolder) -> tools::ProxyDrainState {
    let draining = !matches!(held_by, tools::OperationHolder::None);
    tools::ProxyDrainState {
        phase: if draining {
            tools::ProxyDrainPhase::Draining
        } else {
            tools::ProxyDrainPhase::Accepting
        },
        held_by,
        admission_open: !draining,
        routing_open: !draining,
        readiness_published: !draining,
        zero_pending: !draining,
        pending: tools::ProxyDrainPending {
            active_connections: 0,
            sessions: if draining { 3 } else { 0 },
            receipt_handles: 0,
            prepared_transactions: 0,
            telemetry_links: 0,
            remoting_channels: 0,
            telemetry_commands: 0,
            rpc_in_flight: 0,
        },
    }
}

/// A set finds no override and leaves its own; a restore does the reverse and names no logger.
fn log_filter_response(
    operation: tools::BrokerLogFilterOperation,
    cluster: &str,
    broker_name: &str,
    requested: tools::BrokerLogFilterRequested,
    scenario: Scenario,
) -> tools::BrokerLogFilterToolResponse {
    let status = fake_runtime_status(scenario);
    let truth = target_truth(scenario, 0);
    let set = operation == tools::BrokerLogFilterOperation::BrokerLogFilterSet;
    let state = |held_by: tools::OperationHolder| {
        let overridden = held_by != tools::OperationHolder::None;
        tools::BrokerLogFilterState {
            level: set.then_some(if overridden {
                tools::BrokerLogLevel::Debug
            } else {
                tools::BrokerLogLevel::Info
            }),
            held_by,
            expires_at_millis: overridden.then_some(1_788_048_300_000),
        }
    };
    let before_state = state(holder_before(set, scenario));
    let after_state = state(holder_after(set));
    let target = tools::BrokerLogFilterTarget {
        broker_name: broker_name.to_owned(),
        before: Some(before_state),
        requested: requested.clone(),
        after: truth.after.then_some(after_state),
        applied: truth.applied,
        changed: truth.applied,
        verification: truth.verification,
        failure: truth.failure,
        retryable: truth.retryable,
    };
    tools::BrokerLogFilterToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation,
        cluster: cluster.to_owned(),
        mode: scenario_mode(scenario),
        status,
        error_code: tools::response_error_code(status, [target.failure]),
        target: tools::BrokerLogFilterResource {
            broker_name: broker_name.to_owned(),
        },
        before: BTreeMap::from([(broker_name.to_owned(), before_state)]),
        requested,
        after: truth
            .after
            .then(|| BTreeMap::from([(broker_name.to_owned(), after_state)])),
        targets: vec![target],
        warnings: Vec::new(),
    }
}

const fn scenario_mode(scenario: Scenario) -> tools::MutationMode {
    if matches!(scenario, Scenario::ValidDryRun) {
        tools::MutationMode::DryRun
    } else {
        tools::MutationMode::Execute
    }
}

fn target_brokers(scenario: Scenario) -> Vec<String> {
    if scenario == Scenario::MultiTargetPartial {
        vec!["broker-a".to_owned(), "broker-b".to_owned()]
    } else {
        vec!["broker-a".to_owned()]
    }
}

const fn broker_state(trace_topic_enable: bool) -> tools::BrokerConfigState {
    tools::BrokerConfigState {
        generation: 7,
        auto_create_topic_enable: true,
        auto_create_subscription_group: true,
        broker_permission: 6,
        default_topic_queue_nums: 8,
        message_index_enable: true,
        trace_topic_enable,
    }
}

fn token_for(tool: ToolKind, scenario: Scenario) -> String {
    match scenario {
        Scenario::MissingWriteScope => {
            token_with_subject_claims(SAFE_OPERATOR, "rocketmq:read", vec![tool.claim()], vec!["cluster-a"])
        }
        Scenario::ClusterNotAllowed => token_with_subject_claims(
            SAFE_OPERATOR,
            REQUIRED_WRITE_SCOPE,
            vec![tool.claim()],
            vec!["cluster-b"],
        ),
        Scenario::OperationNotAllowed => {
            token_with_subject_claims(SAFE_OPERATOR, REQUIRED_WRITE_SCOPE, Vec::new(), vec!["cluster-a"])
        }
        _ => token_with_subject_claims(
            SAFE_OPERATOR,
            REQUIRED_WRITE_SCOPE,
            vec![tool.claim()],
            vec!["cluster-a"],
        ),
    }
}

#[derive(Debug)]
struct ObservedCall {
    status: StatusCode,
    request_id: usize,
    body: serde_json::Value,
}

async fn acceptance_tool_call(
    router: &Router,
    token: &str,
    id: usize,
    tool: &str,
    arguments: serde_json::Value,
) -> ObservedCall {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": {"name": tool, "arguments": arguments}
    });
    let response = router
        .clone()
        .oneshot(request("/mcp", Body::from(body.to_string()), Some(token)))
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
    ObservedCall {
        status,
        request_id: id,
        body: serde_json::from_slice(&bytes).unwrap(),
    }
}

fn expected_error_envelope(scenario: Scenario) -> serde_json::Value {
    match scenario {
        Scenario::ExecuteWithoutConfirm => serde_json::json!({
            "schema_version": ERROR_SCHEMA_VERSION,
            "code": "confirmation_required",
            "message": "explicit mutation confirmation is required",
            "retryable": false
        }),
        Scenario::ExecuteWithoutReason | Scenario::ArgumentOutOfBounds => serde_json::json!({
            "schema_version": ERROR_SCHEMA_VERSION,
            "code": "invalid_argument",
            "message": "mutation argument is invalid",
            "retryable": false
        }),
        Scenario::MissingWriteScope => serde_json::json!({
            "schema_version": ERROR_SCHEMA_VERSION,
            "code": "permission_denied",
            "message": "write permission is required",
            "retryable": false
        }),
        Scenario::ClusterNotAllowed => serde_json::json!({
            "schema_version": ERROR_SCHEMA_VERSION,
            "code": "cluster_not_allowed",
            "message": "mutation cluster is not allowed",
            "retryable": false
        }),
        Scenario::OperationNotAllowed => serde_json::json!({
            "schema_version": ERROR_SCHEMA_VERSION,
            "code": "operation_not_allowed",
            "message": "mutation operation is not allowed",
            "retryable": false
        }),
        Scenario::SessionShutdownFailure => serde_json::json!({
            "schema_version": ERROR_SCHEMA_VERSION,
            "code": "shutdown_failed",
            "message": "mutation session shutdown failed",
            "retryable": true
        }),
        Scenario::AuditWriteFailure => serde_json::json!({
            "schema_version": ERROR_SCHEMA_VERSION,
            "code": "audit_unavailable",
            "message": "reliable audit storage is unavailable",
            "retryable": true
        }),
        Scenario::SensitiveRuntimeFailure => serde_json::json!({
            "schema_version": ERROR_SCHEMA_VERSION,
            "code": "execution_failed",
            "message": "mutation execution failed",
            "retryable": false
        }),
        Scenario::ValidDryRun
        | Scenario::ValidExecute
        | Scenario::CasConflict
        | Scenario::SingleTargetFailure
        | Scenario::MultiTargetPartial
        | Scenario::PostReadFailure => unreachable!("typed response is not an error envelope"),
    }
}

fn expected_adapter_error_envelope(code: ControlErrorCode) -> serde_json::Value {
    let (code, message, retryable) = match code {
        ControlErrorCode::InvalidConfig => ("invalid_config", "control configuration is invalid", false),
        ControlErrorCode::RequestRejected => ("request_rejected", "request was rejected", false),
        ControlErrorCode::Unauthorized => ("unauthorized", "authentication is required", false),
        ControlErrorCode::PermissionDenied => ("permission_denied", "write permission is required", false),
        ControlErrorCode::ClusterNotAllowed => (
            "cluster_not_allowed",
            "mutation cluster is not allowed",
            false,
        ),
        ControlErrorCode::OperationNotAllowed => (
            "operation_not_allowed",
            "mutation operation is not allowed",
            false,
        ),
        ControlErrorCode::MutationDisabled => (
            "mutation_disabled",
            "mutation execution is disabled",
            false,
        ),
        ControlErrorCode::OperationUnavailable => (
            "operation_unavailable",
            "mutation operation is unavailable",
            false,
        ),
        ControlErrorCode::RateLimited => ("rate_limited", "mutation call rate limit was reached", true),
        ControlErrorCode::ConfirmationRequired => (
            "confirmation_required",
            "explicit mutation confirmation is required",
            false,
        ),
        ControlErrorCode::InvalidArgument => ("invalid_argument", "mutation argument is invalid", false),
        ControlErrorCode::AuditUnavailable => (
            "audit_unavailable",
            "reliable audit storage is unavailable",
            true,
        ),
        ControlErrorCode::PreconditionConflict => (
            "precondition_conflict",
            "mutation precondition conflict",
            false,
        ),
        ControlErrorCode::PartialApply => (
            "partial_apply",
            "mutation applied to only part of the target set",
            false,
        ),
        ControlErrorCode::VerificationFailed => (
            "verification_failed",
            "mutation result could not be verified",
            true,
        ),
        ControlErrorCode::Timeout => ("timeout", "mutation timed out", true),
        ControlErrorCode::Cancelled => ("cancelled", "mutation was cancelled", true),
        ControlErrorCode::ExecutionFailed => ("execution_failed", "mutation execution failed", false),
        ControlErrorCode::ShutdownFailed => (
            "shutdown_failed",
            "mutation session shutdown failed",
            true,
        ),
    };
    serde_json::json!({
        "schema_version": ERROR_SCHEMA_VERSION,
        "code": code,
        "message": message,
        "retryable": retryable
    })
}

/// The arguments an `invalid_argument` scenario must locate, as path and constraint pairs.
///
/// The pre-epoch timestamp, the empty patch and the missing reason break rules that the input
/// schemas do not declare, so those rejections locate nothing.
fn expected_violations(tool: ToolKind, scenario: Scenario) -> &'static [(&'static str, &'static str)] {
    match (scenario, tool) {
        (Scenario::ArgumentOutOfBounds, ToolKind::Topic) => &[("/read_queue_nums", "minimum")],
        (Scenario::ArgumentOutOfBounds, ToolKind::ConsumerGroup) => &[("/retry_queue_nums", "maximum")],
        (Scenario::ArgumentOutOfBounds, ToolKind::RequestMode) => &[("/timeout_millis", "maximum")],
        (
            Scenario::ArgumentOutOfBounds,
            ToolKind::DrainBegin | ToolKind::DrainCancel | ToolKind::LogFilterRestore,
        ) => &[("/operation_id", "min_length")],
        (Scenario::ArgumentOutOfBounds, ToolKind::LogFilterSet) => &[("/ttl_seconds", "maximum")],
        _ => &[],
    }
}

fn located_error_envelope(scenario: Scenario, violations: &[(&str, &str)]) -> serde_json::Value {
    let mut envelope = expected_error_envelope(scenario);
    if !violations.is_empty() {
        envelope["violations"] = violations
            .iter()
            .map(|(path, constraint)| serde_json::json!({"path": path, "constraint": constraint}))
            .collect();
    }
    envelope
}

fn expected_adapter_retryable(code: ControlErrorCode) -> bool {
    expected_adapter_error_envelope(code)["retryable"].as_bool().unwrap()
}

fn mcp_payload(call: &ObservedCall, is_error: bool) -> &serde_json::Value {
    assert_eq!(call.status, StatusCode::OK);
    assert_eq!(call.body["jsonrpc"], "2.0");
    assert_eq!(call.body["id"], call.request_id);
    assert!(call.body.get("error").is_none());
    let result = call.body.get("result").and_then(serde_json::Value::as_object).unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result.get("isError").and_then(serde_json::Value::as_bool), Some(is_error));
    let structured = result.get("structuredContent").expect("structuredContent is mandatory");
    let content = result.get("content").and_then(serde_json::Value::as_array).unwrap();
    assert_eq!(content.len(), 1);
    assert_eq!(content[0]["type"], "text");
    let text_payload: serde_json::Value = serde_json::from_str(content[0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(&text_payload, structured);
    structured
}

fn strict_payload(call: &ObservedCall, scenario: Scenario) -> &serde_json::Value {
    if scenario == Scenario::MissingWriteScope {
        assert_eq!(call.status, StatusCode::FORBIDDEN);
        assert!(call.body.get("jsonrpc").is_none());
        assert!(call.body.get("id").is_none());
        assert!(call.body.get("result").is_none());
        assert!(call.body.get("isError").is_none());
        assert_eq!(call.body, expected_error_envelope(scenario));
        &call.body
    } else {
        mcp_payload(call, scenario.expected_code().is_some())
    }
}

fn assert_public_contract(
    tool: ToolKind,
    scenario: Scenario,
    violations: &[(&str, &str)],
    first: &ObservedCall,
    second: &ObservedCall,
    bearer: &str,
) -> serde_json::Value {
    let first_payload = strict_payload(first, scenario);
    let second_payload = strict_payload(second, scenario);
    assert_same_response_under_own_invocations(first_payload, second_payload, &format!("{tool:?}/{scenario:?}"));
    let expected_code = scenario.expected_code().map(ControlErrorCode::as_str);
    match scenario.expected_status() {
        Some(status) if scenario != Scenario::SessionShutdownFailure => {
            assert_eq!(
                first_payload["status"],
                serde_json::to_value(status).unwrap(),
                "status mismatch for {tool:?}/{scenario:?}"
            );
            assert_eq!(
                first_payload["error_code"].as_str(),
                expected_code,
                "code mismatch for {tool:?}/{scenario:?}"
            );
            assert_response_truth(first_payload, scenario);
        }
        _ => {
            assert_eq!(
                first_payload,
                &located_error_envelope(scenario, violations),
                "envelope mismatch for {tool:?}/{scenario:?}"
            );
            assert!(first_payload.get("status").is_none());
        }
    }
    let public_text = format!("{}{}", first.body, second.body);
    for forbidden in [
        SAFE_OPERATOR,
        SAFE_REASON,
        bearer,
        BACKEND_CREDENTIAL,
        BACKEND_ENDPOINT,
        RAW_BACKEND_ERROR,
        MESSAGE_BODY,
        CLIENT_IDENTITY,
        OPERATION_ID,
        "127.0.0.1",
    ] {
        assert!(
            !public_text.contains(forbidden),
            "public response leaked protected material for {tool:?}/{scenario:?}"
        );
    }
    if scenario == Scenario::ClusterNotAllowed {
        for logical_target in tool.logical_targets() {
            assert!(
                !public_text.contains(logical_target),
                "cluster denial leaked logical target {logical_target} for {tool:?}"
            );
        }
    }
    first_payload.clone()
}

/// Splits a payload into the invocation it was returned under and everything else.
fn split_invocation(payload: &serde_json::Value) -> (Option<u64>, serde_json::Value) {
    let mut rest = payload.clone();
    let invocation = rest
        .as_object_mut()
        .and_then(|members| members.remove(tools::INVOCATION_ID_MEMBER))
        .map(|invocation| invocation.as_u64().expect("an invocation id is a positive integer"));
    (invocation, rest)
}

/// Asserts that two calls returned the same response, each under an invocation of its own.
///
/// A mutation response carries the invocation of the call that returns it, replayed or not. An
/// error envelope carries none.
fn assert_same_response_under_own_invocations(first: &serde_json::Value, second: &serde_json::Value, case: &str) {
    let (first_invocation, first_rest) = split_invocation(first);
    let (second_invocation, second_rest) = split_invocation(second);
    assert_eq!(first_rest, second_rest, "replay mismatch for {case}");
    match (first_invocation, second_invocation) {
        (Some(first), Some(second)) => {
            assert!(first >= 1 && second >= 1, "invocation ids start at one for {case}");
            assert_ne!(first, second, "two calls shared one invocation for {case}");
        }
        (None, None) => assert!(first.get("status").is_none(), "a mutation response lacks its invocation for {case}"),
        _ => panic!("only one of two equal responses carries an invocation for {case}"),
    }
}

fn assert_response_truth(response: &serde_json::Value, scenario: Scenario) {
    let targets = response["targets"].as_array().unwrap();
    assert!(!targets.is_empty());
    assert!(!response["before"].is_null());
    assert!(response["requested"].is_object());
    assert!(response["target"].is_object());
    match scenario {
        Scenario::ValidDryRun => {
            assert_eq!(response["mode"], "dry_run");
            assert!(response["after"].is_null());
            assert!(targets.iter().all(|target| {
                target["applied"] == false && target["changed"] == false && target["failure"].is_null()
            }));
        }
        Scenario::ValidExecute => {
            assert_eq!(response["mode"], "execute");
            assert!(!response["after"].is_null());
            assert!(targets
                .iter()
                .all(|target| target["applied"] == true && target["failure"].is_null()));
        }
        Scenario::CasConflict => {
            assert!(response["after"].is_null());
            assert!(targets
                .iter()
                .all(|target| target["applied"] == false && target["failure"] == "conflict"));
        }
        Scenario::SingleTargetFailure => {
            assert_eq!(targets.len(), 1);
            assert_eq!(targets[0]["applied"], false);
            assert_eq!(targets[0]["failure"], "unavailable");
        }
        Scenario::MultiTargetPartial => {
            assert_eq!(targets.len(), 2);
            assert!(targets.iter().any(|target| target["applied"] == true));
            assert!(targets.iter().any(|target| target["failure"] == "unavailable"));
        }
        Scenario::PostReadFailure => {
            assert!(response["after"].is_null());
            assert!(targets
                .iter()
                .all(|target| { target["applied"] == true && target["failure"] == "verification_failed" }));
        }
        Scenario::SessionShutdownFailure => unreachable!("shutdown replaces the typed response"),
        Scenario::ExecuteWithoutConfirm
        | Scenario::ExecuteWithoutReason
        | Scenario::MissingWriteScope
        | Scenario::ClusterNotAllowed
        | Scenario::OperationNotAllowed
        | Scenario::ArgumentOutOfBounds
        | Scenario::AuditWriteFailure
        | Scenario::SensitiveRuntimeFailure => unreachable!("error envelope has no mutation truth"),
    }
}

fn assert_lifecycle(tool: ToolKind, scenario: Scenario, counters: &LifecycleCounters) {
    let expected = scenario.sessions_for_two_calls();
    assert_eq!(
        expected > 0,
        scenario.reaches_runtime() && scenario != Scenario::AuditWriteFailure,
        "{scenario:?}"
    );
    assert_eq!(
        counters.starts.load(Ordering::SeqCst),
        expected,
        "{tool:?}/{scenario:?}"
    );
    assert_eq!(counters.runs.load(Ordering::SeqCst), expected, "{tool:?}/{scenario:?}");
    assert_eq!(
        counters.preflight_reads.load(Ordering::SeqCst),
        expected,
        "{tool:?}/{scenario:?}"
    );
    assert_eq!(
        counters.shutdowns.load(Ordering::SeqCst),
        expected,
        "{tool:?}/{scenario:?}"
    );
    let mutation = if matches!(scenario, Scenario::ValidDryRun | Scenario::SensitiveRuntimeFailure) {
        0
    } else {
        expected
    };
    assert_eq!(
        counters.mutation_calls.load(Ordering::SeqCst),
        mutation,
        "{tool:?}/{scenario:?}"
    );
    let verification = if matches!(
        scenario,
        Scenario::ValidExecute
            | Scenario::MultiTargetPartial
            | Scenario::PostReadFailure
            | Scenario::SessionShutdownFailure
    ) {
        expected
    } else {
        0
    };
    assert_eq!(
        counters.verification_reads.load(Ordering::SeqCst),
        verification,
        "{tool:?}/{scenario:?}"
    );
    assert_eq!(
        counters.sensitive_failures.load(Ordering::SeqCst),
        if scenario == Scenario::SensitiveRuntimeFailure {
            expected
        } else {
            0
        },
        "{tool:?}/{scenario:?}"
    );
}

async fn assert_audit(tool: ToolKind, scenario: Scenario, sink: &MemoryAuditSink) {
    let records = sink.records().await.unwrap();
    let Some(expected_terminal) = scenario.expected_audit_result() else {
        assert!(records.is_empty(), "unexpected audit for {tool:?}/{scenario:?}");
        return;
    };
    assert_eq!(records.len(), 4, "audit pair count for {tool:?}/{scenario:?}");
    for pair in records.chunks_exact(2) {
        let started = &pair[0];
        let terminal = &pair[1];
        assert_eq!(started.event, AuditEvent::Started);
        assert_eq!(started.result, AuditResult::Started);
        assert_eq!(started.error_code, None);
        assert_eq!(terminal.result, expected_terminal);
        assert_eq!(terminal.error_code, scenario.expected_code());
        assert_eq!(started.operation, tool.operation());
        assert_eq!(terminal.operation, tool.operation());
        assert_eq!(started.cluster.as_str(), "cluster-a");
        assert_eq!(terminal.cluster.as_str(), "cluster-a");
        assert_eq!(started.operator.as_deref(), Some(SAFE_OPERATOR));
        assert_eq!(terminal.operator.as_deref(), Some(SAFE_OPERATOR));
        assert_eq!(started.reason.as_deref(), Some(SAFE_REASON));
        assert_eq!(terminal.reason.as_deref(), Some(SAFE_REASON));
        assert_eq!(
            started.mode,
            if scenario == Scenario::ValidDryRun {
                AuditMode::DryRun
            } else {
                AuditMode::Execute
            }
        );
        assert_eq!(terminal.mode, started.mode);
        assert_eq!(
            terminal.event,
            if scenario.expected_code().is_some() {
                AuditEvent::Failed
            } else {
                AuditEvent::Completed
            }
        );
        assert!(terminal.duration_millis.is_some());
    }
}

struct TerminalFailureAuditSink {
    inner: MemoryAuditSink,
    appends: AtomicUsize,
}

impl TerminalFailureAuditSink {
    fn new() -> Self {
        Self {
            inner: MemoryAuditSink::new(8, 4096),
            appends: AtomicUsize::new(0),
        }
    }
}

impl ReliableAuditSink for TerminalFailureAuditSink {
    fn append<'a>(&'a self, record: &'a AuditRecord) -> AuditFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            if self.appends.fetch_add(1, Ordering::SeqCst) == 1 {
                Err(ControlError::audit_unavailable())
            } else {
                self.inner.append(record).await
            }
        })
    }

    fn records(&self) -> AuditFuture<'_, Result<Vec<AuditRecord>, ControlError>> {
        self.inner.records()
    }
}

struct BlockingMutationAdminFactory {
    counters: Arc<LifecycleCounters>,
    gate: Arc<tokio::sync::Notify>,
}

impl MutationToolSessionFactory for BlockingMutationAdminFactory {
    fn open<'a>(
        &'a self,
        _cluster: &'a ClusterName,
    ) -> RuntimeFuture<'a, Result<Box<dyn MutationToolSession>, ControlError>> {
        Box::pin(async move {
            self.counters.starts.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(BlockingMutationAdminSession {
                counters: Arc::clone(&self.counters),
                gate: Arc::clone(&self.gate),
            }) as Box<dyn MutationToolSession>)
        })
    }
}

struct BlockingMutationAdminSession {
    counters: Arc<LifecycleCounters>,
    gate: Arc<tokio::sync::Notify>,
}

impl MutationToolSession for BlockingMutationAdminSession {
    fn run<'a>(
        &'a mut self,
        request: MutationToolRequest,
    ) -> RuntimeFuture<'a, Result<MutationToolResponse, ControlError>> {
        Box::pin(async move {
            self.counters.runs.fetch_add(1, Ordering::SeqCst);
            self.counters.preflight_reads.fetch_add(1, Ordering::SeqCst);
            self.gate.notified().await;
            self.counters.mutation_calls.fetch_add(1, Ordering::SeqCst);
            self.counters.verification_reads.fetch_add(1, Ordering::SeqCst);
            Ok(response_for(request, Scenario::ValidExecute))
        })
    }

    fn shutdown(&mut self) -> RuntimeFuture<'_, Result<(), ControlError>> {
        Box::pin(async move {
            self.counters.shutdowns.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

fn changed_collision_arguments(tool: ToolKind) -> serde_json::Value {
    let mut value = arguments(tool, Scenario::ValidExecute);
    match tool {
        ToolKind::Topic => value["write_queue_nums"] = serde_json::json!(9),
        ToolKind::ConsumerGroup => value["retry_max_times"] = serde_json::json!(17),
        ToolKind::Offset => value["force"] = serde_json::json!(true),
        ToolKind::BrokerConfig => value["properties"]["traceTopicEnable"] = serde_json::json!("false"),
        ToolKind::RequestMode => value["pop_share_queue_num"] = serde_json::json!(5),
        ToolKind::DrainBegin | ToolKind::DrainCancel | ToolKind::LogFilterRestore => {
            value["operation_id"] = serde_json::json!("matrix-operation-2");
        }
        ToolKind::LogFilterSet => value["ttl_seconds"] = serde_json::json!(301),
    }
    value
}

#[test]
fn enabled_tool_contract_marks_state_replacing_tools_destructive_and_restoring_ones_not() {
    let enabled = MutationPolicyConfig {
        mutations_enabled: true,
        dry_run: true,
        allowed_operations: ToolKind::ALL.into_iter().map(ToolKind::operation).collect(),
        allowed_clusters: vec![ClusterName::try_new("cluster-a").unwrap()],
        operation_timeout_seconds: 2,
        require_plan_digest: false,
        emergency_stop_file: None,
    };
    let tools = crate::catalog::OperationCatalog::from_policy(&enabled).list_tools().tools;
    assert_eq!(tools.len(), 9);
    // Ending a drain gives a Proxy back, and a log filter is bounded and ends by itself.
    let restoring = [
        crate::tools::CANCEL_PROXY_DRAIN_TOOL,
        crate::tools::SET_BROKER_LOG_FILTER_TTL_TOOL,
        crate::tools::RESTORE_BROKER_LOG_FILTER_TOOL,
    ];
    for tool in tools {
        let annotations = tool.annotations.unwrap();
        assert_eq!(annotations.read_only_hint, Some(false));
        assert_eq!(
            annotations.destructive_hint,
            Some(!restoring.contains(&tool.name.as_ref())),
            "{}",
            tool.name
        );
        assert_eq!(annotations.idempotent_hint, Some(true));
        assert_eq!(tool.input_schema["additionalProperties"], false);
        // No Tool takes an address: a Proxy is named by its alias and a Broker by its name.
        let properties = tool.input_schema["properties"].as_object().unwrap();
        assert!(properties.keys().all(|name| !name.contains("addr") && !name.contains("endpoint")));
        if tool.name.as_ref() == crate::tools::PATCH_BROKER_CONFIG_TOOL {
            assert!(tool.input_schema["properties"].get("broker_name").is_some());
            assert!(tool.input_schema["properties"].get("broker_names").is_none());
        }
    }

    let mut disabled = enabled;
    disabled.mutations_enabled = false;
    assert!(crate::catalog::OperationCatalog::from_policy(&disabled)
        .list_tools()
        .tools
        .is_empty());
}

#[tokio::test]
async fn terminal_audit_failure_never_replays_an_unaudited_success() {
    for (index, tool) in ToolKind::ALL.into_iter().enumerate() {
        let counters = Arc::new(LifecycleCounters::default());
        let sink = Arc::new(TerminalFailureAuditSink::new());
        let router = matrix_router(
            Arc::new(FakeMutationAdminFactory {
                scenario: Scenario::ValidExecute,
                counters: Arc::clone(&counters),
            }),
            sink.clone(),
        )
        .await;
        let bearer = token_for(tool, Scenario::AuditWriteFailure);
        let request_arguments = arguments(tool, Scenario::AuditWriteFailure);
        let first = acceptance_tool_call(
            &router,
            &bearer,
            1_000 + index * 2,
            tool.name(),
            request_arguments.clone(),
        )
        .await;
        let second = acceptance_tool_call(
            &router,
            &bearer,
            1_001 + index * 2,
            tool.name(),
            request_arguments,
        )
        .await;
        assert_public_contract(tool, Scenario::AuditWriteFailure, &[], &first, &second, &bearer);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.runs.load(Ordering::SeqCst), 1);
        assert_eq!(counters.preflight_reads.load(Ordering::SeqCst), 1);
        assert_eq!(counters.mutation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(counters.verification_reads.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        let records = sink.records().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].event, AuditEvent::Started);
        assert_eq!(records[0].operation, tool.operation());
        assert_eq!(records[0].operator.as_deref(), Some(SAFE_OPERATOR));
        assert_eq!(records[0].reason.as_deref(), Some(SAFE_REASON));
    }
}

#[tokio::test]
async fn raw_backend_failures_are_sanitized_across_every_public_surface() {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    for (index, tool) in ToolKind::ALL.into_iter().enumerate() {
        let harness = matrix_harness(Scenario::SensitiveRuntimeFailure).await;
        let bearer = token_for(tool, Scenario::SensitiveRuntimeFailure);
        let request_arguments = arguments(tool, Scenario::SensitiveRuntimeFailure);
        let first = acceptance_tool_call(
            &harness.router,
            &bearer,
            1_500 + index * 2,
            tool.name(),
            request_arguments.clone(),
        )
        .await;
        let second = acceptance_tool_call(
            &harness.router,
            &bearer,
            1_501 + index * 2,
            tool.name(),
            request_arguments,
        )
        .await;
        assert_public_contract(
            tool,
            Scenario::SensitiveRuntimeFailure,
            &[],
            &first,
            &second,
            &bearer,
        );
        assert_lifecycle(tool, Scenario::SensitiveRuntimeFailure, &harness.counters);
        assert_audit(tool, Scenario::SensitiveRuntimeFailure, &harness.sink).await;

        let records = harness.sink.records().await.unwrap();
        let audit_surface = format!("{records:?}");
        let public_surface = format!("{}{}", first.body, second.body);
        for forbidden in [
            BACKEND_CREDENTIAL,
            BACKEND_ENDPOINT,
            RAW_BACKEND_ERROR,
            MESSAGE_BODY,
            CLIENT_IDENTITY,
        ] {
            assert!(!public_surface.contains(forbidden));
            assert!(!audit_surface.contains(forbidden));
        }

        let opens = Arc::new(AtomicUsize::new(0));
        let open_sink = Arc::new(MemoryAuditSink::new(8, 4096));
        let open_router = matrix_router(
            Arc::new(SensitiveOpenFailureFactory {
                opens: Arc::clone(&opens),
            }),
            open_sink.clone(),
        )
        .await;
        let open_first = acceptance_tool_call(
            &open_router,
            &bearer,
            1_600 + index * 2,
            tool.name(),
            arguments(tool, Scenario::SensitiveRuntimeFailure),
        )
        .await;
        let open_second = acceptance_tool_call(
            &open_router,
            &bearer,
            1_601 + index * 2,
            tool.name(),
            arguments(tool, Scenario::SensitiveRuntimeFailure),
        )
        .await;
        assert_public_contract(
            tool,
            Scenario::SensitiveRuntimeFailure,
            &[],
            &open_first,
            &open_second,
            &bearer,
        );
        // A session that could not be opened settles nothing, so the second call tries again.
        assert_eq!(opens.load(Ordering::SeqCst), 2);
        let open_records = open_sink.records().await.unwrap();
        assert_eq!(open_records.len(), 4);
        for pair in open_records.chunks_exact(2) {
            assert_eq!(pair[0].event, AuditEvent::Started);
            assert_eq!(pair[1].event, AuditEvent::Failed);
            assert_eq!(pair[1].result, AuditResult::Failed);
            assert_eq!(pair[1].error_code, Some(ControlErrorCode::ExecutionFailed));
        }
        let open_surfaces = format!("{}{}{open_records:?}", open_first.body, open_second.body);
        for forbidden in [
            BACKEND_CREDENTIAL,
            BACKEND_ENDPOINT,
            RAW_BACKEND_ERROR,
            MESSAGE_BODY,
            CLIENT_IDENTITY,
        ] {
            assert!(!open_surfaces.contains(forbidden));
        }
    }

    let captured = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    for forbidden in [
        SAFE_OPERATOR,
        SAFE_REASON,
        BACKEND_CREDENTIAL,
        BACKEND_ENDPOINT,
        RAW_BACKEND_ERROR,
        MESSAGE_BODY,
        CLIENT_IDENTITY,
        "Bearer ",
    ] {
        assert!(!captured.contains(forbidden));
    }
}

#[tokio::test]
async fn adapter_error_codes_survive_canonical_redaction_for_open_run_followers_and_replay() {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let codes = [
        ControlErrorCode::InvalidConfig,
        ControlErrorCode::RequestRejected,
        ControlErrorCode::Unauthorized,
        ControlErrorCode::PermissionDenied,
        ControlErrorCode::ClusterNotAllowed,
        ControlErrorCode::OperationNotAllowed,
        ControlErrorCode::MutationDisabled,
        ControlErrorCode::OperationUnavailable,
        ControlErrorCode::RateLimited,
        ControlErrorCode::ConfirmationRequired,
        ControlErrorCode::InvalidArgument,
        ControlErrorCode::AuditUnavailable,
        ControlErrorCode::PreconditionConflict,
        ControlErrorCode::PartialApply,
        ControlErrorCode::VerificationFailed,
        ControlErrorCode::Timeout,
        ControlErrorCode::Cancelled,
        ControlErrorCode::ExecutionFailed,
        ControlErrorCode::ShutdownFailed,
    ];

    for stage in [HostileFailureStage::Open, HostileFailureStage::Run] {
        for (index, code) in codes.into_iter().enumerate() {
            let opens = Arc::new(AtomicUsize::new(0));
            let runs = Arc::new(AtomicUsize::new(0));
            let shutdowns = Arc::new(AtomicUsize::new(0));
            let gate = Arc::new(tokio::sync::Notify::new());
            let sink = Arc::new(MemoryAuditSink::new(16, 4096));
            let router = matrix_router(
                Arc::new(HostileErrorFactory {
                    code,
                    retryable: !expected_adapter_retryable(code),
                    stage,
                    gate: Arc::clone(&gate),
                    opens: Arc::clone(&opens),
                    runs: Arc::clone(&runs),
                    shutdowns: Arc::clone(&shutdowns),
                }),
                sink.clone(),
            )
            .await;
            let tool = ToolKind::Topic;
            let bearer = token_for(tool, Scenario::SensitiveRuntimeFailure);
            let mut request_arguments = arguments(tool, Scenario::SensitiveRuntimeFailure);
            request_arguments["request_key"] = serde_json::json!(format!(
                "hostile-{}-{index}",
                match stage {
                    HostileFailureStage::Open => "open",
                    HostileFailureStage::Run => "run",
                }
            ));
            let leader_call = acceptance_tool_call(
                &router,
                &bearer,
                30_000 + index * 3,
                tool.name(),
                request_arguments.clone(),
            );
            let follower_call = acceptance_tool_call(
                &router,
                &bearer,
                30_001 + index * 3,
                tool.name(),
                request_arguments.clone(),
            );
            let release = async {
                tokio::time::timeout(std::time::Duration::from_secs(1), async {
                    while opens.load(Ordering::SeqCst) == 0
                        || (matches!(stage, HostileFailureStage::Run) && runs.load(Ordering::SeqCst) == 0)
                        || sink.records().await.unwrap().len() < 2
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("leader and follower must reach the hostile adapter boundary");
                gate.notify_one();
            };
            let (leader, follower, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(leader_call, follower_call, release)
            })
            .await
            .expect("hostile leader/follower calls must finish");
            let expected = expected_adapter_error_envelope(code);
            assert_eq!(mcp_payload(&leader, true), &expected);
            assert_eq!(mcp_payload(&follower, true), &expected);

            // Only a conflict or a partial apply is settled and replayed. After any other code
            // the same key is a new attempt, which reaches the hostile adapter again.
            let settled = matches!(
                code,
                ControlErrorCode::PreconditionConflict | ControlErrorCode::PartialApply
            );
            if !settled {
                gate.notify_one();
            }
            let replay = acceptance_tool_call(
                &router,
                &bearer,
                30_002 + index * 3,
                tool.name(),
                request_arguments,
            )
            .await;
            assert_eq!(mcp_payload(&replay, true), &expected);
            let attempts = if settled { 1 } else { 2 };
            let sessions = if matches!(stage, HostileFailureStage::Run) {
                attempts
            } else {
                0
            };
            assert_eq!(opens.load(Ordering::SeqCst), attempts, "{code:?}");
            assert_eq!(runs.load(Ordering::SeqCst), sessions, "{code:?}");
            assert_eq!(shutdowns.load(Ordering::SeqCst), sessions, "{code:?}");

            let records = sink.records().await.unwrap();
            assert_eq!(records.len(), 6);
            assert_eq!(
                records.iter().filter(|record| record.event == AuditEvent::Started).count(),
                3
            );
            for record in records.iter().filter(|record| record.event == AuditEvent::Started) {
                assert_eq!(record.result, AuditResult::Started);
                assert_eq!(record.error_code, None);
            }
            let terminal = records
                .iter()
                .filter(|record| record.event == AuditEvent::Failed)
                .collect::<Vec<_>>();
            assert_eq!(terminal.len(), 3);
            let expected_audit_result = match code {
                ControlErrorCode::PreconditionConflict => AuditResult::Conflict,
                ControlErrorCode::PartialApply => AuditResult::Partial,
                _ => AuditResult::Failed,
            };
            for record in terminal {
                assert_eq!(record.result, expected_audit_result);
                assert_eq!(record.error_code, Some(code));
            }
            let surfaces = format!("{}{}{}{records:?}", leader.body, follower.body, replay.body);
            for forbidden in [
                BACKEND_CREDENTIAL,
                BACKEND_ENDPOINT,
                RAW_BACKEND_ERROR,
                MESSAGE_BODY,
                CLIENT_IDENTITY,
            ] {
                assert!(!surfaces.contains(forbidden));
            }
        }
    }

    let captured = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    for forbidden in [
        BACKEND_CREDENTIAL,
        BACKEND_ENDPOINT,
        RAW_BACKEND_ERROR,
        MESSAGE_BODY,
        CLIENT_IDENTITY,
    ] {
        assert!(!captured.contains(forbidden));
    }
}

#[tokio::test]
async fn request_keys_share_leaders_replay_results_and_reject_collisions_for_every_tool() {
    for (index, tool) in ToolKind::ALL.into_iter().enumerate() {
        let counters = Arc::new(LifecycleCounters::default());
        let sink = Arc::new(MemoryAuditSink::new(16, 4096));
        let gate = Arc::new(tokio::sync::Notify::new());
        let router = matrix_router(
            Arc::new(BlockingMutationAdminFactory {
                counters: Arc::clone(&counters),
                gate: Arc::clone(&gate),
            }),
            sink.clone(),
        )
        .await;
        let bearer = token_for(tool, Scenario::ValidExecute);
        let request_arguments = arguments(tool, Scenario::ValidExecute);
        let leader_call = acceptance_tool_call(
            &router,
            &bearer,
            2_000 + index * 4,
            tool.name(),
            request_arguments.clone(),
        );
        let follower_call = acceptance_tool_call(
            &router,
            &bearer,
            2_001 + index * 4,
            tool.name(),
            request_arguments.clone(),
        );
        let release = async {
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while counters.runs.load(Ordering::SeqCst) == 0 || sink.records().await.unwrap().len() < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("leader and follower must both write started audit records");
            gate.notify_one();
        };
        let (first, follower, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(leader_call, follower_call, release)
        })
        .await
        .expect("leader/follower acceptance calls must finish");
        assert_public_contract(
            tool,
            Scenario::ValidExecute,
            &[],
            &first,
            &follower,
            &bearer,
        );
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.runs.load(Ordering::SeqCst), 1);
        assert_eq!(counters.mutation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(counters.verification_reads.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);

        let collision = acceptance_tool_call(
            &router,
            &bearer,
            2_002 + index * 4,
            tool.name(),
            changed_collision_arguments(tool),
        )
        .await;
        let collision_payload = mcp_payload(&collision, true);
        assert_eq!(collision_payload["schema_version"], ERROR_SCHEMA_VERSION);
        assert_eq!(collision_payload["code"], "invalid_argument");
        assert_eq!(collision_payload, &expected_error_envelope(Scenario::ArgumentOutOfBounds));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.mutation_calls.load(Ordering::SeqCst), 1);

        let replay = acceptance_tool_call(
            &router,
            &bearer,
            2_003 + index * 4,
            tool.name(),
            request_arguments,
        )
        .await;
        assert_same_response_under_own_invocations(
            mcp_payload(&replay, false),
            mcp_payload(&first, false),
            &format!("{tool:?}/replay"),
        );
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.mutation_calls.load(Ordering::SeqCst), 1);

        let records = sink.records().await.unwrap();
        assert_eq!(records.len(), 8);
        assert_eq!(records.iter().filter(|record| record.event == AuditEvent::Started).count(), 4);
        assert_eq!(
            records
                .iter()
                .filter(|record| record.result == AuditResult::Applied)
                .count(),
            3
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| record.error_code == Some(ControlErrorCode::InvalidArgument))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn nine_tool_by_fourteen_scenario_acceptance_matrix() {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let mut applicable = 0;
    let mut passed = 0;
    let mut not_applicable = 0;
    let mut exact_contract = BTreeMap::new();

    for tool in ToolKind::ALL {
        for scenario in Scenario::ALL {
            if let (Some((accepted, rejected)), Scenario::MultiTargetPartial) = (tool.single_target(), scenario) {
                // A Tool that changes one target cannot end partially: it takes no target list.
                let value = arguments(tool, scenario);
                assert!(value.get(accepted).is_some());
                assert!(value.get(rejected).is_none());
                assert!(tool.decodes(value.clone()));
                let mut listed = value;
                listed[rejected] = serde_json::json!(["first-target", "second-target"]);
                assert!(!tool.decodes(listed), "{tool:?} accepted {rejected}");
                assert!(exact_contract
                    .insert(
                        format!("{}/{}", tool.slug(), scenario.slug()),
                        serde_json::json!({
                            "applicability": "not_applicable",
                            "contract": format!("{}_is_single_target", tool.claim()),
                            "accepted_argument": accepted,
                            "rejected_argument": rejected
                        }),
                    )
                    .is_none());
                not_applicable += 1;
                continue;
            }
            applicable += 1;
            let harness = matrix_harness(scenario).await;
            let bearer = token_for(tool, scenario);
            let request_arguments = arguments(tool, scenario);
            let first = acceptance_tool_call(
                &harness.router,
                &bearer,
                applicable * 2,
                tool.name(),
                request_arguments.clone(),
            )
            .await;
            let second = acceptance_tool_call(
                &harness.router,
                &bearer,
                applicable * 2 + 1,
                tool.name(),
                request_arguments,
            )
            .await;
            let payload = assert_public_contract(
                tool,
                scenario,
                expected_violations(tool, scenario),
                &first,
                &second,
                &bearer,
            );
            assert!(exact_contract
                .insert(format!("{}/{}", tool.slug(), scenario.slug()), payload)
                .is_none());
            if scenario == Scenario::ArgumentOutOfBounds {
                let mut unknown = arguments(tool, Scenario::ValidExecute);
                unknown["unexpected"] = serde_json::json!(true);
                let unknown_first = acceptance_tool_call(
                    &harness.router,
                    &bearer,
                    10_000 + applicable * 2,
                    tool.name(),
                    unknown.clone(),
                )
                .await;
                let unknown_second = acceptance_tool_call(
                    &harness.router,
                    &bearer,
                    10_001 + applicable * 2,
                    tool.name(),
                    unknown,
                )
                .await;
                assert_public_contract(
                    tool,
                    Scenario::ArgumentOutOfBounds,
                    &[("/*", "additional_property")],
                    &unknown_first,
                    &unknown_second,
                    &bearer,
                );
                // The undeclared property is located without repeating the name the caller chose.
                assert!(!format!("{}{}", unknown_first.body, unknown_second.body).contains("unexpected"));
            }
            if scenario == Scenario::ExecuteWithoutReason {
                let mut unsafe_reason = arguments(tool, Scenario::ValidExecute);
                unsafe_reason["reason"] = serde_json::json!("token=must-not-leak");
                let unsafe_first = acceptance_tool_call(
                    &harness.router,
                    &bearer,
                    20_000 + applicable * 2,
                    tool.name(),
                    unsafe_reason.clone(),
                )
                .await;
                let unsafe_second = acceptance_tool_call(
                    &harness.router,
                    &bearer,
                    20_001 + applicable * 2,
                    tool.name(),
                    unsafe_reason,
                )
                .await;
                assert_public_contract(
                    tool,
                    Scenario::ExecuteWithoutReason,
                    &[],
                    &unsafe_first,
                    &unsafe_second,
                    &bearer,
                );
                assert!(
                    !format!("{}{}", unsafe_first.body, unsafe_second.body).contains("token=must-not-leak")
                );
            }
            assert_lifecycle(tool, scenario, &harness.counters);
            assert_audit(tool, scenario, &harness.sink).await;
            passed += 1;
        }
    }

    assert_eq!((applicable, passed, not_applicable), (121, 121, 5));
    assert_eq!(exact_contract.len(), 126);
    insta::assert_json_snapshot!("nine_tool_by_fourteen_scenario_acceptance_truth", exact_contract);
    let captured = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    for forbidden in [
        SAFE_OPERATOR,
        SAFE_REASON,
        BACKEND_CREDENTIAL,
        BACKEND_ENDPOINT,
        RAW_BACKEND_ERROR,
        MESSAGE_BODY,
        CLIENT_IDENTITY,
        OPERATION_ID,
        "127.0.0.1",
        "Bearer ",
    ] {
        assert!(!captured.contains(forbidden), "logs leaked protected material");
    }
}

#[tokio::test]
async fn unknown_proxy_alias_is_rejected_before_a_call_is_recorded_or_a_session_opens() {
    for (index, tool) in [ToolKind::DrainBegin, ToolKind::DrainCancel].into_iter().enumerate() {
        let harness = matrix_harness(Scenario::ValidExecute).await;
        let bearer = token_for(tool, Scenario::ValidExecute);
        // Each alias is well formed. It is just not one that the selected cluster configures.
        for (attempt, alias) in ["proxy-b", "Proxy-A", "proxy-a-"].into_iter().enumerate() {
            let mut request_arguments = arguments(tool, Scenario::ValidExecute);
            request_arguments["proxy_name"] = serde_json::json!(alias);
            let call = acceptance_tool_call(
                &harness.router,
                &bearer,
                40_000 + index * 10 + attempt,
                tool.name(),
                request_arguments,
            )
            .await;
            assert_eq!(
                mcp_payload(&call, true),
                &serde_json::json!({
                    "schema_version": ERROR_SCHEMA_VERSION,
                    "code": "invalid_argument",
                    "message": "mutation argument is invalid",
                    "retryable": false,
                    "violations": [{"path": "/proxy_name", "constraint": "other"}]
                }),
                "{tool:?}/{alias}"
            );
            // The refusal repeats neither the alias that was sent nor one that is configured.
            assert!(!call.body.to_string().contains(alias));
        }
        assert_eq!(harness.counters.starts.load(Ordering::SeqCst), 0);
        assert_eq!(harness.counters.runs.load(Ordering::SeqCst), 0);
        assert!(harness.sink.records().await.unwrap().is_empty());

        // The configured alias is recorded as the object of the call, and nothing else about it.
        let call = acceptance_tool_call(
            &harness.router,
            &bearer,
            40_009 + index * 10,
            tool.name(),
            arguments(tool, Scenario::ValidExecute),
        )
        .await;
        assert_eq!(mcp_payload(&call, false)["target"], serde_json::json!({"proxy_name": "proxy-a"}));
        let records = harness.sink.records().await.unwrap();
        assert_eq!(records.len(), 2);
        for record in &records {
            let target = serde_json::to_value(record.target.as_ref().unwrap()).unwrap();
            assert_eq!(target, serde_json::json!({"proxy": "proxy-a"}));
            // The identifier of the drain is digested with what was asked, never recorded.
            let digest = record.requested_digest.as_deref().unwrap();
            assert!(digest.len() == 64 && !digest.contains(OPERATION_ID));
        }
    }
}
