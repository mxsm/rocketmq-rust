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

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use chrono::SecondsFormat;
use chrono::Utc;
use rocketmq_admin_core::core::consumer::ConsumerMutationAdmin;
use rocketmq_admin_core::core::consumer::DeleteSubscriptionGroupsRequest;
use rocketmq_admin_core::core::supervised_mutation::BrokerMutationConfigState;
use rocketmq_admin_core::core::supervised_mutation::RequestMode;
use rocketmq_admin_core::core::supervised_mutation::RequestModePreflightRequest;
use rocketmq_admin_core::core::supervised_mutation::RequestModeValue;
use rocketmq_admin_core::core::supervised_mutation::SupervisedMutationAdmin;
use rocketmq_admin_core::core::supervised_mutation::TopicMessageType;
use rocketmq_admin_core::core::supervised_mutation::TopicMutationPreflightRequest;
use rocketmq_admin_core::core::supervised_mutation::TopicReplacement;
use rocketmq_admin_core::core::topic::DeleteTopicAdminRequest;
use rocketmq_admin_core::core::topic::ResetTopicConsumerOffsetRequest;
use rocketmq_admin_core::core::topic::TopicMutationAdmin;
use rocketmq_admin_core::core::topic::TopicSendRequest;
use rocketmq_admin_core::core::AdminError;
use rocketmq_admin_core::mutation_client_adapter::MutationAdminBuilder;
use rocketmq_admin_core::mutation_client_adapter::MutationAdminSession;
use serde_json::Value;
use tempfile::TempDir;

use super::ensure;
use super::process::ClusterProcesses;
use super::process::PortSet;
use super::protocol;
use super::protocol::ControlInstance;
use super::protocol::McpClient;
use super::E2eContext;
use super::E2eError;
use super::E2eResult;
use super::EXPECTED_AUDIT_RECORDS;
use super::EXPECTED_CALLS;
use super::MESSAGE_BODY;
use crate::audit::AuditEvent;
use crate::audit::AuditMode;
use crate::audit::AuditRecord;
use crate::audit::AuditResult;
use crate::audit::AuditSchemaVersion;
use crate::error::ControlErrorCode;
use crate::model::ControlOperation;
use crate::tools::PATCH_BROKER_CONFIG_TOOL;
use crate::tools::RESET_CONSUMER_OFFSET_TOOL;
use crate::tools::SET_CONSUMER_REQUEST_MODE_TOOL;
use crate::tools::UPSERT_CONSUMER_GROUP_TOOL;
use crate::tools::UPSERT_TOPIC_TOOL;

const CLUSTER: &str = "E2eCluster";
const BROKER_CLUSTER: &str = CLUSTER;
const BROKER: &str = "e2e-broker";
const ARGUMENT_SCHEMA: &str = "rocketmq-mcp-control.arguments.v1";
const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(45);
const FIXTURE_STOP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CleanupEvidence {
    pub cluster_root_removed: bool,
    pub children_reaped: bool,
    pub broker_config_restored: bool,
    pub consumer_request_mode_restored: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct ExpectedAudit {
    operation: ControlOperation,
    mode: AuditMode,
    result: AuditResult,
    error_code: Option<ControlErrorCode>,
}

struct ControlConfigMaterial {
    path: PathBuf,
    tls_private_key_markers: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum RestorationState {
    #[default]
    Clean,
    Required,
}

impl RestorationState {
    fn before_execute(&mut self) {
        *self = Self::Required;
    }

    fn complete(&mut self) {
        *self = Self::Clean;
    }

    const fn is_required(self) -> bool {
        matches!(self, Self::Required)
    }
}

pub(super) struct E2eHarness {
    temp: Option<TempDir>,
    root: PathBuf,
    ports: PortSet,
    cluster: ClusterProcesses,
    control_config: PathBuf,
    tls_private_key_markers: Vec<String>,
    audit_path: PathBuf,
    control: Option<ControlInstance>,
    mcp: Option<McpClient>,
    fixture: Option<FixtureAdmin>,
    topic: String,
    group: String,
    next_request_id: u64,
    expected_audit: Vec<ExpectedAudit>,
    original_broker_state: Option<BrokerMutationConfigState>,
    broker_change_generation: Option<u64>,
    broker_repeat_generation: Option<u64>,
    broker_restore_generation: Option<u64>,
    broker_restart_generation: Option<u64>,
    broker_restoration: RestorationState,
    request_mode_baseline: Option<RequestModeValue>,
    request_mode_restoration: RestorationState,
    topic_created: bool,
    group_created: bool,
    cleanup_outcome: Option<Result<CleanupEvidence, String>>,
}

impl E2eHarness {
    pub fn sanitized_process_diagnostics(&self) -> String {
        self.cluster.sanitized_diagnostics()
    }

    pub async fn start() -> E2eResult<Self> {
        let temp = tempfile::Builder::new()
            .prefix("rocketmq-mcp-control-e2e-")
            .tempdir()
            .e2e("create isolated E2E root")?;
        let root = temp.path().to_path_buf();
        let mut ports = PortSet::allocate()?;
        let run_id = unique_id()?;
        let topic = format!("McpE2eTopic_{run_id}");
        let group = format!("McpE2eGroup_{run_id}");
        let namesrv_config = write_namesrv_config(&root, &ports)?;
        let broker_config = write_broker_config(&root, &ports)?;
        let audit_path = root.join("control-audit.jsonl");
        let ControlConfigMaterial {
            path: control_config,
            tls_private_key_markers,
        } = write_control_config(&root, &ports, &audit_path)?;
        let mut cluster = ClusterProcesses::new(&root, &ports, namesrv_config, broker_config)?;
        cluster.start_namesrv(&mut ports).await?;
        cluster.start_broker(&mut ports).await?;
        let mut fixture = FixtureAdmin::start(ports.namesrv.value(), ports.broker.value()).await?;
        if let Err(error) = fixture.wait_for_broker(&topic).await {
            let diagnostics = cluster.sanitized_diagnostics();
            let fixture_cleanup_failed = fixture.shutdown().await.is_err();
            let cluster_cleanup_failed = cluster.stop_all().await.is_err();
            return Err(E2eError::new(format!(
                "{error}; {diagnostics}; fixture_cleanup_failed={fixture_cleanup_failed}; \
                 cluster_cleanup_failed={cluster_cleanup_failed}"
            )));
        }
        ports.control_https.release();
        let (control, mcp) = match ControlInstance::start(&control_config).await {
            Ok(started) => started,
            Err(error) => {
                let fixture_cleanup_failed = fixture.shutdown().await.is_err();
                let cluster_cleanup_failed = cluster.stop_all().await.is_err();
                return Err(E2eError::new(format!(
                    "{error}; fixture_cleanup_failed={fixture_cleanup_failed}; \
                     cluster_cleanup_failed={cluster_cleanup_failed}"
                )));
            }
        };
        Ok(Self {
            temp: Some(temp),
            root,
            ports,
            cluster,
            control: Some(control),
            mcp: Some(mcp),
            fixture: Some(fixture),
            control_config,
            tls_private_key_markers,
            audit_path,
            topic,
            group,
            next_request_id: 10,
            expected_audit: Vec::new(),
            original_broker_state: None,
            broker_change_generation: None,
            broker_repeat_generation: None,
            broker_restore_generation: None,
            broker_restart_generation: None,
            broker_restoration: RestorationState::default(),
            request_mode_baseline: None,
            request_mode_restoration: RestorationState::default(),
            topic_created: false,
            group_created: false,
            cleanup_outcome: None,
        })
    }

    pub async fn exercise(&mut self) -> E2eResult<()> {
        self.exercise_topic().await?;
        self.exercise_group().await?;
        self.exercise_offset().await?;
        self.exercise_broker_config().await?;
        self.exercise_request_mode().await?;
        self.exercise_broker_outage().await?;
        self.restart_control_and_prove_audit_recovery().await?;
        Ok(())
    }

    async fn exercise_topic(&mut self) -> E2eResult<()> {
        let dry = self
            .invoke(
                UPSERT_TOPIC_TOOL,
                topic_args(&self.topic, true, "topic-dry-0001"),
                ControlOperation::TopicUpsert,
                AuditMode::DryRun,
                AuditResult::Planned,
            )
            .await?;
        ensure(
            dry["status"] == "planned",
            format!(
                "Topic dry-run status={} error_code={} stable_code={} stable_message={} target_failures={:?}",
                dry["status"],
                dry["error_code"],
                dry["code"],
                dry["message"],
                dry["targets"].as_array().map(|targets| targets
                    .iter()
                    .map(|target| target["failure"].clone())
                    .collect::<Vec<_>>())
            ),
        )?;
        ensure(
            dry["before"][BROKER]["kind"] == "absent",
            "Topic pre-state was not absent",
        )?;
        ensure(dry["after"].is_null(), "Topic dry-run exposed a post-state")?;

        let applied = self
            .invoke(
                UPSERT_TOPIC_TOOL,
                topic_args(&self.topic, false, "topic-exec-0001"),
                ControlOperation::TopicUpsert,
                AuditMode::Execute,
                AuditResult::Applied,
            )
            .await?;
        self.topic_created = true;
        ensure(applied["status"] == "applied", "Topic execute was not applied")?;
        ensure(
            applied["before"] == dry["before"],
            "Topic execute pre-state differed from its dry-run pre-state",
        )?;
        ensure(
            applied["after"][BROKER]["kind"] == "present",
            "Topic execute did not verify a present post-state",
        )?;
        ensure(
            applied["after"][BROKER]["value"] == applied["requested"],
            "Topic execute did not verify the exact requested post-state",
        )?;

        let repeat = self
            .invoke(
                UPSERT_TOPIC_TOOL,
                topic_args(&self.topic, false, "topic-repeat-01"),
                ControlOperation::TopicUpsert,
                AuditMode::Execute,
                AuditResult::Applied,
            )
            .await?;
        ensure(repeat["status"] == "applied", "Topic repeat was not stable")?;
        ensure(
            repeat["before"] == repeat["after"],
            "Topic idempotent repeat changed the resulting state",
        )
    }

    async fn exercise_group(&mut self) -> E2eResult<()> {
        let dry = self
            .invoke(
                UPSERT_CONSUMER_GROUP_TOOL,
                group_args(&self.group, true, "group-dry-0001"),
                ControlOperation::ConsumerGroupUpsert,
                AuditMode::DryRun,
                AuditResult::Planned,
            )
            .await?;
        ensure(dry["status"] == "planned", "Consumer Group dry-run was not planned")?;
        ensure(
            dry["before"][BROKER]["kind"] == "absent",
            "Consumer Group pre-state was not absent",
        )?;
        ensure(dry["after"].is_null(), "Consumer Group dry-run exposed a post-state")?;

        let applied = self
            .invoke(
                UPSERT_CONSUMER_GROUP_TOOL,
                group_args(&self.group, false, "group-exec-0001"),
                ControlOperation::ConsumerGroupUpsert,
                AuditMode::Execute,
                AuditResult::Applied,
            )
            .await?;
        self.group_created = true;
        ensure(applied["status"] == "applied", "Consumer Group execute was not applied")?;
        ensure(
            applied["before"] == dry["before"],
            "Consumer Group execute pre-state differed from its dry-run pre-state",
        )?;
        ensure(
            applied["after"][BROKER]["kind"] == "present",
            "Consumer Group execute did not verify its exact post-state",
        )?;
        ensure(
            applied["after"][BROKER]["value"] == applied["requested"],
            "Consumer Group execute did not verify the exact requested values",
        )?;

        let repeat = self
            .invoke(
                UPSERT_CONSUMER_GROUP_TOOL,
                group_args(&self.group, false, "group-repeat-01"),
                ControlOperation::ConsumerGroupUpsert,
                AuditMode::Execute,
                AuditResult::Applied,
            )
            .await?;
        ensure(
            repeat["before"] == repeat["after"],
            "Consumer Group idempotent repeat changed the resulting state",
        )
    }

    async fn exercise_offset(&mut self) -> E2eResult<()> {
        let reset_time = current_millis()?;
        let topic = self.topic.clone();
        let group = self.group.clone();
        self.fixture_mut()?.publish_and_seed_progress(&topic, &group).await?;
        let initial = self
            .fixture_mut()?
            .offset_rows(&topic, &group, reset_time as i64)
            .await?;
        ensure(
            initial.iter().any(|(_, current, planned)| current > planned),
            "fixture did not create observable Consumer Group progress",
        )?;
        let timestamp = millis_timestamp(reset_time)?;
        let dry = self
            .invoke(
                RESET_CONSUMER_OFFSET_TOOL,
                offset_args(&self.topic, &self.group, &timestamp, true, "offset-dry-001"),
                ControlOperation::ConsumerOffsetReset,
                AuditMode::DryRun,
                AuditResult::Planned,
            )
            .await?;
        ensure(dry["status"] == "planned", "Offset Reset dry-run was not planned")?;
        let before = dry["before"].clone();
        let after_dry = self
            .fixture_mut()?
            .offset_rows(&topic, &group, reset_time as i64)
            .await?;
        ensure(initial == after_dry, "Offset Reset dry-run changed Consumer offsets")?;

        let applied = self
            .invoke(
                RESET_CONSUMER_OFFSET_TOOL,
                offset_args(&self.topic, &self.group, &timestamp, false, "offset-exec-001"),
                ControlOperation::ConsumerOffsetReset,
                AuditMode::Execute,
                AuditResult::Applied,
            )
            .await?;
        ensure(applied["status"] == "applied", "Offset Reset execute was not applied")?;
        ensure(
            applied["before"] == before,
            "Offset Reset before-state drifted after dry-run",
        )?;
        ensure(
            applied["targets"].as_array().is_some_and(|targets| {
                !targets.is_empty() && targets.iter().all(|target| target["after"] == target["planned"])
            }),
            "Offset Reset did not verify every queue post-state",
        )?;
        let observed = self
            .fixture_mut()?
            .offset_rows(&topic, &group, reset_time as i64)
            .await?;
        ensure(
            !observed.is_empty() && observed.iter().all(|(_, current, planned)| current == planned),
            "typed fixture did not observe every reset queue at its planned offset",
        )?;

        let repeat = self
            .invoke(
                RESET_CONSUMER_OFFSET_TOOL,
                offset_args(&self.topic, &self.group, &timestamp, false, "offset-repeat-1"),
                ControlOperation::ConsumerOffsetReset,
                AuditMode::Execute,
                AuditResult::Applied,
            )
            .await?;
        ensure(repeat["status"] == "applied", "Offset Reset repeat was not stable")?;
        ensure(
            repeat["targets"]
                .as_array()
                .is_some_and(|targets| targets.iter().all(|target| target["changed"] == false)),
            "Offset Reset repeat was not idempotent",
        )
    }

    async fn exercise_broker_config(&mut self) -> E2eResult<()> {
        let original = self
            .fixture
            .as_mut()
            .ok_or_else(|| E2eError::new("fixture Admin is unavailable"))?
            .broker_state()
            .await?;
        self.original_broker_state = Some(original);
        let desired = !original.trace_topic_enable;
        let dry = self
            .invoke(
                PATCH_BROKER_CONFIG_TOOL,
                broker_args(desired, true, "broker-dry-0001"),
                ControlOperation::BrokerConfigPatch,
                AuditMode::DryRun,
                AuditResult::Planned,
            )
            .await?;
        ensure(
            dry["before"][BROKER] == broker_state_json(original),
            "Broker dry-run did not expose the complete original allowlisted state",
        )?;
        ensure(dry["after"].is_null(), "Broker dry-run exposed a post-state")?;
        let after_dry = self.fixture_mut()?.broker_state().await?;
        ensure(
            after_dry == original,
            "Broker dry-run changed the configuration or generation",
        )?;

        self.broker_restoration.before_execute();
        let applied = self
            .invoke(
                PATCH_BROKER_CONFIG_TOOL,
                broker_args(desired, false, "broker-exec-0001"),
                ControlOperation::BrokerConfigPatch,
                AuditMode::Execute,
                AuditResult::Applied,
            )
            .await?;
        ensure(
            applied["before"][BROKER] == broker_state_json(original),
            "Broker execute pre-state differed from its dry-run pre-state",
        )?;
        ensure(
            applied["after"][BROKER]["traceTopicEnable"] == desired,
            format!(
                "Broker patch result did not verify the changed property: status={} error_code={} before={} after={} target={}",
                applied["status"],
                applied["error_code"],
                applied["before"][BROKER]["traceTopicEnable"],
                applied["after"][BROKER]["traceTopicEnable"],
                applied["targets"].as_array().and_then(|targets| targets.first()).cloned().unwrap_or(Value::Null),
            ),
        )?;
        let applied_generation = broker_generation(&applied, "after")?;
        ensure(
            applied_generation > original.generation,
            "Broker patch generation did not advance",
        )?;
        let applied_target = single_broker_target(&applied)?;
        ensure(
            applied_target["changed"] == true && applied_target["applied"] == true,
            "Broker patch did not report a changed, applied target",
        )?;
        self.broker_change_generation = Some(applied_generation);
        let mut expected = original;
        expected.trace_topic_enable = desired;
        expected.generation = applied_generation;
        ensure(
            applied["after"][BROKER] == broker_state_json(expected),
            "Broker patch response did not preserve all six allowlisted fields",
        )?;
        let observed = self.fixture_mut()?.broker_state().await?;
        ensure(
            observed == expected,
            "typed fixture did not observe the exact Broker configuration patch",
        )?;

        self.broker_restoration.before_execute();
        let repeat = self
            .invoke(
                PATCH_BROKER_CONFIG_TOOL,
                broker_args(desired, false, "broker-repeat-1"),
                ControlOperation::BrokerConfigPatch,
                AuditMode::Execute,
                AuditResult::Applied,
            )
            .await?;
        let repeat_before_generation = broker_generation(&repeat, "before")?;
        let repeat_generation = broker_generation(&repeat, "after")?;
        ensure(
            repeat_before_generation == applied_generation && repeat_generation > repeat_before_generation,
            "Broker no-op repeat did not advance from the preceding generation",
        )?;
        let mut repeat_before = expected;
        repeat_before.generation = repeat_before_generation;
        let mut repeat_after = expected;
        repeat_after.generation = repeat_generation;
        ensure(
            repeat["before"][BROKER] == broker_state_json(repeat_before)
                && repeat["after"][BROKER] == broker_state_json(repeat_after),
            "Broker patch repeat changed an allowlisted field",
        )?;
        let repeat_target = single_broker_target(&repeat)?;
        ensure(
            repeat_target["changed"] == false && repeat_target["applied"] == true,
            "Broker patch repeat did not report its applied no-op semantics",
        )?;
        let repeated = self.fixture_mut()?.broker_state().await?;
        ensure(
            repeated == repeat_after,
            "typed fixture did not observe the no-op repeat generation",
        )?;
        self.broker_repeat_generation = Some(repeat_generation);

        ensure(
            self.restore_broker_config(Some(true), "broker-restore1").await?,
            "Broker configuration was not restored in the mutation epoch",
        )
    }

    async fn exercise_request_mode(&mut self) -> E2eResult<()> {
        let baseline = RequestModeValue {
            mode: RequestMode::Pull,
            pop_share_queue_num: 0,
        };
        let topic = self.topic.clone();
        let group = self.group.clone();
        let before_baseline_dry = self.fixture_mut()?.request_mode(&topic, &group).await?;
        let dry = self
            .invoke(
                SET_CONSUMER_REQUEST_MODE_TOOL,
                request_mode_args(&self.topic, &self.group, "pull", 0, true, "mode-dry-base1"),
                ControlOperation::ConsumerRequestMode,
                AuditMode::DryRun,
                AuditResult::Planned,
            )
            .await?;
        ensure(
            dry["status"] == "planned",
            "request-mode baseline dry-run was not planned",
        )?;
        let after_baseline_dry = self.fixture_mut()?.request_mode(&topic, &group).await?;
        ensure(
            after_baseline_dry == before_baseline_dry,
            "request-mode baseline dry-run changed real Broker state",
        )?;
        self.request_mode_baseline = Some(baseline);
        self.request_mode_restoration.before_execute();
        self.invoke(
            SET_CONSUMER_REQUEST_MODE_TOOL,
            request_mode_args(&self.topic, &self.group, "pull", 0, false, "mode-exec-base"),
            ControlOperation::ConsumerRequestMode,
            AuditMode::Execute,
            AuditResult::Applied,
        )
        .await?;
        ensure(
            self.fixture_mut()?.request_mode(&topic, &group).await?.as_ref() == Some(&baseline),
            "typed fixture did not observe the request-mode baseline",
        )?;

        let before_change_dry = self.fixture_mut()?.request_mode(&topic, &group).await?;
        self.invoke(
            SET_CONSUMER_REQUEST_MODE_TOOL,
            request_mode_args(&self.topic, &self.group, "pop", 1, true, "mode-dry-change"),
            ControlOperation::ConsumerRequestMode,
            AuditMode::DryRun,
            AuditResult::Planned,
        )
        .await?;
        let after_change_dry = self.fixture_mut()?.request_mode(&topic, &group).await?;
        ensure(
            after_change_dry == before_change_dry,
            "request-mode change dry-run changed real Broker state",
        )?;
        self.request_mode_restoration.before_execute();
        let changed = self
            .invoke(
                SET_CONSUMER_REQUEST_MODE_TOOL,
                request_mode_args(&self.topic, &self.group, "pop", 1, false, "mode-exec-change"),
                ControlOperation::ConsumerRequestMode,
                AuditMode::Execute,
                AuditResult::Applied,
            )
            .await?;
        ensure(
            changed["after"][BROKER]["mode"] == "pop" && changed["after"][BROKER]["pop_share_queue_num"] == 1,
            "request-mode change was not verified",
        )?;
        let expected = RequestModeValue {
            mode: RequestMode::Pop,
            pop_share_queue_num: 1,
        };
        ensure(
            self.fixture_mut()?.request_mode(&topic, &group).await?.as_ref() == Some(&expected),
            "typed fixture did not observe the exact request-mode change",
        )?;
        self.request_mode_restoration.before_execute();
        let repeat = self
            .invoke(
                SET_CONSUMER_REQUEST_MODE_TOOL,
                request_mode_args(&self.topic, &self.group, "pop", 1, false, "mode-repeat-001"),
                ControlOperation::ConsumerRequestMode,
                AuditMode::Execute,
                AuditResult::Applied,
            )
            .await?;
        ensure(
            repeat["before"] == repeat["after"],
            "request-mode repeat changed the resulting state",
        )
    }

    async fn exercise_broker_outage(&mut self) -> E2eResult<()> {
        self.cluster.stop_broker().await?;
        let error = self
            .invoke_error(
                UPSERT_TOPIC_TOOL,
                topic_args(&self.topic, true, "outage-dry-0001"),
                ControlOperation::TopicUpsert,
                AuditMode::DryRun,
                ControlErrorCode::ExecutionFailed,
            )
            .await?;
        ensure(
            error["code"] == "execution_failed",
            "Broker outage did not return the stable error code",
        )?;
        ensure(
            error["message"] == "mutation execution failed",
            "Broker outage exposed a raw backend error",
        )?;

        self.cluster.restart_broker().await?;
        let fixture = self
            .fixture
            .as_mut()
            .ok_or_else(|| E2eError::new("fixture Admin is unavailable"))?;
        fixture.wait_for_broker(&self.topic).await?;
        let restarted = fixture.broker_state().await?;
        let original = self
            .original_broker_state
            .ok_or_else(|| E2eError::new("original Broker configuration is unavailable"))?;
        let restored_generation = self
            .broker_restore_generation
            .ok_or_else(|| E2eError::new("pre-outage Broker restoration generation is unavailable"))?;
        // Generation is process-local: restart begins a new epoch from the static TOML state.
        ensure(
            restarted.generation == original.generation
                && restarted.generation < restored_generation
                && same_broker_config_values(restarted, original),
            "Broker restart did not begin a fresh generation epoch from the original configuration",
        )?;
        self.broker_restart_generation = Some(restarted.generation);
        ensure(
            matches!(
                (
                    self.broker_change_generation,
                    self.broker_repeat_generation,
                    self.broker_restore_generation,
                    self.broker_restart_generation,
                ),
                (Some(change), Some(repeat), Some(restore), Some(restart))
                    if original.generation < change
                        && change < repeat
                        && repeat < restore
                        && restart == original.generation
            ),
            "Broker generation evidence did not prove monotonic progress within the original epoch and reset on restart",
        )?;
        let recovered = self
            .invoke(
                UPSERT_TOPIC_TOOL,
                topic_args(&self.topic, true, "recovery-dry-01"),
                ControlOperation::TopicUpsert,
                AuditMode::DryRun,
                AuditResult::Planned,
            )
            .await?;
        ensure(
            recovered["status"] == "planned",
            "Broker restart did not restore mutation readiness",
        )
    }

    async fn restart_control_and_prove_audit_recovery(&mut self) -> E2eResult<()> {
        self.stop_control().await?;
        let (control, mcp) = ControlInstance::start(&self.control_config).await?;
        self.control = Some(control);
        self.mcp = Some(mcp);
        let recovered = self
            .invoke(
                UPSERT_CONSUMER_GROUP_TOOL,
                group_args(&self.group, true, "control-recover1"),
                ControlOperation::ConsumerGroupUpsert,
                AuditMode::DryRun,
                AuditResult::Planned,
            )
            .await?;
        ensure(
            recovered["status"] == "planned",
            "Control restart did not recover the audit file",
        )
    }

    async fn invoke(
        &mut self,
        tool: &str,
        arguments: Value,
        operation: ControlOperation,
        mode: AuditMode,
        result: AuditResult,
    ) -> E2eResult<Value> {
        let id = self.next_id();
        let response = self
            .mcp
            .as_mut()
            .ok_or_else(|| E2eError::new("MCP client is unavailable"))?
            .call(id, tool, arguments)
            .await?;
        self.expected_audit.push(ExpectedAudit {
            operation,
            mode,
            result,
            error_code: None,
        });
        Ok(response)
    }

    async fn invoke_error(
        &mut self,
        tool: &str,
        arguments: Value,
        operation: ControlOperation,
        mode: AuditMode,
        error_code: ControlErrorCode,
    ) -> E2eResult<Value> {
        let id = self.next_id();
        let response = self
            .mcp
            .as_mut()
            .ok_or_else(|| E2eError::new("MCP client is unavailable"))?
            .call_expect_error(id, tool, arguments)
            .await?;
        self.expected_audit.push(ExpectedAudit {
            operation,
            mode,
            result: AuditResult::Failed,
            error_code: Some(error_code),
        });
        Ok(response)
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_request_id;
        self.next_request_id += 1;
        id
    }

    fn fixture_mut(&mut self) -> E2eResult<&mut FixtureAdmin> {
        self.fixture
            .as_mut()
            .ok_or_else(|| E2eError::new("fixture Admin is unavailable"))
    }

    pub async fn cleanup(&mut self) -> E2eResult<CleanupEvidence> {
        if let Some(outcome) = self.cleanup_outcome.clone() {
            return outcome.map_err(E2eError::new);
        }

        let mut failures = Vec::new();
        let broker_restore_was_required = self.broker_restoration.is_required();
        let request_mode_restore_was_required = self.request_mode_restoration.is_required();
        let needs_live_broker = broker_restore_was_required
            || request_mode_restore_was_required
            || self.topic_created
            || self.group_created;
        if needs_live_broker {
            if self.cluster.ensure_broker_running().await.is_err() {
                failures.push("Broker was unavailable during cleanup".to_owned());
            }
            if let Some(fixture) = self.fixture.as_mut() {
                if fixture.wait_for_broker(&self.topic).await.is_err() {
                    failures.push("Broker did not become ready for cleanup".to_owned());
                }
            }
        }

        let request_mode_restored = self.restore_request_mode().await.unwrap_or(false);
        if request_mode_restore_was_required && !request_mode_restored {
            failures.push("Consumer request mode restoration failed".to_owned());
        }
        let broker_config_restored = self
            .restore_broker_config(None, "broker-cleanup-1")
            .await
            .unwrap_or(false);
        if broker_restore_was_required && !broker_config_restored {
            failures.push("Broker configuration restoration failed".to_owned());
        }

        if let Some(fixture) = self.fixture.as_mut() {
            if fixture
                .cleanup_resources(&self.topic, &self.group, self.topic_created, self.group_created)
                .await
                .is_err()
            {
                failures.push("typed Admin resource cleanup failed".to_owned());
            } else {
                self.topic_created = false;
                self.group_created = false;
            }
        }
        if let Some(mut fixture) = self.fixture.take() {
            if fixture.shutdown().await.is_err() {
                failures.push("fixture Admin shutdown failed".to_owned());
            }
        }
        if self.stop_control().await.is_err() {
            failures.push("Control shutdown failed".to_owned());
        }
        if verify_audit(&self.audit_path, &self.expected_audit).is_err() {
            failures.push("durable audit pair verification failed".to_owned());
        }
        if self.cluster.stop_all().await.is_err() {
            failures.push("cluster child shutdown failed".to_owned());
        }
        let children_reaped = self.cluster.all_reaped();
        if !children_reaped {
            failures.push("an owned cluster child was not reaped".to_owned());
        }

        let root = self.root.clone();
        if let Some(temp) = self.temp.take() {
            if temp.close().is_err() {
                failures.push("ephemeral cluster root removal failed".to_owned());
            }
        }
        let cluster_root_removed = !root.exists();
        if !cluster_root_removed {
            failures.push("ephemeral cluster root remains on disk".to_owned());
        }
        let outcome = if failures.is_empty() {
            Ok(CleanupEvidence {
                cluster_root_removed,
                children_reaped,
                broker_config_restored: !broker_restore_was_required || broker_config_restored,
                consumer_request_mode_restored: !request_mode_restore_was_required || request_mode_restored,
            })
        } else {
            Err(failures.join("; "))
        };
        self.cleanup_outcome = Some(outcome.clone());
        outcome.map_err(E2eError::new)
    }

    async fn restore_broker_config(&mut self, expected_changed: Option<bool>, request_key: &str) -> E2eResult<bool> {
        if !self.broker_restoration.is_required() {
            return Ok(true);
        }
        let Some(original) = self.original_broker_state else {
            return Ok(false);
        };
        let before = self.fixture_mut()?.broker_state().await?;
        let changed = !same_broker_config_values(before, original);
        if let Some(expected_changed) = expected_changed {
            ensure(
                changed == expected_changed,
                "Broker restoration pre-state did not have the expected change semantics",
            )?;
            ensure(
                self.broker_repeat_generation == Some(before.generation),
                "Broker restoration did not start from the saved repeat generation",
            )?;
        }
        self.broker_restoration.before_execute();
        let response = self
            .invoke(
                PATCH_BROKER_CONFIG_TOOL,
                broker_args(original.trace_topic_enable, false, request_key),
                ControlOperation::BrokerConfigPatch,
                AuditMode::Execute,
                AuditResult::Applied,
            )
            .await?;
        ensure(
            response["before"][BROKER] == broker_state_json(before),
            "Broker restoration response did not begin from the exact observed state",
        )?;
        let restored_generation = broker_generation(&response, "after")?;
        ensure(
            restored_generation > before.generation,
            "Broker restoration generation did not advance within its process epoch",
        )?;
        let target = single_broker_target(&response)?;
        ensure(
            target["changed"] == changed && target["applied"] == true,
            "Broker restoration target did not report the expected change semantics",
        )?;
        let mut expected = original;
        expected.generation = restored_generation;
        ensure(
            response["after"][BROKER] == broker_state_json(expected),
            "Broker restoration response did not verify all six original fields",
        )?;
        let restored = self
            .fixture
            .as_mut()
            .ok_or_else(|| E2eError::new("fixture Admin is unavailable"))?
            .broker_state()
            .await?;
        ensure(
            restored == expected,
            "typed fixture did not observe the exact Broker restoration generation and fields",
        )?;
        self.broker_restore_generation = Some(restored_generation);
        self.broker_restoration.complete();
        Ok(true)
    }

    async fn restore_request_mode(&mut self) -> E2eResult<bool> {
        if !self.request_mode_restoration.is_required() {
            return Ok(true);
        }
        let Some(baseline) = self.request_mode_baseline else {
            return Ok(false);
        };
        let (mode, share) = match baseline.mode {
            RequestMode::Pull => ("pull", baseline.pop_share_queue_num),
            RequestMode::Pop => ("pop", baseline.pop_share_queue_num),
        };
        self.request_mode_restoration.before_execute();
        let response = self
            .invoke(
                SET_CONSUMER_REQUEST_MODE_TOOL,
                request_mode_args(&self.topic, &self.group, mode, share, false, "mode-restore-01"),
                ControlOperation::ConsumerRequestMode,
                AuditMode::Execute,
                AuditResult::Applied,
            )
            .await?;
        let response_restored =
            response["after"][BROKER]["mode"] == mode && response["after"][BROKER]["pop_share_queue_num"] == share;
        let restored = self
            .fixture
            .as_mut()
            .ok_or_else(|| E2eError::new("fixture Admin is unavailable"))?
            .request_mode(&self.topic, &self.group)
            .await?;
        let verified = response_restored && restored.as_ref() == Some(&baseline);
        if verified {
            self.request_mode_restoration.complete();
        }
        Ok(verified)
    }

    async fn stop_control(&mut self) -> E2eResult<()> {
        let redaction_result = self.mcp.as_ref().map_or(Ok(()), |mcp| {
            let mut private_surfaces = vec![
                self.root.to_string_lossy().into_owned(),
                "127.0.0.1".to_owned(),
                MESSAGE_BODY.to_owned(),
            ];
            private_surfaces.extend(self.tls_private_key_markers.iter().cloned());
            private_surfaces.extend(
                [
                    self.ports.namesrv.value(),
                    self.ports.namesrv_health.value(),
                    self.ports.broker.value(),
                    self.ports.broker_fast.value(),
                    self.ports.broker_ha.value(),
                    self.ports.broker_health.value(),
                    self.ports.control_https.value(),
                ]
                .into_iter()
                .map(|port| port.to_string()),
            );
            mcp.assert_public_surfaces_redacted(protocol::operator(), protocol::reason(), &private_surfaces)
        });
        self.mcp.take();
        if let Some(mut control) = self.control.take() {
            control.stop().await?;
        }
        redaction_result
    }
}

impl Drop for E2eHarness {
    fn drop(&mut self) {
        self.mcp.take();
        self.control.take();
        self.fixture.take();
    }
}

struct FixtureAdmin {
    runtime: rocketmq_runtime::RuntimeContext,
    session: Option<MutationAdminSession>,
    broker_port: u16,
}

impl FixtureAdmin {
    async fn start(namesrv_port: u16, broker_port: u16) -> E2eResult<Self> {
        let runtime = rocketmq_runtime::RuntimeContext::from_current("mcp-control-real-cluster-fixture");
        let client_runtime = rocketmq_admin_core::mutation_client_adapter::create_mutation_client_runtime(
            runtime.service_context("fixture-client"),
        )
        .map_err(|_| E2eError::new("create fixture mutation client runtime failed"))?;
        let session = MutationAdminBuilder::new(client_runtime)
            .namesrv_addr(format!("127.0.0.1:{namesrv_port}"))
            .admin_group("mcp-control-e2e-fixture")
            .instance_name("mcp-control-e2e-fixture")
            .timeout_millis(5_000)
            .build_and_start()
            .await
            .map_err(|_| E2eError::new("start fixture mutation Admin failed"))?;
        Ok(Self {
            runtime,
            session: Some(session),
            broker_port,
        })
    }

    fn session(&mut self) -> E2eResult<&mut MutationAdminSession> {
        self.session
            .as_mut()
            .ok_or_else(|| E2eError::new("fixture mutation Admin is shut down"))
    }

    async fn wait_for_broker(&mut self, topic: &str) -> E2eResult<()> {
        let deadline = tokio::time::Instant::now() + REGISTRATION_TIMEOUT;
        loop {
            let failure =
                match SupervisedMutationAdmin::preflight_broker_config_target(self.session()?, BROKER_CLUSTER, BROKER)
                    .await
                {
                    Ok(_) => {
                        match SupervisedMutationAdmin::preflight_topic_targets(
                            self.session()?,
                            &TopicMutationPreflightRequest {
                                cluster: BROKER_CLUSTER.to_owned(),
                                topic: topic.to_owned(),
                                replacement: TopicReplacement {
                                    read_queue_nums: 1,
                                    write_queue_nums: 1,
                                    perm: 6,
                                    order: false,
                                    message_type: TopicMessageType::Normal,
                                },
                            },
                            &[BROKER.to_owned()],
                        )
                        .await
                        {
                            Ok(_) => return Ok(()),
                            Err(error) => safe_admin_error(&error),
                        }
                    }
                    Err(error) => safe_admin_error(&error),
                };
            if tokio::time::Instant::now() >= deadline {
                return Err(E2eError::new(format!(
                    "Broker did not register a usable mutation topology before deadline: {failure}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }

    async fn publish_and_seed_progress(&mut self, topic: &str, group: &str) -> E2eResult<()> {
        let result = TopicMutationAdmin::send_topic_test_message(
            self.session()?,
            &TopicSendRequest {
                topic: topic.to_owned(),
                key: "mcp-e2e-key".to_owned(),
                tag: "McpE2e".to_owned(),
                message_body: MESSAGE_BODY.to_owned(),
                trace_enabled: false,
            },
        )
        .await
        .map_err(|_| E2eError::new("typed fixture message publish failed"))?;
        ensure(result.queue_offset == 0, "unexpected first fixture queue offset")?;
        TopicMutationAdmin::reset_topic_consumer_offset(
            self.session()?,
            &ResetTopicConsumerOffsetRequest {
                consumer_group: group.to_owned(),
                topic: topic.to_owned(),
                reset_timestamp: current_millis()?.saturating_add(1_000),
                force: true,
            },
        )
        .await
        .map_err(|_| E2eError::new("typed fixture Consumer offset seed failed"))?;
        Ok(())
    }

    async fn offset_rows(&mut self, topic: &str, group: &str, timestamp: i64) -> E2eResult<Vec<(i32, i64, i64)>> {
        let plan = SupervisedMutationAdmin::preview_offset_reset(
            self.session()?,
            &rocketmq_admin_core::core::supervised_mutation::OffsetResetPreviewRequest {
                cluster: BROKER_CLUSTER.to_owned(),
                topic: topic.to_owned(),
                consumer_group: group.to_owned(),
                timestamp,
                force: true,
            },
        )
        .await
        .map_err(|_| E2eError::new("typed fixture offset read failed"))?;
        Ok(plan
            .rows()
            .into_iter()
            .map(|row| (row.queue_id, row.current_offset, row.planned_offset))
            .collect())
    }

    async fn broker_state(&mut self) -> E2eResult<BrokerMutationConfigState> {
        let plan = SupervisedMutationAdmin::preflight_broker_config_target(self.session()?, BROKER_CLUSTER, BROKER)
            .await
            .map_err(|_| E2eError::new("typed fixture Broker config read failed"))?;
        plan.targets()
            .into_iter()
            .find(|target| target.broker_name == BROKER)
            .map(|target| target.state)
            .ok_or_else(|| E2eError::new("typed fixture Broker config target was absent"))
    }

    async fn request_mode(&mut self, topic: &str, group: &str) -> E2eResult<Option<RequestModeValue>> {
        let plan = SupervisedMutationAdmin::preflight_request_mode(
            self.session()?,
            &RequestModePreflightRequest {
                cluster: BROKER_CLUSTER.to_owned(),
                topic: topic.to_owned(),
                consumer_group: group.to_owned(),
                replacement: RequestModeValue {
                    mode: RequestMode::Pull,
                    pop_share_queue_num: 0,
                },
            },
        )
        .await
        .map_err(|_| E2eError::new("typed fixture request-mode read failed"))?;
        plan.targets()
            .into_iter()
            .find(|(broker, _)| broker == BROKER)
            .map(|(_, value)| value)
            .ok_or_else(|| E2eError::new("typed fixture request-mode target was absent"))
    }

    async fn cleanup_resources(
        &mut self,
        topic: &str,
        group: &str,
        topic_created: bool,
        group_created: bool,
    ) -> E2eResult<()> {
        let broker_addr = format!("127.0.0.1:{}", self.broker_port);
        if group_created {
            ConsumerMutationAdmin::delete_subscription_groups(
                self.session()?,
                &DeleteSubscriptionGroupsRequest::try_new(broker_addr, vec![group.to_owned()], true)
                    .map_err(|_| E2eError::new("build typed Consumer Group cleanup request failed"))?,
            )
            .await
            .map_err(|_| E2eError::new("typed Consumer Group cleanup failed"))?;
        }
        if topic_created {
            TopicMutationAdmin::delete_topic(
                self.session()?,
                &DeleteTopicAdminRequest {
                    topic: topic.to_owned(),
                    cluster_name: Some(BROKER_CLUSTER.to_owned()),
                    broker_name: None,
                },
            )
            .await
            .map_err(|_| E2eError::new("typed Topic cleanup failed"))?;
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> E2eResult<()> {
        let session_result = match self.session.take() {
            Some(mut session) => tokio::time::timeout(FIXTURE_STOP_TIMEOUT, session.shutdown())
                .await
                .map_err(|_| E2eError::new("fixture Admin shutdown deadline expired")),
            None => Ok(()),
        };
        let runtime_result = self
            .runtime
            .shutdown_tasks(FIXTURE_STOP_TIMEOUT)
            .await
            .assert_no_task_leak()
            .map_err(E2eError::new);
        session_result.and(runtime_result)
    }
}

impl Drop for FixtureAdmin {
    fn drop(&mut self) {
        self.session.take();
        let _ = self.runtime.shutdown_tasks_now();
    }
}

fn same_broker_config_values(left: BrokerMutationConfigState, right: BrokerMutationConfigState) -> bool {
    left.auto_create_topic_enable == right.auto_create_topic_enable
        && left.auto_create_subscription_group == right.auto_create_subscription_group
        && left.broker_permission == right.broker_permission
        && left.default_topic_queue_nums == right.default_topic_queue_nums
        && left.message_index_enable == right.message_index_enable
        && left.trace_topic_enable == right.trace_topic_enable
}

fn broker_state_json(state: BrokerMutationConfigState) -> Value {
    serde_json::json!({
        "generation": state.generation,
        "autoCreateTopicEnable": state.auto_create_topic_enable,
        "autoCreateSubscriptionGroup": state.auto_create_subscription_group,
        "brokerPermission": state.broker_permission,
        "defaultTopicQueueNums": state.default_topic_queue_nums,
        "messageIndexEnable": state.message_index_enable,
        "traceTopicEnable": state.trace_topic_enable,
    })
}

fn broker_generation(response: &Value, section: &str) -> E2eResult<u64> {
    response[section][BROKER]["generation"]
        .as_u64()
        .ok_or_else(|| E2eError::new(format!("Broker {section} state omitted its generation")))
}

fn single_broker_target(response: &Value) -> E2eResult<&Value> {
    let targets = response["targets"]
        .as_array()
        .ok_or_else(|| E2eError::new("Broker response omitted its target array"))?;
    ensure(
        targets.len() == 1 && targets[0]["broker_name"] == BROKER,
        "Broker response did not contain exactly the selected Broker target",
    )?;
    Ok(&targets[0])
}

fn topic_args(topic: &str, dry_run: bool, request_key: &str) -> Value {
    serde_json::json!({
        "schema_version": ARGUMENT_SCHEMA,
        "cluster": CLUSTER,
        "topic": topic,
        "broker_names": [BROKER],
        "read_queue_nums": 1,
        "write_queue_nums": 1,
        "perm": 6,
        "order": false,
        "message_type": "NORMAL",
        "dry_run": dry_run,
        "confirm": !dry_run,
        "reason": protocol::reason(),
        "request_key": request_key
    })
}

fn group_args(group: &str, dry_run: bool, request_key: &str) -> Value {
    serde_json::json!({
        "schema_version": ARGUMENT_SCHEMA,
        "cluster": CLUSTER,
        "consumer_group": group,
        "broker_names": [BROKER],
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
        "consume_timeout_minute": 15,
        "dry_run": dry_run,
        "confirm": !dry_run,
        "reason": protocol::reason(),
        "request_key": request_key
    })
}

fn offset_args(topic: &str, group: &str, timestamp: &str, dry_run: bool, request_key: &str) -> Value {
    serde_json::json!({
        "schema_version": ARGUMENT_SCHEMA,
        "cluster": CLUSTER,
        "topic": topic,
        "consumer_group": group,
        "timestamp": timestamp,
        "force": true,
        "dry_run": dry_run,
        "confirm": !dry_run,
        "reason": protocol::reason(),
        "request_key": request_key
    })
}

fn broker_args(trace_topic_enable: bool, dry_run: bool, request_key: &str) -> Value {
    serde_json::json!({
        "schema_version": ARGUMENT_SCHEMA,
        "cluster": CLUSTER,
        "broker_name": BROKER,
        "properties": {"traceTopicEnable": trace_topic_enable.to_string()},
        "dry_run": dry_run,
        "confirm": !dry_run,
        "reason": protocol::reason(),
        "request_key": request_key
    })
}

fn request_mode_args(topic: &str, group: &str, mode: &str, share: i32, dry_run: bool, key: &str) -> Value {
    serde_json::json!({
        "schema_version": ARGUMENT_SCHEMA,
        "cluster": CLUSTER,
        "topic": topic,
        "consumer_group": group,
        "mode": mode,
        "pop_share_queue_num": share,
        "timeout_millis": 5000,
        "dry_run": dry_run,
        "confirm": !dry_run,
        "reason": protocol::reason(),
        "request_key": key
    })
}

fn write_namesrv_config(root: &Path, ports: &PortSet) -> E2eResult<PathBuf> {
    let home = protocol::path_for_toml(&root.join("namesrv"));
    let path = root.join("namesrv.toml");
    let content = format!(
        "rocketmqHome = \"{home}\"\nkvConfigPath = \"{home}/kvConfig.json\"\nconfigStorePath = \"{home}/namesrv.properties\"\nneedWaitForService = false\nlistenPort = {}\nbindAddress = \"127.0.0.1\"\n",
        ports.namesrv.value()
    );
    std::fs::create_dir_all(root.join("namesrv")).e2e("create NameServer root")?;
    std::fs::write(&path, content).e2e("write NameServer E2E configuration")?;
    Ok(path)
}

fn write_broker_config(root: &Path, ports: &PortSet) -> E2eResult<PathBuf> {
    let store = protocol::path_for_toml(&root.join("broker"));
    let path = root.join("broker.toml");
    let content = format!(
        "[broker]\nlistenPort = {}\nbrokerIp1 = \"127.0.0.1\"\nstorePathRootDir = \"{store}\"\nnamesrvAddr = \"127.0.0.1:{}\"\nautoCreateTopicEnable = false\nautoCreateSubscriptionGroup = true\ntraceTopicEnable = false\n\n[broker.brokerIdentity]\nbrokerName = \"{BROKER}\"\nbrokerClusterName = \"{BROKER_CLUSTER}\"\nbrokerId = 0\n\n[broker.brokerServerConfig]\nbindAddress = \"127.0.0.1\"\n\n[store]\nstorePathRootDir = \"{store}\"\nhaListenAddress = \"127.0.0.1\"\nhaListenPort = {}\nmappedFileSizeCommitLog = 1048576\nmappedFileSizeConsumeQueue = 6000\nmappedFileSizeConsumeQueueExt = 65536\nmaxHashSlotNum = 1000\nmaxIndexNum = 4000\ntimerWheelEnable = false\n",
        ports.broker.value(),
        ports.namesrv.value(),
        ports.broker_ha.value()
    );
    std::fs::create_dir_all(root.join("broker")).e2e("create Broker root")?;
    std::fs::write(&path, content).e2e("write Broker E2E configuration")?;
    Ok(path)
}

fn write_control_config(root: &Path, ports: &PortSet, audit_path: &Path) -> E2eResult<ControlConfigMaterial> {
    let tls = protocol::write_tls_material(root)?;
    let audit_path = protocol::path_for_toml(audit_path);
    let path = root.join("control.toml");
    let content = format!(
        "[server]\nbind = \"127.0.0.1:{}\"\nendpoint = \"/mcp\"\npublic_base_url = \"https://{}\"\n\n[server.tls]\ncert_path = \"{cert_path}\"\nkey_path = \"{key_path}\"\n\n[oauth]\nissuer = \"https://issuer.example.test\"\naudience = \"rocketmq-mcp-control\"\njwks_url = \"https://issuer.example.test/jwks\"\n\n[mutations]\nmutations_enabled = true\ndry_run = true\nallowed_operations = [\"topic_upsert\", \"consumer_group_upsert\", \"consumer_offset_reset\", \"broker_config_patch\", \"consumer_request_mode\"]\nallowed_clusters = [\"{CLUSTER}\"]\noperation_timeout_seconds = 8\n\n[[clusters]]\nname = \"{CLUSTER}\"\nnamesrv_addr = \"127.0.0.1:{}\"\nuse_tls = false\n\n[audit]\npath = \"{audit_path}\"\ncapacity = 256\nmax_record_bytes = 4096\n",
        ports.control_https.value(),
        protocol::public_host(),
        ports.namesrv.value(),
        cert_path = tls.certificate_path,
        key_path = tls.private_key_path,
    );
    std::fs::write(&path, content).e2e("write Control E2E configuration")?;
    Ok(ControlConfigMaterial {
        path,
        tls_private_key_markers: tls.private_key_markers,
    })
}

fn verify_audit(path: &Path, expected: &[ExpectedAudit]) -> E2eResult<()> {
    let text = std::fs::read_to_string(path).e2e("read durable E2E audit file")?;
    let records = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<AuditRecord>(line).e2e("decode durable E2E audit record"))
        .collect::<E2eResult<Vec<_>>>()?;
    ensure_audit_cardinality(expected.len(), records.len())?;
    let mut invocations = BTreeMap::new();
    for record in records {
        invocations
            .entry(record.invocation_id)
            .or_insert_with(Vec::new)
            .push(record);
    }
    ensure(
        invocations.len() == EXPECTED_CALLS,
        "audit invocation count was not exactly 22",
    )?;
    let mut observed = Vec::with_capacity(invocations.len());
    for pair in invocations.values() {
        ensure(pair.len() == 2, "an E2E audit invocation was not a pair")?;
        let started = &pair[0];
        let terminal = &pair[1];
        ensure(
            started.schema_version == AuditSchemaVersion::V2,
            "audit start was not schema v2",
        )?;
        ensure(
            started.event == AuditEvent::Started,
            "audit pair did not begin with started",
        )?;
        ensure(
            started.result == AuditResult::Started,
            "audit start result was not started",
        )?;
        let terminal_event = match terminal.result {
            AuditResult::Planned | AuditResult::Applied => AuditEvent::Completed,
            AuditResult::Partial | AuditResult::Conflict | AuditResult::Failed | AuditResult::Started => {
                AuditEvent::Failed
            }
        };
        ensure(
            terminal.event == terminal_event,
            "audit terminal event did not match its result",
        )?;
        ensure(
            started.operation == terminal.operation,
            "audit operation changed within its pair",
        )?;
        ensure(started.mode == terminal.mode, "audit mode changed within its pair")?;
        ensure(
            started.cluster.as_str() == CLUSTER,
            "audit start cluster mismatched the call",
        )?;
        ensure(
            terminal.cluster.as_str() == CLUSTER,
            "audit terminal cluster mismatched the call",
        )?;
        ensure(
            started.operator.as_deref() == Some(protocol::operator())
                && terminal.operator.as_deref() == Some(protocol::operator()),
            "audit operator evidence was absent",
        )?;
        ensure(
            started.reason.as_deref() == Some(protocol::reason())
                && terminal.reason.as_deref() == Some(protocol::reason()),
            "audit reason evidence was absent",
        )?;
        ensure(started.error_code.is_none(), "audit start carried an error code")?;
        ensure(
            started.duration_millis.is_none(),
            "audit start carried a terminal duration",
        )?;
        ensure(terminal.duration_millis.is_some(), "audit terminal duration was absent")?;
        ensure(
            terminal.sequence > started.sequence,
            "audit terminal did not follow its start sequence",
        )?;
        observed.push(ExpectedAudit {
            operation: terminal.operation,
            mode: terminal.mode,
            result: terminal.result,
            error_code: terminal.error_code,
        });
    }
    let mut unmatched = expected.to_vec();
    for actual in observed {
        let Some(index) = unmatched.iter().position(|expected| *expected == actual) else {
            return Err(E2eError::new(
                "durable audit contained an unexpected terminal operation, mode, result, or code",
            ));
        };
        unmatched.swap_remove(index);
    }
    ensure(unmatched.is_empty(), "durable audit omitted an expected invocation")?;
    Ok(())
}

fn ensure_audit_cardinality(expected_calls: usize, observed_records: usize) -> E2eResult<()> {
    ensure(
        expected_calls == EXPECTED_CALLS,
        format!("expected exactly {EXPECTED_CALLS} E2E calls, observed {expected_calls}"),
    )?;
    ensure(
        observed_records == EXPECTED_AUDIT_RECORDS,
        format!("expected exactly {EXPECTED_AUDIT_RECORDS} audit records, observed {observed_records}"),
    )
}

fn millis_timestamp(millis: u64) -> E2eResult<String> {
    let millis = i64::try_from(millis).e2e("convert E2E timestamp")?;
    chrono::DateTime::<Utc>::from_timestamp_millis(millis)
        .map(|time| time.to_rfc3339_opts(SecondsFormat::Millis, true))
        .ok_or_else(|| E2eError::new("E2E timestamp was outside RFC3339 range"))
}

fn current_millis() -> E2eResult<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .e2e("read E2E clock")?
        .as_millis();
    u64::try_from(millis).e2e("convert E2E clock")
}

fn unique_id() -> E2eResult<String> {
    Ok(format!("{}_{}", std::process::id(), current_millis()?))
}

fn safe_admin_error(error: &AdminError) -> String {
    match error {
        AdminError::InvalidArgument { field, .. } => format!("invalid_argument:{field}"),
        AdminError::NotFound { resource, .. } => format!("not_found:{resource}"),
        AdminError::Backend {
            operation,
            code,
            http_status,
            retryable,
            ..
        } => format!(
            "backend:{operation}:code={}:http={}:retryable={retryable}",
            code.as_deref().unwrap_or("none"),
            http_status.map_or_else(|| "none".to_owned(), |status| status.to_string())
        ),
        AdminError::SessionClosed => "session_closed".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::ensure_audit_cardinality;
    use super::RestorationState;

    #[test]
    fn audit_cardinality_cannot_adapt_to_missing_calls_or_records() {
        assert!(ensure_audit_cardinality(22, 44).is_ok());
        assert!(ensure_audit_cardinality(21, 42).is_err());
        assert!(ensure_audit_cardinality(22, 42).is_err());
    }

    #[test]
    fn restoration_state_survives_execute_error_and_clears_only_after_verified_restore() {
        let mut state = RestorationState::default();
        assert!(!state.is_required());

        state.before_execute();
        let invoke_result = Result::<(), ()>::Err(());
        assert!(invoke_result.is_err());
        assert!(state.is_required());

        state.before_execute();
        assert!(state.is_required());
        state.complete();
        assert!(!state.is_required());
    }
}
