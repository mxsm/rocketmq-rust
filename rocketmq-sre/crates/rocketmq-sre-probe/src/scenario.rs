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

//! Five bounded Phase 2 synthetic scenarios and their lifecycle runner.

use std::time::Duration;
use std::time::Instant;

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

use crate::ProbePlan;
use crate::cleanup::ProbeCleanupResult;
use crate::consumer::ProbeConsumeObservation;
use crate::consumer::ProbeConsumerMode;
use crate::producer::ProbeMessageBatch;
use crate::producer::ProbeSendMode;
use crate::producer::ProbeSendObservation;

/// Phase 2 probe scenario catalog.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProbeScenario {
    SendConsumeAck,
    ProxyPath,
    TransactionCommit,
    DelayedTimer,
    PopAck,
}

impl ProbeScenario {
    /// Stable CLI/config identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SendConsumeAck => "send-consume-ack",
            Self::ProxyPath => "proxy-path",
            Self::TransactionCommit => "transaction-commit",
            Self::DelayedTimer => "delayed-timer",
            Self::PopAck => "pop-ack",
        }
    }

    /// All Phase 2 probe scenarios in stable order.
    #[must_use]
    pub const fn all() -> [Self; 5] {
        [
            Self::SendConsumeAck,
            Self::ProxyPath,
            Self::TransactionCommit,
            Self::DelayedTimer,
            Self::PopAck,
        ]
    }

    const fn modes(self) -> (ProbeSendMode, ProbeConsumerMode) {
        match self {
            Self::SendConsumeAck => (ProbeSendMode::Standard, ProbeConsumerMode::PushAck),
            Self::ProxyPath => (ProbeSendMode::ProxyPath, ProbeConsumerMode::PushAck),
            Self::TransactionCommit => (ProbeSendMode::TransactionCommit, ProbeConsumerMode::PushAck),
            Self::DelayedTimer => (ProbeSendMode::DelayedTimer, ProbeConsumerMode::PushAck),
            Self::PopAck => (ProbeSendMode::PopSeed, ProbeConsumerMode::PreprovisionedPopAck),
        }
    }
}

impl std::str::FromStr for ProbeScenario {
    type Err = ProbeScenarioParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "send-consume-ack" => Ok(Self::SendConsumeAck),
            "proxy-path" => Ok(Self::ProxyPath),
            "transaction-commit" => Ok(Self::TransactionCommit),
            "delayed-timer" => Ok(Self::DelayedTimer),
            "pop-ack" => Ok(Self::PopAck),
            _ => Err(ProbeScenarioParseError),
        }
    }
}

/// Invalid scenario identifier.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("unknown probe scenario")]
pub struct ProbeScenarioParseError;

/// Stable scenario completion status.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeRunStatus {
    Succeeded,
    Failed,
    TimedOut,
    BudgetExceeded,
}

/// One bounded stage timing.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProbeStage {
    pub name: String,
    pub latency_millis: u64,
    pub succeeded: bool,
}

/// Persistable scenario result. It deliberately contains no message body.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProbeRunResult {
    pub probe_id: String,
    pub scenario: ProbeScenario,
    pub status: ProbeRunStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub trace_id: String,
    pub stages: Vec<ProbeStage>,
    pub sent_messages: u16,
    pub received_messages: u16,
    pub acknowledged_messages: u16,
    pub error_code: Option<String>,
    pub cleanup: ProbeCleanupResult,
}

/// Sanitized driver failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("probe driver failed with code `{code}`")]
pub struct ProbeDriverError {
    pub code: String,
}

impl ProbeDriverError {
    /// Creates a stable, non-sensitive error.
    #[must_use]
    pub fn new(code: impl Into<String>) -> Self {
        Self { code: code.into() }
    }
}

/// Driver boundary used by the real RocketMQ adapter and deterministic tests.
#[allow(
    async_fn_in_trait,
    reason = "the probe driver is not dyn compatible and owns its lifecycle"
)]
pub trait ProbeDriver {
    /// Binds the consumer observation to this invocation's message-key prefix.
    fn set_expected_key_prefix(&mut self, _key_prefix: &str) {}

    async fn start_consumer(&mut self, plan: &ProbePlan, mode: ProbeConsumerMode) -> Result<(), ProbeDriverError>;

    async fn send(
        &mut self,
        plan: &ProbePlan,
        mode: ProbeSendMode,
        batch: &ProbeMessageBatch,
    ) -> Result<ProbeSendObservation, ProbeDriverError>;

    async fn await_acknowledgements(&mut self, expected: u16) -> Result<ProbeConsumeObservation, ProbeDriverError>;

    async fn cleanup(&mut self) -> ProbeCleanupResult;
}

/// Hard budget state checked before every message batch.
#[derive(Debug)]
pub struct ProbeBudget {
    message_limit: u16,
    payload_limit: u32,
    duration_limit: Duration,
    started: Instant,
    charged_messages: u16,
}

impl ProbeBudget {
    /// Creates a budget from an already validated plan.
    #[must_use]
    pub fn from_plan(plan: &ProbePlan) -> Self {
        Self {
            message_limit: plan.max_messages,
            payload_limit: plan.max_payload_bytes,
            duration_limit: Duration::from_secs(u64::from(plan.max_duration_seconds)),
            started: Instant::now(),
            charged_messages: 0,
        }
    }

    /// Charges a batch before it can reach a producer.
    ///
    /// # Errors
    ///
    /// Returns a stable limit code without clamping the requested work.
    pub fn charge(&mut self, messages: u16, payload_bytes: u32) -> Result<(), ProbeBudgetError> {
        if payload_bytes > self.payload_limit {
            return Err(ProbeBudgetError::Payload);
        }
        if self.charged_messages.saturating_add(messages) > self.message_limit {
            return Err(ProbeBudgetError::Messages);
        }
        if self.started.elapsed() >= self.duration_limit {
            return Err(ProbeBudgetError::Duration);
        }
        self.charged_messages += messages;
        Ok(())
    }
}

/// Hard probe budget exceeded.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProbeBudgetError {
    #[error("message budget exceeded")]
    Messages,
    #[error("payload budget exceeded")]
    Payload,
    #[error("duration budget exceeded")]
    Duration,
}

/// Runs one scenario, always requesting driver cleanup afterward.
pub async fn run_scenario<D>(driver: &mut D, plan: &ProbePlan, scenario: ProbeScenario) -> ProbeRunResult
where
    D: ProbeDriver,
{
    let started_at = Utc::now();
    let trace_id = format!("probe-{}", Uuid::new_v4().simple());
    let mut stages = Vec::new();
    let mut budget = ProbeBudget::from_plan(plan);
    let batch = ProbeMessageBatch {
        count: plan.max_messages,
        payload_bytes: plan.max_payload_bytes,
        minimum_interval_millis: 1_000_u64.div_ceil(u64::from(plan.max_messages_per_second)),
        tag: "rocketmq-sre-phase2",
        key_prefix: trace_id.clone(),
    };
    let (send_mode, consumer_mode) = scenario.modes();
    let timeout = Duration::from_secs(u64::from(plan.max_duration_seconds));

    let operation = async {
        plan.identity
            .validate()
            .map_err(|_| ProbeDriverError::new("resource_namespace_rejected"))?;
        budget.charge(batch.count, batch.payload_bytes).map_err(|error| {
            ProbeDriverError::new(match error {
                ProbeBudgetError::Messages => "message_budget_exceeded",
                ProbeBudgetError::Payload => "payload_budget_exceeded",
                ProbeBudgetError::Duration => "duration_budget_exceeded",
            })
        })?;
        driver.set_expected_key_prefix(&batch.key_prefix);

        record_stage(
            &mut stages,
            "consumer_start",
            driver.start_consumer(plan, consumer_mode),
        )
        .await?;
        let send = record_stage(&mut stages, "send", driver.send(plan, send_mode, &batch)).await?;
        let consumed = record_stage(
            &mut stages,
            "consume_ack",
            driver.await_acknowledgements(send.accepted_messages),
        )
        .await?;
        Ok::<_, ProbeDriverError>((send, consumed))
    };

    let (status, sent, received, acknowledged, error_code) = match tokio::time::timeout(timeout, operation).await {
        Ok(Ok((send, consumed))) => (
            ProbeRunStatus::Succeeded,
            send.accepted_messages,
            consumed.received_messages,
            consumed.acknowledged_messages,
            None,
        ),
        Ok(Err(error)) => {
            let status = if error.code.ends_with("budget_exceeded") {
                ProbeRunStatus::BudgetExceeded
            } else {
                ProbeRunStatus::Failed
            };
            (status, 0, 0, 0, Some(error.code))
        }
        Err(_) => (ProbeRunStatus::TimedOut, 0, 0, 0, Some("probe_timeout".to_owned())),
    };
    let cleanup = driver.cleanup().await;

    ProbeRunResult {
        probe_id: plan.run_id.to_string(),
        scenario,
        status,
        started_at,
        finished_at: Utc::now(),
        trace_id,
        stages,
        sent_messages: sent,
        received_messages: received,
        acknowledged_messages: acknowledged,
        error_code,
        cleanup,
    }
}

async fn record_stage<T>(
    stages: &mut Vec<ProbeStage>,
    name: &str,
    future: impl Future<Output = Result<T, ProbeDriverError>>,
) -> Result<T, ProbeDriverError> {
    let started = Instant::now();
    let result = future.await;
    stages.push(ProbeStage {
        name: name.to_owned(),
        latency_millis: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
        succeeded: result.is_ok(),
    });
    result
}

#[cfg(test)]
mod tests {
    use rocketmq_sre_contracts::ClusterId;
    use uuid::Uuid;

    use super::*;

    #[derive(Debug, Default)]
    struct FixtureDriver {
        send_modes: Vec<ProbeSendMode>,
        consumer_modes: Vec<ProbeConsumerMode>,
        cleanup_calls: usize,
        fail_send: bool,
        expected_key_prefix: Option<String>,
    }

    impl ProbeDriver for FixtureDriver {
        fn set_expected_key_prefix(&mut self, key_prefix: &str) {
            self.expected_key_prefix = Some(key_prefix.to_owned());
        }

        async fn start_consumer(&mut self, _plan: &ProbePlan, mode: ProbeConsumerMode) -> Result<(), ProbeDriverError> {
            self.consumer_modes.push(mode);
            Ok(())
        }

        async fn send(
            &mut self,
            _plan: &ProbePlan,
            mode: ProbeSendMode,
            batch: &ProbeMessageBatch,
        ) -> Result<ProbeSendObservation, ProbeDriverError> {
            self.send_modes.push(mode);
            if self.fail_send {
                return Err(ProbeDriverError::new("fixture_send_failed"));
            }
            Ok(ProbeSendObservation {
                accepted_messages: batch.count,
            })
        }

        async fn await_acknowledgements(&mut self, expected: u16) -> Result<ProbeConsumeObservation, ProbeDriverError> {
            Ok(ProbeConsumeObservation {
                received_messages: expected,
                acknowledged_messages: expected,
            })
        }

        async fn cleanup(&mut self) -> ProbeCleanupResult {
            self.cleanup_calls += 1;
            ProbeCleanupResult::default()
        }
    }

    fn plan() -> ProbePlan {
        crate::ProbeConfig {
            cluster_id: ClusterId::new(),
            max_messages: 3,
            max_messages_per_second: 20,
            max_payload_bytes: 32,
            max_duration_seconds: 2,
        }
        .plan(Uuid::nil())
        .expect("fixture plan")
    }

    #[tokio::test]
    async fn all_five_scenarios_are_bounded_and_acknowledged() {
        for scenario in ProbeScenario::all() {
            let mut driver = FixtureDriver::default();
            let result = run_scenario(&mut driver, &plan(), scenario).await;

            assert_eq!(result.status, ProbeRunStatus::Succeeded, "{scenario:?}");
            assert_eq!(result.sent_messages, 3);
            assert_eq!(result.received_messages, 3);
            assert_eq!(result.acknowledged_messages, 3);
            assert_eq!(driver.cleanup_calls, 1);
            assert_eq!(driver.send_modes, vec![scenario.modes().0]);
            assert_eq!(driver.consumer_modes, vec![scenario.modes().1]);
            assert_eq!(driver.expected_key_prefix.as_deref(), Some(result.trace_id.as_str()));
        }
    }

    #[tokio::test]
    async fn each_invocation_uses_a_unique_trace_key_prefix() {
        let first = run_scenario(&mut FixtureDriver::default(), &plan(), ProbeScenario::SendConsumeAck).await;
        let second = run_scenario(&mut FixtureDriver::default(), &plan(), ProbeScenario::SendConsumeAck).await;

        assert_ne!(first.trace_id, second.trace_id);
    }

    #[test]
    fn budget_rejects_work_before_it_can_be_clamped() {
        let plan = plan();
        let mut budget = ProbeBudget::from_plan(&plan);

        assert_eq!(budget.charge(4, 1), Err(ProbeBudgetError::Messages));
        assert_eq!(budget.charge(1, 33), Err(ProbeBudgetError::Payload));
        assert_eq!(budget.charge(3, 32), Ok(()));
        assert_eq!(budget.charge(1, 32), Err(ProbeBudgetError::Messages));
    }

    #[tokio::test]
    async fn driver_failure_still_runs_cleanup_and_returns_a_sanitized_code() {
        let mut driver = FixtureDriver {
            fail_send: true,
            ..FixtureDriver::default()
        };

        let result = run_scenario(&mut driver, &plan(), ProbeScenario::SendConsumeAck).await;

        assert_eq!(result.status, ProbeRunStatus::Failed);
        assert_eq!(result.error_code.as_deref(), Some("fixture_send_failed"));
        assert_eq!(driver.cleanup_calls, 1);
        assert_eq!(result.sent_messages, 0);
    }
}
