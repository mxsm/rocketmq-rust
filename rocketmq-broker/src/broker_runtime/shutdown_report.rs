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

use super::deferred::BrokerDeferredRegistryShutdownReport;
use super::*;
pub(super) struct BrokerRemotingServerReportReceiver {
    pub(super) name: &'static str,
    pub(super) receiver: oneshot::Receiver<Option<ShutdownReport>>,
}

#[derive(Debug, Clone)]
pub(crate) struct BrokerRemotingServerReport {
    pub(crate) name: &'static str,
    pub(crate) report: Option<ShutdownReport>,
}

#[derive(Debug, Clone)]
pub(crate) struct BrokerRemotingServerShutdownReport {
    pub(crate) task_group: ShutdownReport,
    pub(crate) server_reports: Vec<BrokerRemotingServerReport>,
}

impl BrokerRemotingServerShutdownReport {
    pub(crate) fn is_healthy(&self) -> bool {
        self.task_group.is_healthy()
            && self
                .server_reports
                .iter()
                .all(|server| server.report.as_ref().is_some_and(ShutdownReport::is_healthy))
    }

    pub(crate) fn has_timed_out(&self) -> bool {
        shutdown_report_has_timed_out(&self.task_group)
            || self
                .server_reports
                .iter()
                .any(|server| server.report.as_ref().is_some_and(shutdown_report_has_timed_out))
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BrokerBasicServiceShutdownReport {
    pub(crate) remoting: Option<BrokerRemotingServerShutdownReport>,
    pub(crate) request_processor: Option<ShutdownReport>,
    pub(crate) topic_config: Option<TopicConfigCoordinatorShutdownReport>,
    pub(crate) broker_outer_api: BrokerShutdownComponentReport,
    pub(crate) client_housekeeping: BrokerShutdownComponentReport,
    pub(crate) auth: BrokerShutdownComponentReport,
    pub(crate) service_tasks: BrokerShutdownComponentReport,
    pub(crate) observability: BrokerShutdownComponentReport,
    pub(crate) scheduled_tasks: BrokerShutdownComponentReport,
    pub(crate) message_store: BrokerShutdownComponentReport,
    pub(crate) deferred_services: BrokerShutdownComponentReport,
    pub(crate) deferred_producer_tasks: Option<ShutdownReport>,
    pub(crate) deferred_registry_shutdown: Option<BrokerDeferredRegistryShutdownReport>,
    pub(crate) deferred_resources: Option<BrokerDeferredResourceSnapshot>,
    pub(crate) transaction_services: BrokerShutdownComponentReport,
    pub(crate) fast_failure: BrokerShutdownComponentReport,
    pub(crate) topic_route: BrokerShutdownComponentReport,
    pub(crate) consumer_offset: BrokerShutdownComponentReport,
    pub(crate) subscription_group: BrokerShutdownComponentReport,
    pub(crate) metadata_io: BrokerShutdownComponentReport,
    pub(crate) deadline: BrokerShutdownComponentReport,
    pub(crate) unfinished_components: Vec<&'static str>,
}

#[derive(Clone)]
pub(super) struct BrokerShutdownProgress {
    unfinished: Arc<StdMutex<Vec<&'static str>>>,
    message_store_report: Arc<StdMutex<Option<BrokerShutdownComponentReport>>>,
}

impl BrokerShutdownProgress {
    pub(super) fn new() -> Self {
        Self {
            unfinished: Arc::new(StdMutex::new(
                BrokerBasicServiceShutdownReport::COMPONENT_NAMES
                    [..BrokerBasicServiceShutdownReport::COMPONENT_NAMES.len() - 1]
                    .to_vec(),
            )),
            message_store_report: Arc::new(StdMutex::new(None)),
        }
    }

    pub(super) fn complete(&self, name: &'static str) {
        self.unfinished
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|component| *component != name);
    }

    pub(super) fn unfinished(&self) -> Vec<&'static str> {
        self.unfinished
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(super) fn record_message_store_report(&self, report: BrokerShutdownComponentReport) {
        *self
            .message_store_report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(report);
    }

    pub(super) fn message_store_report(&self) -> Option<BrokerShutdownComponentReport> {
        self.message_store_report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BrokerShutdownComponentReport {
    pub(crate) name: &'static str,
    pub(crate) present: bool,
    pub(crate) healthy: bool,
    pub(crate) timed_out: bool,
    pub(crate) elapsed: Duration,
    pub(crate) error_kind: Option<&'static str>,
    pub(crate) detail: Option<String>,
}

impl Default for BrokerShutdownComponentReport {
    fn default() -> Self {
        Self::skipped("unknown")
    }
}

impl BrokerShutdownComponentReport {
    pub(crate) fn skipped(name: &'static str) -> Self {
        Self {
            name,
            present: false,
            healthy: true,
            timed_out: false,
            elapsed: Duration::ZERO,
            error_kind: None,
            detail: None,
        }
    }

    pub(crate) fn completed(name: &'static str, elapsed: Duration) -> Self {
        Self {
            name,
            present: true,
            healthy: true,
            timed_out: false,
            elapsed,
            error_kind: None,
            detail: None,
        }
    }

    pub(crate) fn completed_with_detail(name: &'static str, elapsed: Duration, detail: impl Into<String>) -> Self {
        Self {
            name,
            present: true,
            healthy: true,
            timed_out: false,
            elapsed,
            error_kind: None,
            detail: Some(detail.into()),
        }
    }

    pub(crate) fn unhealthy(name: &'static str, elapsed: Duration, detail: impl Into<String>) -> Self {
        Self {
            name,
            present: true,
            healthy: false,
            timed_out: false,
            elapsed,
            error_kind: None,
            detail: Some(detail.into()),
        }
    }

    pub(crate) fn timed_out(name: &'static str, elapsed: Duration) -> Self {
        Self {
            name,
            present: true,
            healthy: false,
            timed_out: true,
            elapsed,
            error_kind: None,
            detail: Some("timed out".to_string()),
        }
    }

    pub(crate) fn from_shutdown_report(name: &'static str, report: Option<&ShutdownReport>, elapsed: Duration) -> Self {
        let Some(report) = report else {
            return Self::skipped(name);
        };
        if shutdown_report_has_timed_out(report) {
            return Self::timed_out(name, elapsed);
        }
        if report.is_healthy() {
            Self::completed(name, elapsed)
        } else {
            Self::unhealthy(name, elapsed, report.to_json())
        }
    }

    pub(crate) fn from_telemetry_shutdown_report(
        report: &rocketmq_observability::TelemetryShutdownReport,
        elapsed: Duration,
    ) -> Self {
        let detail = report.to_json();
        if report.is_healthy() {
            Self::completed_with_detail("observability", elapsed, detail)
        } else {
            Self::unhealthy("observability", elapsed, detail)
        }
    }
}

pub(super) fn record_message_store_shutdown_outcome(
    shutdown_report: &mut BrokerBasicServiceShutdownReport,
    progress: &BrokerShutdownProgress,
    outcome: MessageStoreShutdownOutcome,
    elapsed: Duration,
) {
    match outcome {
        MessageStoreShutdownOutcome::Absent => {
            shutdown_report.message_store = BrokerShutdownComponentReport::skipped("message_store");
            progress.complete("message_store");
        }
        MessageStoreShutdownOutcome::Completed(store_report) => {
            shutdown_report.message_store = BrokerShutdownComponentReport::completed_with_detail(
                "message_store",
                elapsed,
                format!("{store_report:?}"),
            );
            progress.complete("message_store");
        }
        MessageStoreShutdownOutcome::Failed(error) => {
            let error_kind = error.kind().as_str();
            warn!(error_kind, error = %error, "Failed to shutdown message store durably");
            shutdown_report.message_store = BrokerShutdownComponentReport {
                name: "message_store",
                present: true,
                healthy: false,
                timed_out: false,
                elapsed,
                error_kind: Some(error_kind),
                detail: Some(error.to_string()),
            };
        }
        MessageStoreShutdownOutcome::TimedOut => {
            warn!("Timed out shutting down message store durably");
            shutdown_report.message_store = BrokerShutdownComponentReport::timed_out("message_store", elapsed);
        }
    }
    progress.record_message_store_report(shutdown_report.message_store.clone());
}

impl BrokerBasicServiceShutdownReport {
    const COMPONENT_NAMES: [&'static str; 18] = [
        "remoting",
        "request_processor",
        "topic_config",
        "broker_outer_api",
        "client_housekeeping",
        "auth",
        "service_tasks",
        "observability",
        "scheduled_tasks",
        "message_store",
        "deferred_services",
        "transaction_services",
        "fast_failure",
        "topic_route",
        "consumer_offset",
        "subscription_group",
        "metadata_io",
        "shutdown_deadline",
    ];

    pub(crate) fn is_healthy(&self) -> bool {
        self.unfinished_components.is_empty()
            && self
                .remoting
                .as_ref()
                .is_none_or(BrokerRemotingServerShutdownReport::is_healthy)
            && self.request_processor.as_ref().is_none_or(ShutdownReport::is_healthy)
            && self
                .topic_config
                .as_ref()
                .is_none_or(TopicConfigCoordinatorShutdownReport::is_healthy)
            && self.component_reports().into_iter().all(|component| component.healthy)
    }

    pub(crate) fn component_names(&self) -> Vec<&'static str> {
        Self::COMPONENT_NAMES.to_vec()
    }

    pub(crate) fn unhealthy_component_count(&self) -> usize {
        self.unhealthy_component_names().len()
    }

    pub(crate) fn unhealthy_component_names(&self) -> Vec<&'static str> {
        let mut names = Vec::new();
        if self.remoting.as_ref().is_some_and(|report| !report.is_healthy()) {
            names.push("remoting");
        }
        if self
            .request_processor
            .as_ref()
            .is_some_and(|report| !report.is_healthy())
        {
            names.push("request_processor");
        }
        if self.topic_config.as_ref().is_some_and(|report| !report.is_healthy()) {
            names.push("topic_config");
        }
        names.extend(
            self.component_reports()
                .into_iter()
                .filter(|component| component.present && !component.healthy)
                .map(|component| component.name),
        );
        for component in &self.unfinished_components {
            if !names.contains(component) {
                names.push(component);
            }
        }
        names
    }

    pub(crate) fn timed_out_component_names(&self) -> Vec<&'static str> {
        let mut names = Vec::new();
        if self
            .remoting
            .as_ref()
            .is_some_and(BrokerRemotingServerShutdownReport::has_timed_out)
        {
            names.push("remoting");
        }
        if self
            .request_processor
            .as_ref()
            .is_some_and(shutdown_report_has_timed_out)
        {
            names.push("request_processor");
        }
        if self.topic_config.as_ref().is_some_and(|report| report.timed_out) {
            names.push("topic_config");
        }
        names.extend(
            self.component_reports()
                .into_iter()
                .filter(|component| component.timed_out)
                .map(|component| component.name),
        );
        names
    }

    fn component_reports(&self) -> Vec<&BrokerShutdownComponentReport> {
        vec![
            &self.broker_outer_api,
            &self.client_housekeeping,
            &self.auth,
            &self.service_tasks,
            &self.observability,
            &self.scheduled_tasks,
            &self.message_store,
            &self.deferred_services,
            &self.transaction_services,
            &self.fast_failure,
            &self.topic_route,
            &self.consumer_offset,
            &self.subscription_group,
            &self.metadata_io,
            &self.deadline,
        ]
    }
}

pub(super) fn shutdown_report_has_timed_out(report: &ShutdownReport) -> bool {
    report.timed_out > 0 || report.children.iter().any(shutdown_report_has_timed_out)
}
