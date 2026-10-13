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

//! The Broker health diagnosis: the instances of one Broker, their diagnostics, their replication
//! and their configuration, read from one session inside one request budget.

use std::time::Duration;

use super::describe_broker_in_session;
use super::diagnosis_support::note;
use super::diagnosis_support::wrap_report;
use super::diagnosis_support::SupportingBudget;
use super::normalized_logical_identifier;
use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::adapter::admin_session::ResolvedCluster;
use crate::model::contract::QueryCompleteness;
use crate::model::contract::QueryPayload;
use crate::model::contract::QueryResult;
use crate::model::contract::QuerySource;
use crate::model::diagnosis::DiagnosisReport;
use crate::service::broker_health_diagnosis;
use crate::service::broker_health_diagnosis::BrokerHealthEvidence;
use crate::tools::diagnosis_tools::DiagnoseBrokerHealthArgs;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    pub(crate) async fn diagnose_broker_health(
        &self,
        mut args: DiagnoseBrokerHealthArgs,
    ) -> Result<QueryResult<DiagnosisReport>, ToolFailure> {
        args.broker_name = normalized_logical_identifier("broker_name", &args.broker_name)?;
        let cluster = self.resolve_cluster(&args.cluster)?;
        let key = self.cache_key(
            "diagnose_broker_health",
            &cluster.name,
            &format!("broker={}", args.broker_name),
        );
        let ttl = Duration::from_millis(self.config.cache.broker_metrics_ttl_ms);
        let policy_profile = self.config.diagnosis.consumer_lag_policy_profile.clone();
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolFailure::Rejected(ToolRejection::Cancelled),
                move || async {
                    let budget = SupportingBudget::new(&self.control);
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(diagnose_in_session(session, cluster, args, policy_profile, budget))
                    })
                    .await
                },
            )
            .await
    }
}

/// Collects the evidence and evaluates the rules on it.
///
/// Which instances exist and whether they answer is read first and may take the whole request
/// budget: without it the health is unknown. Diagnostics, replication and configuration share the
/// supporting budget. No read fails the diagnosis.
async fn diagnose_in_session<S>(
    session: &mut S,
    cluster: &ResolvedCluster,
    args: DiagnoseBrokerHealthArgs,
    policy_profile: String,
    mut budget: SupportingBudget,
) -> Result<QueryPayload<DiagnosisReport>, ToolFailure>
where
    S: AdminSession,
{
    let mut completeness = QueryCompleteness::default();
    let broker_name = args.broker_name.as_str();

    let summary = note(
        &mut completeness,
        QuerySource::BrokerRuntime,
        broker_name,
        describe_broker_in_session(session, cluster, broker_name.to_string()).await,
    )
    .map(|summary| summary.data);
    let evidence = if summary.as_ref().err().and_then(ToolFailure::not_found_entity) == Some(NotFoundEntity::Broker) {
        // A Broker that is not registered has no instance to ask anything of.
        let absent = || ToolFailure::not_found(NotFoundEntity::Broker);
        BrokerHealthEvidence {
            summary,
            diagnostics: Err(absent()),
            ha: Err(absent()),
            config: Err(absent()),
        }
    } else {
        let diagnostics = note(
            &mut completeness,
            QuerySource::BrokerRuntime,
            broker_name,
            budget.read(session.broker_diagnostics(broker_name)).await,
        )
        .map(|diagnostics| diagnostics.data);
        let masters = [broker_name.to_string()];
        let ha = note(
            &mut completeness,
            QuerySource::BrokerHaRuntime,
            broker_name,
            budget.read(session.ha_status(&masters, false, &[])).await,
        )
        .map(|ha| ha.data);
        let config = note(
            &mut completeness,
            QuerySource::BrokerConfig,
            broker_name,
            budget.read(session.broker_config_summary(broker_name)).await,
        )
        .map(|config| config.data);
        BrokerHealthEvidence {
            summary,
            diagnostics,
            ha,
            config,
        }
    };

    let report = broker_health_diagnosis::evaluate(&args, evidence, &policy_profile);
    Ok(wrap_report(completeness, report))
}
