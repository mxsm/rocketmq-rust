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

use std::collections::BTreeSet;

use chrono::Utc;
use rocketmq_runtime::{ChildServiceContext, ScheduledExecutionPolicy, ScheduledTaskConfig};

use super::InvestigationRunService;
use crate::{ControlPlaneRequestFailure, auth::AuthContext};

const RECOVERY_BATCH_SIZE: u32 = 32;
const RECOVERY_LEASE_SECONDS: u32 = 30;

pub(crate) struct RecoverySummary {
    pub candidates: usize,
    pub reconciled: usize,
    pub failures: usize,
}

impl InvestigationRunService {
    pub(crate) fn schedule_recovery(&self, context: &ChildServiceContext) -> Result<(), ControlPlaneRequestFailure> {
        let scheduled = context.scheduled_tasks("investigation-run-recovery");
        let mut config =
            ScheduledTaskConfig::fixed_rate_no_overlap("investigation-run-recovery", std::time::Duration::from_secs(5));
        config.initial_delay = std::time::Duration::ZERO;
        config.max_run_time = Some(std::time::Duration::from_secs(20));
        let recovery = self.clone();
        let cancellation = context.task_group().cancellation_token();
        scheduled
            .schedule(config, ScheduledExecutionPolicy::default(), move || {
                let recovery = recovery.clone();
                let cancellation = cancellation.clone();
                async move {
                    // The scheduler stops new ticks; an in-flight SQL future
                    // must also observe owner cancellation and drop its transaction.
                    let result = tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return,
                        result = recovery.reconcile_once() => result,
                    };
                    match result {
                        Ok(summary) if summary.reconciled > 0 || summary.failures > 0 => {
                            tracing::info!(
                                candidates = summary.candidates,
                                reconciled = summary.reconciled,
                                failures = summary.failures,
                                "completed bounded investigation run recovery"
                            );
                        }
                        Ok(_) => {}
                        Err(error) => {
                            tracing::warn!(error_code = error.code(), "investigation run recovery scan failed")
                        }
                    }
                }
            })
            .map_err(ControlPlaneRequestFailure::configuration_source)?;
        Ok(())
    }

    /// Repairs expired ownership without collecting data or starting queued work.
    /// The caller owns this bounded future through ChildServiceContext.
    pub(crate) async fn reconcile_once(&self) -> Result<RecoverySummary, ControlPlaneRequestFailure> {
        let candidates = self
            .repository
            .investigation_run_recoverable(RECOVERY_BATCH_SIZE, Utc::now())
            .await?;
        let mut summary = RecoverySummary {
            candidates: candidates.len(),
            reconciled: 0,
            failures: 0,
        };
        for candidate in candidates {
            // This maintenance identity can only reach the internal reconcile
            // transition, which closes admission and accounts for orphaned work.
            let auth = AuthContext {
                tenant_id: candidate.tenant_id,
                subject: "investigation-run-recovery".to_owned(),
                clusters: BTreeSet::from([candidate.cluster_id]),
                roles: BTreeSet::from(["operator".to_owned()]),
            };
            match self
                .repository
                .investigation_run_reconcile(
                    &auth,
                    candidate.run_id,
                    self.recovery_owner,
                    RECOVERY_LEASE_SECONDS,
                    Utc::now(),
                )
                .await
            {
                Ok(Some(_)) => summary.reconciled += 1,
                Ok(None) => {}
                Err(error) => {
                    summary.failures += 1;
                    tracing::warn!(error_code = error.code(), "investigation run recovery candidate failed");
                }
            }
        }
        Ok(summary)
    }
}
