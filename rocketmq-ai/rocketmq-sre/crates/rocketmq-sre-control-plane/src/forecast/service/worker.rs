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

use std::time::Duration;

use rocketmq_runtime::{ChildServiceContext, ScheduledExecutionPolicy, ScheduledTaskConfig};

use super::*;

impl ForecastService {
    pub(crate) fn schedule(&self, context: &ChildServiceContext) -> Result<(), ControlPlaneError> {
        let mut config =
            ScheduledTaskConfig::fixed_rate_no_overlap("phase2-forecast-evaluator", self.worker_interval());
        config.initial_delay = Duration::from_secs(7);
        config.max_run_time = Some(Duration::from_secs(10 * 60));
        self.schedule_configured(context, config)
    }

    fn schedule_configured(
        &self,
        context: &ChildServiceContext,
        config: ScheduledTaskConfig,
    ) -> Result<(), ControlPlaneError> {
        let tasks = context.scheduled_tasks("forecast-evaluator");
        let owner = tasks.group().clone();
        let worker = self.clone();
        tasks
            .schedule(config, ScheduledExecutionPolicy::default(), move || {
                let worker = worker.clone();
                let owner = owner.clone();
                async move {
                    worker.run_due(&owner).await;
                }
            })
            .map_err(ControlPlaneError::configuration_source)?;
        Ok(())
    }
}

#[cfg(test)]
mod postgres_tests;
#[cfg(test)]
mod tests;
