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

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;

use crate::PostgresRepository;
use crate::forecast::ForecastService;
use crate::supervised_execution::SupervisedExecutionService;

mod integration;
mod release;
mod release_execution;
mod release_rollback;
mod release_validation;
mod support;

type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// Coordinates release preparation, supervised execution, integration
/// delivery, regression pause, rollback, and immutable reporting.
#[derive(Clone)]
pub(crate) struct ReleaseManagementService {
    pub(super) repository: PostgresRepository,
    pub(super) supervised: SupervisedExecutionService,
    pub(super) forecast: ForecastService,
    clock: Clock,
}

impl ReleaseManagementService {
    pub(crate) fn new(
        repository: PostgresRepository,
        supervised: SupervisedExecutionService,
        forecast: ForecastService,
    ) -> Self {
        Self {
            repository,
            supervised,
            forecast,
            clock: Arc::new(Utc::now),
        }
    }

    pub(super) fn now(&self) -> DateTime<Utc> {
        (self.clock)()
    }
}
