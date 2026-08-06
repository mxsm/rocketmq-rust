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

mod api;
mod model;
mod operations;
mod operations_analytics_repository;
mod operations_repository;
mod operations_service;
mod reconciler;
mod repository;
mod service;

#[cfg(test)]
mod logger_ttl_lifecycle_tests;
#[cfg(test)]
mod model_tests;
#[cfg(test)]
mod operations_repository_tests;
#[cfg(test)]
mod repository_tests;

pub(crate) use api::routes;
pub(crate) use operations_service::AutonomyOperationsService;
pub(crate) use reconciler::AutonomyPauseReconciler;
pub(crate) use service::AutonomyService;
