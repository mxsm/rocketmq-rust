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
mod catalog;
mod executor_client;
mod model;
mod policy;
mod repository;
mod service;
pub(crate) mod signing;

#[cfg(test)]
mod credential_rotation_e2e_tests;
#[cfg(test)]
mod critic_tests;
#[cfg(test)]
mod proxy_restart_e2e_tests;
#[cfg(test)]
mod service_tests;
#[cfg(test)]
mod wave_actions_e2e_tests;

pub(crate) use api::routes;
pub(crate) use executor_client::ExecutorSubmissionClient;
pub(crate) use model::ActionPlanView;
pub(crate) use model::ApprovalDecisionRequest;
pub(crate) use model::ExecutionSubmissionView;
pub(crate) use model::ExternalApprovalSource;
pub(crate) use model::SubmitExecutionRequest;
pub(crate) use service::SupervisedExecutionService;
