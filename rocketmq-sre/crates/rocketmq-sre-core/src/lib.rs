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

//! Provider-neutral coordination primitives for RocketMQ AI SRE.

mod action_catalog;
mod automation_state;
mod autonomy_policy;
mod canonical_hash;
mod change_calendar;
pub mod correlation;
pub mod diagnostics;
mod embedded_actions;
mod eligibility;
mod execution_state;
pub mod health;
mod incident_manager;
mod integration;
mod plan;
pub mod postmortem;
pub mod prediction;
mod registry;
mod release;
mod runbook;
pub mod slo;

pub use action_catalog::ActionCatalog;
pub use action_catalog::ActionCatalogError;
pub use automation_state::AutonomyActor;
pub use automation_state::AutonomyStateMachine;
pub use automation_state::AutonomyTransitionError;
pub use automation_state::PromotionQualification;
pub use autonomy_policy::ActualModelIdentity;
pub use autonomy_policy::AutonomyPolicy;
pub use canonical_hash::canonical_plan_hash;
pub use change_calendar::ChangeCalendar;
pub use change_calendar::ChangeCalendarError;
pub use embedded_actions::EMBEDDED_ACTION_DESCRIPTOR_YAMLS;
pub use eligibility::AutonomyCandidatePath;
pub use eligibility::BaseEligibilityFacts;
pub use eligibility::DynamicSafetyEvaluation;
pub use eligibility::DynamicSafetyFacts;
pub use eligibility::EligibilityEngine;
pub use eligibility::FinalEligibilityFacts;
pub use execution_state::ExecutionStateMachine;
pub use incident_manager::IncidentManager;
pub use incident_manager::IncidentManagerError;
pub use integration::IntegrationError;
pub use integration::IntegrationValidator;
pub use plan::PlanError;
pub use plan::PlanService;
pub use registry::DescriptorRegistry;
pub use registry::RegistryError;
pub use release::ReleaseError;
pub use release::ReleaseStateMachine;
pub use release::ReleaseValidator;
pub use runbook::RunbookError;
pub use runbook::RunbookValidator;
