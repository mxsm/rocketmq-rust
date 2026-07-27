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
mod canonical_hash;
pub mod correlation;
pub mod diagnostics;
mod execution_state;
pub mod health;
mod incident_manager;
mod plan;
pub mod postmortem;
pub mod prediction;
mod registry;
pub mod slo;

pub use action_catalog::ActionCatalog;
pub use action_catalog::ActionCatalogError;
pub use canonical_hash::canonical_plan_hash;
pub use execution_state::ExecutionStateMachine;
pub use incident_manager::IncidentManager;
pub use incident_manager::IncidentManagerError;
pub use plan::PlanError;
pub use plan::PlanService;
pub use registry::DescriptorRegistry;
pub use registry::RegistryError;
