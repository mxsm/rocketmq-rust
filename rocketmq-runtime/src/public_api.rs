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

//! Deliberate stable runtime ownership and shutdown entry points.

pub use crate::blocking::BlockingExecutor;
pub use crate::owner::RuntimeOwner;
pub use crate::resource_budget::ResourceBudget;
pub use crate::service_context::ChildServiceContext;
pub use crate::service_context::RootServiceContext;
pub use crate::shutdown_deadline::ShutdownDeadline;
pub use crate::shutdown_report::ShutdownReport;
pub use crate::task_group::TaskGroup;
