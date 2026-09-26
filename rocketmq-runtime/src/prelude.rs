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

//! Minimal imports for owned service lifecycles, and the recommended entry path.
//!
//! A component that owns its runtime follows five steps, in this order: own the
//! runtime, derive exactly one component context, register services, register
//! bounded periodic work instead of driving a raw loop, and read the shutdown
//! report after draining final I/O. The example below runs as a documented test,
//! so it fails the test suite rather than drifting from the API.
//!
//! ```
//! use std::time::Duration;
//!
//! use rocketmq_runtime::prelude::*;
//! use rocketmq_runtime::RuntimeConfig;
//! use rocketmq_runtime::ScheduledExecutionPolicy;
//! use rocketmq_runtime::ScheduledTaskConfig;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 1. Own the runtime and validate its configuration before construction.
//!     let owner = RuntimeOwner::plan(RuntimeConfig::default())?.build()?;
//!
//!     // 2. Derive exactly one component context from the sealed root.
//!     let component = owner.root_context().component("broker");
//!
//!     // 3. Register a service that observes cancellation and cleans up in order.
//!     let cancellation = component.task_group().cancellation_token();
//!     component.spawn_service("broker.accept-loop", async move {
//!         cancellation.cancelled().await;
//!     })?;
//!
//!     // 4. Register bounded periodic work instead of driving a raw loop.
//!     component
//!         .scheduled_tasks("broker.schedules")
//!         .schedule(
//!             ScheduledTaskConfig::fixed_delay("broker.flush", Duration::from_secs(5)),
//!             ScheduledExecutionPolicy::default(),
//!             || async {},
//!         )?;
//!
//!     // 5. Drain already accepted I/O inside the shutdown budget, then read the
//!     //    report. The deadline is absolute and cannot be extended later.
//!     owner.block_on(async {
//!         component
//!             .storage_io()
//!             .spawn_io_until(
//!                 "broker.final-flush",
//!                 ShutdownDeadline::after(Duration::from_secs(5)),
//!                 || (),
//!             )
//!             .await
//!     })?;
//!     let report = owner.shutdown_runtime_blocking()?;
//!     assert_eq!(report.timed_out, 0);
//!
//!     Ok(())
//! }
//! ```
//!
//! Work that can fail critically registers through
//! [`ChildServiceContext::spawn_critical_service`](crate::ChildServiceContext::spawn_critical_service)
//! and is handled by a monitor on an owner outside the monitored group. Work
//! that is not owned by this process uses
//! [`RuntimeContext`](crate::RuntimeContext), which is the migration and test
//! harness rather than a production entry point.

pub use crate::BlockingExecutor;
pub use crate::ChildServiceContext;
pub use crate::ResourceBudget;
pub use crate::RootServiceContext;
pub use crate::RuntimeOwner;
pub use crate::ShutdownDeadline;
pub use crate::ShutdownReport;
pub use crate::TaskGroup;
