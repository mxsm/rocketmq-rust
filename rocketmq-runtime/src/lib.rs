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

#![deny(missing_docs)]

//! Runtime substrate for the RocketMQ Rust unified thread model.
//!
//! This crate standardizes how components own or borrow a Tokio runtime, how
//! they derive service-level task scopes through [`RootServiceContext`] and
//! [`ChildServiceContext`], how
//! [`TaskGroup`] and [`ScheduledTaskGroup`] track long-running and periodic
//! tasks, how [`BlockingExecutor`] isolates short blocking work, and how
//! [`ShutdownReport`] records verifiable shutdown evidence.
//!
//! Process entrypoints own [`RuntimeOwner`] and derive a single
//! [`RootServiceContext`]. Production libraries receive a
//! [`ChildServiceContext`] or a narrower capability. [`RuntimeContext`] is a
//! test and migration harness, while [`RocketMQRuntime`] remains only as a
//! deprecated compatibility boundary.

/// Actor types and operations.
pub mod actor;
pub mod blocking;
/// Common types and operations.
pub mod common;
/// Config types and operations.
pub mod config;
/// Context types and operations.
pub mod context;
/// Diagnostics types and operations.
pub mod diagnostics;
/// Error types and operations.
pub mod error;
/// Executor service types and operations.
pub mod executor_service;
mod handle;
/// Legacy types and operations.
pub mod legacy;
pub mod metadata_io;
/// Bounded operation context types and operations.
pub mod operation;
/// Owner types and operations.
pub mod owner;
pub mod prelude;
mod public_api;
/// Resource budget types and operations.
pub mod resource_budget;
/// Schedule types and operations.
pub mod schedule;
/// Scheduled types and operations.
pub mod scheduled;
/// Service context types and operations.
pub mod service_context;
pub mod service_lifecycle;
/// Shutdown types and operations.
pub mod shutdown;
/// Shutdown deadline types and operations.
pub mod shutdown_deadline;
/// Shutdown report types and operations.
pub mod shutdown_report;
/// Signal types and operations.
pub mod signal;
/// Task types and operations.
pub mod task;
/// Task group types and operations.
pub mod task_group;
mod task_spawner;
/// Tokio lock types and operations.
pub mod tokio_lock;

pub use actor::ActorRuntime;
pub use blocking::BlockingExecutorSnapshot;
pub use blocking::BlockingKind;
pub use blocking::BlockingLane;
pub use blocking::BlockingLanePolicies;
pub use blocking::BlockingPoolPolicy;
pub use blocking::BlockingTaskSnapshot;
pub use config::RuntimeConfig;
pub use context::RuntimeContext;
pub use diagnostics::RuntimeBlockingKindSummaryV1;
pub use diagnostics::RuntimeBlockingKindV1;
pub use diagnostics::RuntimeBlockingLaneSummaryV1;
pub use diagnostics::RuntimeBlockingLaneV1;
pub use diagnostics::RuntimeComponent;
pub use diagnostics::RuntimeDiagnostics;
pub use diagnostics::RuntimeDiagnosticsSnapshot;
pub use diagnostics::RuntimeDiagnosticsViewOptions;
pub use diagnostics::RuntimeDiagnosticsViewV1;
pub use diagnostics::RuntimeLifecycleStateV1;
pub use diagnostics::RuntimeTaskKindSummaryV1;
pub use diagnostics::RuntimeTaskKindV1;
pub use error::RuntimeError;
pub use error::RuntimeResult;
pub use executor_service::FuturesExecutorService;
pub use executor_service::FuturesExecutorServiceBuilder;
pub use executor_service::ScheduledExecutorService;
pub use executor_service::TokioExecutorService;
pub(crate) use handle::RuntimeHandle;
#[allow(deprecated)]
pub use legacy::RocketMQRuntime;
pub use metadata_io::LocalMetadataFileSystem;
pub use metadata_io::MetadataDeadline;
pub use metadata_io::MetadataDurability;
pub use metadata_io::MetadataFileSystem;
pub use metadata_io::MetadataGeneration;
pub use metadata_io::MetadataIoActor;
pub use metadata_io::MetadataIoConfig;
pub use metadata_io::MetadataIoError;
pub use metadata_io::MetadataIoOperation;
pub use metadata_io::MetadataIoReceipt;
pub use metadata_io::MetadataIoResourceSnapshot;
pub use metadata_io::MetadataIoShutdownReport;
pub use metadata_io::MetadataIoSnapshot;
pub use metadata_io::MetadataWriteRequest;
pub use operation::OperationContext;
pub use public_api::*;
pub use resource_budget::BudgetAcquireError;
pub use resource_budget::BudgetCapacity;
pub use resource_budget::BudgetClass;
pub use resource_budget::BudgetConfigError;
pub use resource_budget::BudgetDimension;
pub use resource_budget::BudgetLimit;
pub use resource_budget::BudgetSnapshot;
pub use resource_budget::BudgetedItem;
pub use resource_budget::BudgetedQueue;
pub use resource_budget::FullPolicy;
pub use resource_budget::MemoryLimitError;
pub use resource_budget::MemoryLimitSource;
pub use resource_budget::MonotonicClock;
pub use resource_budget::ProcessMemoryLimit;
pub use resource_budget::QueuePushError;
pub use resource_budget::QueuePushErrorKind;
pub use resource_budget::QueuePushOutcome;
pub use resource_budget::QueueSnapshot;
pub use resource_budget::RateLimit;
pub use resource_budget::ResourceBudgetTree;
pub use resource_budget::ResourcePermit;
pub use resource_budget::SystemMonotonicClock;
pub use schedule::executor::ExecutorConfig;
pub use schedule::executor::ExecutorPool;
pub use schedule::executor::TaskExecutor;
pub use schedule::scheduler::SchedulerConfig;
pub use schedule::scheduler::TaskScheduler;
pub use schedule::task::Task;
pub use schedule::task::TaskContext;
pub use schedule::task::TaskResult as LegacyTaskResult;
pub use schedule::task::TaskStatus;
pub use schedule::trigger::CronTrigger;
pub use schedule::trigger::DelayTrigger;
pub use schedule::trigger::DelayedIntervalTrigger;
pub use schedule::trigger::IntervalTrigger;
pub use schedule::trigger::Trigger;
pub use scheduled::ScheduleMode;
pub use scheduled::ScheduledTaskConfig;
pub use scheduled::ScheduledTaskControl;
pub use scheduled::ScheduledTaskGroup;
pub use scheduled::ScheduledTaskSnapshot;
pub use service_context::ScopeId;
pub use service_lifecycle::ServiceLifecycle;
pub use service_lifecycle::ServiceLifecycleConfig;
pub use service_lifecycle::ServiceLifecycleState;
pub use service_lifecycle::ShutdownReason;
pub use service_lifecycle::ShutdownRequest;
pub use shutdown::Shutdown;
pub use shutdown_report::ShutdownAnnotation;
pub use shutdown_report::TaskSnapshot;
pub use signal::wait_for_signal;
pub use signal::wait_for_signal_result;
pub use task_group::DetachedTaskPolicy;
pub use task_group::TaskGroupChildLease;
pub use task_group::TaskGroupChildStats;
pub use task_group::TaskGroupId;
pub use task_group::TaskGroupLifecycleState;
pub use task_group::TaskId;
pub use task_group::TaskKind;
pub use task_group::TaskResult;
pub use task_spawner::TaskSpawner;
