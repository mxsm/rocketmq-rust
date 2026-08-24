// Copyright 2025 The RocketMQ Rust Authors
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

//! Feature-local state for the first dashboard delivery.

#[path = "features/login.rs"]
pub mod login;

#[path = "features/ops.rs"]
pub mod ops;

#[path = "features/proxy.rs"]
pub mod proxy;

#[path = "features/dashboard_store.rs"]
pub mod dashboard_store;

#[path = "features/dashboard.rs"]
pub mod dashboard;

#[path = "features/brokers_store.rs"]
pub mod brokers_store;

#[path = "features/brokers.rs"]
pub mod brokers;

#[path = "features/inspector_store.rs"]
pub mod inspector_store;

#[path = "features/broker_inspector.rs"]
pub mod broker_inspector;

#[path = "features/topics_store.rs"]
pub mod topics_store;

#[path = "features/topic_dialogs.rs"]
pub mod topic_dialogs;

#[path = "features/topic_mutations.rs"]
mod topic_mutations;

#[path = "features/topic_detail.rs"]
pub mod topic_detail;

#[path = "features/topics.rs"]
pub mod topics;

#[cfg(test)]
#[path = "features/topics_product_tests.rs"]
mod topics_product_tests;
