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

//! Desktop infrastructure owned by the application composition root.

#[path = "infrastructure/admin_provider.rs"]
pub mod admin_provider;
/// Real query/mutation Admin session adapters hidden behind the provider.
pub mod admin_session;
#[path = "infrastructure/auth_state.rs"]
pub mod auth_state;
#[path = "infrastructure/client_runtime.rs"]
pub mod client_runtime;
#[path = "infrastructure/config_store.rs"]
pub mod config_store;
#[path = "infrastructure/history_collector.rs"]
pub mod history_collector;
#[path = "infrastructure/history_store.rs"]
pub mod history_store;
#[path = "infrastructure/monitor_store.rs"]
pub mod monitor_store;
