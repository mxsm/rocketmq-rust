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

//! Offline tools for inspecting and preparing RocketMQ-Rust stores.
//!
//! The Cargo package is named `rocketmq-store-inspect`, while its executable is
//! `rocketmq-cli-rust`. The tool reads local files and never connects to a running
//! cluster. Stop the Broker before running downgrade preflight or multipath
//! consolidation because those commands require the exclusive Store lock.

pub mod command_line;
pub mod content_show;
pub mod downgrade_preflight;
pub(crate) mod errors;
pub mod multipath_consolidate;
