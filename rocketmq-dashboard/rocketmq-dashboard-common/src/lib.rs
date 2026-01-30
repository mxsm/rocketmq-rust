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

//! Common shared code for RocketMQ Dashboard implementations
//!
//! This crate provides shared functionality, data models, and business logic
//! that can be reused across different dashboard implementations (GPUI, Tauri, etc.)

pub mod api;
pub mod models;
pub mod service;

pub use api::*;
pub use models::*;
pub use service::*;
