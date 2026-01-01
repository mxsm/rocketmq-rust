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

//! Route management module for NameServer
//!
//! This module handles broker registration, topic routing, and cluster management.

pub mod async_segmented_lock;
pub mod batch_unregistration_service;
pub mod error;
pub mod route_info_manager;
pub mod route_info_manager_v2;
pub mod route_info_manager_wrapper;
pub mod segmented_lock;
pub mod tables;
pub mod types;
pub(crate) mod zone_route_rpc_hook;
