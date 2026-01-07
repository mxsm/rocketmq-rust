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

//! Authentication metadata manager module.
//!
//! This module provides high-level management APIs for authentication metadata,
//! including user creation, update, deletion, and querying operations.

pub mod authentication_metadata_manager;
pub mod authentication_metadata_manager_impl;

pub use authentication_metadata_manager::AuthenticationMetadataManager;
pub use authentication_metadata_manager::ManagerResult;
pub use authentication_metadata_manager_impl::AuthenticationMetadataManagerImpl;
