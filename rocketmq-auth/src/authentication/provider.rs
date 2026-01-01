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

//! Authentication Provider Module (Rust 2021 Standard - No mod.rs)

// Declare submodules using #[path] attribute

pub mod authentication_metadata_provider;
pub mod authentication_provider;
pub mod default_authentication_provider;
pub mod local_authentication_metadata_provider;

// Re-export for convenience
pub use authentication_metadata_provider::AuthenticationMetadataProvider;
pub use authentication_provider::AuthenticationProvider;
pub use default_authentication_provider::DefaultAuthenticationProvider;
pub use local_authentication_metadata_provider::LocalAuthenticationMetadataProvider;
