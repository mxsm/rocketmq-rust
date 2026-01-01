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

pub mod service_task;

pub use service_task::ServiceManager;

/// Helper macro to create a service thread implementation
#[macro_export]
macro_rules! service_manager {
    ($service_type:ty) => {
        impl $service_type {
            pub fn create_service_task(self) -> $crate::task::service_task::ServiceManager<Self> {
                $crate::task::service_task::ServiceManager::new(self)
            }
        }
    };
}
