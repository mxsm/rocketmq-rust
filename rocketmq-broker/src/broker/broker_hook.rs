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

use std::sync::Arc;

/// Trait defining a shutdown hook.
///
/// This trait should be implemented by types that need to perform specific actions
/// before the system or application is shutdown. The `before_shutdown` method will
/// be called as part of the shutdown sequence, allowing for cleanup or other shutdown
/// procedures to be executed.
pub trait ShutdownHook {
    /// Method to be called before shutdown.
    ///
    /// Implementors should place any necessary cleanup or pre-shutdown logic within
    /// this method. This method is called automatically when a shutdown event occurs.
    fn before_shutdown(&self);
}

pub type BrokerShutdownHook = Arc<dyn ShutdownHook + Send + Sync + 'static>;
