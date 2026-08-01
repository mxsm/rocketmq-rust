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

//! Shared ownership boundary for client capabilities.

use std::fmt;
use std::sync::Arc;

use crate::runtime::ClientRuntime;

/// Identifies the application-owned runtime and task owner shared by a set of
/// client capabilities.
///
/// Cloning a session only clones the runtime `Arc`; it does not create a
/// runtime, connection pool, route cache, or background task.
#[derive(Clone)]
pub struct ClientSession {
    runtime: Arc<ClientRuntime>,
}

impl ClientSession {
    #[must_use]
    pub fn new(runtime: Arc<ClientRuntime>) -> Self {
        Self { runtime }
    }

    /// Returns the shared application runtime.
    #[must_use]
    pub fn runtime(&self) -> Arc<ClientRuntime> {
        Arc::clone(&self.runtime)
    }

    /// Returns whether both handles are backed by the same runtime and task
    /// owner.
    #[must_use]
    pub fn shares_runtime_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.runtime, &other.runtime)
    }
}

impl fmt::Debug for ClientSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientSession")
            .field("runtime", &"<shared>")
            .finish()
    }
}

/// Implemented by concrete client handles that borrow one shared session.
///
/// Publicly constructed handles always return `Some`. `None` is reserved for
/// crate-internal bootstrap producers that are owned directly by a
/// [`crate::runtime::ServiceContext`] instead of a public client session.
pub trait ClientSessionProvider {
    #[must_use]
    fn client_session(&self) -> Option<&ClientSession>;
}

#[cfg(test)]
mod tests {
    use super::ClientSession;
    use crate::runtime::test_client_runtime;

    #[test]
    fn cloning_session_preserves_runtime_identity() {
        let session = ClientSession::new(test_client_runtime("client-session-identity"));
        let clone = session.clone();

        assert!(session.shares_runtime_with(&clone));
    }
}
