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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use rocketmq_admin_core::mutation_client_adapter::MutationAdminSession;
use tokio::sync::Mutex;

use crate::ConfigWriteClient;

/// The only SRE component allowed to own target mutation clients.
///
/// No method returns credentials, raw configuration, or an untyped mutation
/// interface. Action handlers receive only the exact client they implement.
#[derive(Clone, Default)]
pub struct MutationCredentialOwner {
    admin: Option<Arc<Mutex<MutationAdminSession>>>,
    kubernetes: Option<kube::Client>,
    config: Option<Arc<dyn ConfigWriteClient>>,
}

impl MutationCredentialOwner {
    #[must_use]
    pub fn new(
        admin: Option<MutationAdminSession>,
        kubernetes: Option<kube::Client>,
        config: Option<Arc<dyn ConfigWriteClient>>,
    ) -> Self {
        Self {
            admin: admin.map(|session| Arc::new(Mutex::new(session))),
            kubernetes,
            config,
        }
    }

    #[must_use]
    pub const fn has_admin(&self) -> bool {
        self.admin.is_some()
    }

    #[must_use]
    pub const fn has_kubernetes(&self) -> bool {
        self.kubernetes.is_some()
    }

    #[must_use]
    pub const fn has_config(&self) -> bool {
        self.config.is_some()
    }
}

impl Debug for MutationCredentialOwner {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MutationCredentialOwner")
            .field("admin", &self.admin.is_some())
            .field("kubernetes", &self.kubernetes.is_some())
            .field("config", &self.config.is_some())
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}
