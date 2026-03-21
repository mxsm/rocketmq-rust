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
use std::time::SystemTime;

use dashmap::DashMap;

use crate::context::ProxyContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSession {
    pub client_id: String,
    pub remote_addr: Option<String>,
    pub namespace: Option<String>,
    pub last_seen: SystemTime,
}

#[derive(Clone, Default)]
pub struct ClientSessionRegistry {
    sessions: Arc<DashMap<String, ClientSession>>,
}

impl ClientSessionRegistry {
    pub fn upsert_from_context(&self, context: &ProxyContext) {
        let Some(client_id) = context.client_id() else {
            return;
        };

        self.sessions.insert(
            client_id.to_owned(),
            ClientSession {
                client_id: client_id.to_owned(),
                remote_addr: context.remote_addr().map(str::to_owned),
                namespace: context.namespace().map(str::to_owned),
                last_seen: SystemTime::now(),
            },
        );
    }

    pub fn remove(&self, client_id: &str) -> Option<ClientSession> {
        self.sessions.remove(client_id).map(|(_, session)| session)
    }

    pub fn get(&self, client_id: &str) -> Option<ClientSession> {
        self.sessions.get(client_id).map(|entry| entry.clone())
    }

    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }
}
