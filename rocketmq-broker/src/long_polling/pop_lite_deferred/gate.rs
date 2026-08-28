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

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Weak;

use cheetah_string::CheetahString;
use parking_lot::Mutex;

/// Per-client single-flight gate for message-arrival claims.
#[derive(Clone, Default)]
pub(crate) struct PopLiteEventGate {
    inner: Arc<PopLiteEventGateInner>,
}

#[derive(Default)]
struct PopLiteEventGateInner {
    active_clients: Mutex<HashSet<CheetahString>>,
}

impl PopLiteEventGate {
    pub(crate) fn try_reserve(&self, client_id: &CheetahString) -> Option<PopLiteEventGateReservation> {
        let mut active = self.inner.active_clients.lock();
        if !active.insert(client_id.clone()) {
            return None;
        }
        Some(PopLiteEventGateReservation {
            inner: Arc::downgrade(&self.inner),
            client_id: Some(client_id.clone()),
        })
    }

    pub(crate) fn active_count(&self) -> usize {
        self.inner.active_clients.lock().len()
    }
}

/// Affine gate ownership held through canonical resume and write completion.
#[must_use]
pub(crate) struct PopLiteEventGateReservation {
    inner: Weak<PopLiteEventGateInner>,
    client_id: Option<CheetahString>,
}

impl Drop for PopLiteEventGateReservation {
    fn drop(&mut self) {
        let (Some(inner), Some(client_id)) = (self.inner.upgrade(), self.client_id.take()) else {
            return;
        };
        inner.active_clients.lock().remove(&client_id);
    }
}
