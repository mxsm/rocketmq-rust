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

use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::Notify;

/// Generation-neutral side effects emitted by the V1/V2 dispatcher conformance fixtures.
#[allow(
    dead_code,
    reason = "the V1 integration crate only needs EventLog while the V2 crate-unit module consumes the shared vocabulary"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DispatcherEvent {
    Ordering {
        code: i32,
        opaque: i32,
        extension_value: String,
    },
    Clone,
    RejectCheck {
        code: i32,
    },
    Reject {
        code: i32,
    },
    Before {
        code: i32,
        opaque: i32,
        extension_value: String,
    },
    Process {
        code: i32,
        opaque: i32,
        extension_value: String,
    },
    After {
        code: i32,
        opaque: i32,
        extension_value: String,
    },
    Observe {
        request_code: i32,
        response_code: i32,
    },
    V1DirectWriteThenNone,
    V2ProtocolNoResponse,
}

/// Expected events for one admitted request that reaches the dispatcher-owned response write.
#[allow(
    dead_code,
    reason = "the V1 integration crate only needs EventLog while the V2 crate-unit module consumes the shared vocabulary"
)]
pub fn admitted_events(
    code: i32,
    opaque: i32,
    mutated_code: i32,
    mutated_opaque: i32,
    ingress_extension_value: &str,
    mutated_extension_value: &str,
    response_code: i32,
) -> Vec<DispatcherEvent> {
    vec![
        DispatcherEvent::Ordering {
            code,
            opaque,
            extension_value: ingress_extension_value.into(),
        },
        DispatcherEvent::Clone,
        DispatcherEvent::RejectCheck { code },
        DispatcherEvent::Before {
            code,
            opaque,
            extension_value: ingress_extension_value.into(),
        },
        DispatcherEvent::Process {
            code: mutated_code,
            opaque: mutated_opaque,
            extension_value: mutated_extension_value.into(),
        },
        DispatcherEvent::After {
            code: mutated_code,
            opaque: mutated_opaque,
            extension_value: mutated_extension_value.into(),
        },
        DispatcherEvent::Observe {
            request_code: code,
            response_code,
        },
    ]
}

/// Expected events for an admitted request whose original one-way identity suppresses the write.
#[allow(
    dead_code,
    reason = "the V1 integration crate only needs EventLog while the V2 crate-unit module consumes the shared vocabulary"
)]
pub fn admitted_events_without_dispatcher_write(
    code: i32,
    opaque: i32,
    mutated_code: i32,
    mutated_opaque: i32,
    ingress_extension_value: &str,
    mutated_extension_value: &str,
) -> Vec<DispatcherEvent> {
    let mut events = admitted_events(
        code,
        opaque,
        mutated_code,
        mutated_opaque,
        ingress_extension_value,
        mutated_extension_value,
        0,
    );
    let observed = events.pop();
    assert!(matches!(observed, Some(DispatcherEvent::Observe { .. })));
    events
}

/// Expected events for an admitted response when no RPC hook is registered.
#[allow(
    dead_code,
    reason = "the V1 integration crate only needs EventLog while the V2 crate-unit module consumes the shared vocabulary"
)]
pub fn admitted_events_without_hooks(
    code: i32,
    opaque: i32,
    ingress_extension_value: &str,
    response_code: i32,
) -> Vec<DispatcherEvent> {
    vec![
        DispatcherEvent::Ordering {
            code,
            opaque,
            extension_value: ingress_extension_value.into(),
        },
        DispatcherEvent::Clone,
        DispatcherEvent::RejectCheck { code },
        DispatcherEvent::Process {
            code,
            opaque,
            extension_value: ingress_extension_value.into(),
        },
        DispatcherEvent::Observe {
            request_code: code,
            response_code,
        },
    ]
}

/// Expected events for a structured rejection that bypasses hooks and processing.
#[allow(
    dead_code,
    reason = "the V1 integration crate only needs EventLog while the V2 crate-unit module consumes the shared vocabulary"
)]
pub fn rejected_events(
    code: i32,
    opaque: i32,
    ingress_extension_value: &str,
    response_code: i32,
) -> Vec<DispatcherEvent> {
    vec![
        DispatcherEvent::Ordering {
            code,
            opaque,
            extension_value: ingress_extension_value.into(),
        },
        DispatcherEvent::Clone,
        DispatcherEvent::RejectCheck { code },
        DispatcherEvent::Reject { code },
        DispatcherEvent::Observe {
            request_code: code,
            response_code,
        },
    ]
}

/// Thread-safe, test-only recorder for a sequence of copyable test events.
#[derive(Clone)]
pub struct EventLog<Event> {
    events: Arc<Mutex<Vec<Event>>>,
    changed: Arc<Notify>,
}

impl<Event> Default for EventLog<Event> {
    fn default() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            changed: Arc::new(Notify::new()),
        }
    }
}

impl<Event> EventLog<Event> {
    pub fn push(&self, event: Event) {
        self.events
            .lock()
            .expect("event log lock should not be poisoned")
            .push(event);
        self.changed.notify_waiters();
    }

    pub fn snapshot(&self) -> Vec<Event>
    where
        Event: Clone,
    {
        self.events
            .lock()
            .expect("event log lock should not be poisoned")
            .clone()
    }

    /// Waits until a predicate over the recorded event sequence becomes true.
    #[allow(
        dead_code,
        reason = "the V1 integration crate only needs snapshots while the V2 crate-unit module uses predicate barriers"
    )]
    pub async fn wait_for(&self, predicate: impl Fn(&[Event]) -> bool) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if predicate(
                self.events
                    .lock()
                    .expect("event log lock should not be poisoned")
                    .as_slice(),
            ) {
                return;
            }
            notified.await;
        }
    }
}
