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

//! Immutable facts captured when a remoting request enters the transport.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use super::RequestId;

const FIRST_ALLOCATED_ID: u64 = 1;
const EXHAUSTED_ID: u64 = u64::MAX;

static NEXT_SESSION_OWNER: AtomicU64 = AtomicU64::new(FIRST_ALLOCATED_ID);

fn reserve_checked(counter: &AtomicU64) -> Option<u64> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| match next {
            0 | EXHAUSTED_ID => None,
            _ => Some(next + 1),
        })
        .ok()
}

/// Reserves a process-local owner for one real network or embedded session.
///
/// Zero is never allocated, and `u64::MAX` remains reserved for synthetic V1
/// response receipts. Once exhausted, the process-local allocator never wraps.
pub(crate) fn reserve_session_owner() -> Option<u64> {
    reserve_checked(&NEXT_SESSION_OWNER)
}

/// Immutable wire identity captured for one inbound request.
///
/// This value preserves the raw request code, protocol opaque, and one-way flag
/// as they arrived at the trusted transport boundary. Later hook or processor
/// mutations of the command cannot change these facts.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::OriginalRequestIdentity;
///
/// fn fields_are_private(identity: OriginalRequestIdentity) {
///     let _ = identity.original_code;
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OriginalRequestIdentity {
    request_id: RequestId,
    original_code: i32,
    original_opaque: i32,
    one_way: bool,
}

impl OriginalRequestIdentity {
    pub(crate) fn capture(owner_id: u64, request_sequence: &AtomicU64, command: &RemotingCommand) -> Option<Self> {
        if owner_id == 0 || owner_id == EXHAUSTED_ID {
            return None;
        }
        let sequence = reserve_checked(request_sequence)?;
        Some(Self {
            request_id: RequestId::real(owner_id, sequence)?,
            original_code: command.code(),
            original_opaque: command.opaque(),
            one_way: command.is_oneway_rpc(),
        })
    }

    /// Returns the process-local identity allocated for this request.
    #[must_use]
    pub const fn request_id(self) -> RequestId {
        self.request_id
    }

    /// Returns the raw request code captured from the inbound frame.
    #[must_use]
    pub const fn original_code(self) -> i32 {
        self.original_code
    }

    /// Returns the protocol opaque captured from the inbound frame.
    #[must_use]
    pub const fn original_opaque(self) -> i32 {
        self.original_opaque
    }

    /// Returns whether the inbound frame was originally marked one-way.
    #[must_use]
    pub const fn is_one_way(self) -> bool {
        self.one_way
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    use super::*;

    fn request(code: i32, opaque: i32) -> RemotingCommand {
        RemotingCommand::create_remoting_command(code).set_opaque(opaque)
    }

    #[test]
    fn checked_allocator_is_sequential_and_excludes_reserved_values() {
        let counter = AtomicU64::new(1);

        assert_eq!(reserve_checked(&counter), Some(1));
        assert_eq!(reserve_checked(&counter), Some(2));
        assert_ne!(counter.load(Ordering::Relaxed), 0);
        assert_ne!(counter.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn checked_allocator_issues_max_minus_one_then_stays_exhausted() {
        let counter = AtomicU64::new(u64::MAX - 1);

        assert_eq!(reserve_checked(&counter), Some(u64::MAX - 1));
        assert_eq!(reserve_checked(&counter), None);
        assert_eq!(reserve_checked(&counter), None);
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn checked_allocator_rejects_zero_without_recovery() {
        let counter = AtomicU64::new(0);

        assert_eq!(reserve_checked(&counter), None);
        assert_eq!(reserve_checked(&counter), None);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn concurrent_reservations_are_unique() {
        const THREADS: usize = 8;
        const IDS_PER_THREAD: usize = 256;

        let counter = Arc::new(AtomicU64::new(1));
        let threads = (0..THREADS)
            .map(|_| {
                let counter = Arc::clone(&counter);
                thread::spawn(move || {
                    (0..IDS_PER_THREAD)
                        .map(|_| reserve_checked(&counter).expect("test allocator should have capacity"))
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        let ids = threads
            .into_iter()
            .flat_map(|thread| thread.join().expect("allocator thread should finish"))
            .collect::<HashSet<_>>();

        assert_eq!(ids.len(), THREADS * IDS_PER_THREAD);
        assert!(!ids.contains(&0));
        assert!(!ids.contains(&u64::MAX));
    }

    #[test]
    fn capture_preserves_raw_ingress_facts_and_increases_sequence() {
        let sequence = AtomicU64::new(1);
        let first = OriginalRequestIdentity::capture(17, &sequence, &request(-123_456, 91).mark_oneway_rpc())
            .expect("identity should be allocated");
        let second = OriginalRequestIdentity::capture(17, &sequence, &request(-123_456, 91))
            .expect("identity should be allocated");

        assert_eq!(first.request_id().owner_id(), 17);
        assert_eq!(first.request_id().sequence(), 1);
        assert_eq!(second.request_id().owner_id(), 17);
        assert_eq!(second.request_id().sequence(), 2);
        assert_eq!(first.original_code(), -123_456);
        assert_eq!(first.original_opaque(), 91);
        assert!(first.is_one_way());
        assert!(!second.is_one_way());
    }

    #[test]
    fn capture_rejects_reserved_owner_and_exhausted_sequence() {
        let available = AtomicU64::new(1);
        let exhausted = AtomicU64::new(u64::MAX);
        let command = request(10, 20);

        assert!(OriginalRequestIdentity::capture(0, &available, &command).is_none());
        assert!(OriginalRequestIdentity::capture(u64::MAX, &available, &command).is_none());
        assert_eq!(available.load(Ordering::Relaxed), 1);
        assert!(OriginalRequestIdentity::capture(1, &exhausted, &command).is_none());
    }
}
