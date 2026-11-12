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

//! Request-scoped resources that a write holds only until its CommitLog append is in place.

use std::any::Any;
use std::cell::Cell;
use std::future::Future;

type HeldUntilAppended = Cell<Option<Box<dyn Any + Send>>>;

tokio::task_local! {
    static HELD_UNTIL_APPENDED: HeldUntilAppended;
}

/// Runs `future` and drops `held` as soon as a CommitLog append inside it completes, before the
/// write waits for the disk flush or replica acknowledgement.
///
/// This lets a caller bound how many writes are appending at once without also bounding how many
/// wait for durability, as Java's `asyncPutMessage` frees its send thread after the append. Only
/// the first append releases `held`. When `future` appends nothing, fails before appending, or
/// appends on another task, `held` is dropped when the returned future completes or is dropped.
///
/// It returns the task-local scope future itself: an `async fn` wrapper would hold `future` both
/// as its argument and inside the scope it awaits, doubling a send's processing future.
pub fn hold_until_appended<T, F>(held: T, future: F) -> impl Future<Output = F::Output>
where
    T: Send + 'static,
    F: Future,
{
    HELD_UNTIL_APPENDED.scope(Cell::new(Some(Box::new(held))), future)
}

/// Releases the value held by the enclosing [`hold_until_appended`] scope, if any.
///
/// The CommitLog calls this after each append. It is public so callers can stand in for the
/// append when testing a scope.
#[doc(hidden)]
pub fn release_held_until_appended() {
    let _ = HELD_UNTIL_APPENDED.try_with(|held| drop(held.take()));
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use super::*;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn append_releases_the_held_value_before_the_scope_ends() {
        let dropped = Arc::new(AtomicBool::new(false));

        let released_inside = hold_until_appended(DropFlag(dropped.clone()), {
            let dropped = dropped.clone();
            async move {
                assert!(!dropped.load(Ordering::SeqCst));
                release_held_until_appended();
                let released = dropped.load(Ordering::SeqCst);
                release_held_until_appended();
                released
            }
        })
        .await;

        assert!(released_inside);
    }

    #[tokio::test]
    async fn held_value_is_dropped_with_the_scope_when_nothing_is_appended() {
        let dropped = Arc::new(AtomicBool::new(false));

        hold_until_appended(DropFlag(dropped.clone()), {
            let dropped = dropped.clone();
            async move { assert!(!dropped.load(Ordering::SeqCst)) }
        })
        .await;

        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn release_outside_a_scope_is_a_no_op() {
        release_held_until_appended();
    }

    #[tokio::test]
    async fn scope_holds_the_future_once() {
        let write = async {
            let state = [7_u8; 4096];
            tokio::task::yield_now().await;
            std::hint::black_box(state);
        };
        let write_size = std::mem::size_of_val(&write);

        let held = hold_until_appended((), write);

        assert!(
            std::mem::size_of_val(&held) < write_size + 256,
            "held write is {} bytes for a {write_size}-byte write",
            std::mem::size_of_val(&held)
        );
        held.await;
    }
}
