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

//! Read turns of a session whose reader and writer share one task.

use std::future::Future;
use std::pin::pin;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

/// Read turns that may end within one poll of a shared session task before it yields to the
/// scheduler, so the request tasks the reader spawned on its worker still run.
const TURNS_PER_POLL: usize = 8;

/// Frames and bytes the reader handles before the writer that shares its task gets a turn.
pub(crate) struct ReadTurn {
    handoff: Option<TurnHandoff>,
    frames: usize,
    bytes: usize,
}

impl ReadTurn {
    /// As many frames as Netty's `maxMessagesPerRead`, so a response waits at most one turn.
    const FRAMES: usize = 16;
    const BYTES: usize = 64 * 1024;

    /// Turns of a reader that hands its task to the writer through `handoff`. A reader with a
    /// task of its own passes `None`; its turns never end.
    pub(crate) const fn new(handoff: Option<TurnHandoff>) -> Self {
        Self {
            handoff,
            frames: 0,
            bytes: 0,
        }
    }

    pub(crate) fn record(&mut self, frame_bytes: usize) {
        if self.handoff.is_some() {
            self.frames += 1;
            self.bytes = self.bytes.saturating_add(frame_bytes);
        }
    }

    /// Returns the end of the turn to await when the turn is used up, starting the next one.
    pub(crate) fn take_exhausted(&mut self) -> Option<EndTurn<'_>> {
        let handoff = self.handoff.as_ref()?;
        if self.frames < Self::FRAMES && self.bytes < Self::BYTES {
            return None;
        }
        self.frames = 0;
        self.bytes = 0;
        Some(EndTurn { handoff, ended: false })
    }
}

/// Tells [`join_shared_task`] that the reader ended a turn.
#[derive(Clone, Debug, Default)]
pub(crate) struct TurnHandoff(Arc<AtomicBool>);

impl TurnHandoff {
    fn take(&self) -> bool {
        // The reader and the join run on the same task, so no other memory is published here.
        self.0.swap(false, Ordering::Relaxed)
    }
}

/// The end of a read turn: pending once without waking the task.
///
/// Only a reader run by [`join_shared_task`] with the same [`TurnHandoff`] may await it; the join
/// polls the reader again right after the writer.
pub(crate) struct EndTurn<'a> {
    handoff: &'a TurnHandoff,
    ended: bool,
}

impl Future for EndTurn<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.ended {
            return Poll::Ready(());
        }
        self.ended = true;
        self.handoff.0.store(true, Ordering::Relaxed);
        Poll::Pending
    }
}

/// Runs the reader and the writer of a session on one task until both finish.
///
/// Both halves are polled on every wake of the task. The writer writes while writes are queued
/// and hands over once the queue is empty; it yields only before a batch of at most two responses
/// while more of the session's requests are running, so their responses share one `writev`.
///
/// When the reader ends a turn, the writer is polled and the reader resumes in the same poll.
/// Yielding to the scheduler at every turn end instead put the session behind the request tasks
/// it had just spawned on its worker and cut single-connection throughput by about 14%. After
/// [`TURNS_PER_POLL`] turns the task wakes itself and yields, so those tasks still run.
/// Other measured alternatives were slower too: yielding after every write batch put the session
/// behind its own requests, and per-half wakers (`FuturesUnordered`) cut single-connection
/// throughput.
pub(crate) async fn join_shared_task<R, W>(handoff: TurnHandoff, reader: R, writer: W)
where
    R: Future<Output = ()>,
    W: Future<Output = ()>,
{
    let mut reader = pin!(reader);
    let mut writer = pin!(writer);
    let mut reader_done = false;
    let mut writer_done = false;
    std::future::poll_fn(move |cx| {
        let mut turns = 0;
        loop {
            let mut turn_ended = false;
            if !reader_done {
                if reader.as_mut().poll(cx).is_ready() {
                    reader_done = true;
                } else {
                    turn_ended = handoff.take();
                }
            }
            if !writer_done && writer.as_mut().poll(cx).is_ready() {
                writer_done = true;
            }
            if reader_done && writer_done {
                return Poll::Ready(());
            }
            if !turn_ended {
                return Poll::Pending;
            }
            turns += 1;
            if turns == TURNS_PER_POLL {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::task::Wake;
    use std::task::Waker;

    use super::*;

    #[derive(Default)]
    struct CountingWaker(AtomicUsize);

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A reader that reads `turns` full turns and then finishes.
    async fn reader(handoff: TurnHandoff, turns: usize, completed: Arc<AtomicUsize>) {
        let mut read_turn = ReadTurn::new(Some(handoff));
        for _ in 0..turns {
            for _ in 0..ReadTurn::FRAMES {
                read_turn.record(1);
            }
            completed.fetch_add(1, Ordering::SeqCst);
            if let Some(end_turn) = read_turn.take_exhausted() {
                end_turn.await;
            }
        }
    }

    /// A writer that counts its polls and finishes once `done` is set.
    fn writer(polls: Arc<AtomicUsize>, done: Arc<AtomicBool>) -> impl Future<Output = ()> {
        std::future::poll_fn(move |_| {
            polls.fetch_add(1, Ordering::SeqCst);
            if done.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }

    struct Session {
        completed_turns: Arc<AtomicUsize>,
        writer_polls: Arc<AtomicUsize>,
        writer_done: Arc<AtomicBool>,
        wakes: Arc<CountingWaker>,
        waker: Waker,
    }

    impl Session {
        fn new() -> Self {
            let wakes = Arc::new(CountingWaker::default());
            Self {
                completed_turns: Arc::default(),
                writer_polls: Arc::default(),
                writer_done: Arc::default(),
                waker: Waker::from(Arc::clone(&wakes)),
                wakes,
            }
        }

        fn task(&self, turns: usize) -> impl Future<Output = ()> {
            let handoff = TurnHandoff::default();
            let done = Arc::clone(&self.writer_done);
            let completed = Arc::clone(&self.completed_turns);
            let reader_half = reader(handoff.clone(), turns, completed);
            let reader_half = async move {
                reader_half.await;
                done.store(true, Ordering::SeqCst);
            };
            join_shared_task(
                handoff,
                reader_half,
                writer(Arc::clone(&self.writer_polls), Arc::clone(&self.writer_done)),
            )
        }

        fn poll(&self, task: Pin<&mut impl Future<Output = ()>>) -> Poll<()> {
            task.poll(&mut Context::from_waker(&self.waker))
        }
    }

    #[test]
    fn a_turn_end_polls_the_writer_and_resumes_the_reader_in_the_same_poll() {
        let session = Session::new();
        let mut task = pin!(session.task(3));

        assert_eq!(session.poll(task.as_mut()), Poll::Ready(()));
        assert_eq!(session.completed_turns.load(Ordering::SeqCst), 3);
        // Once at each turn end, and once more after the reader finished.
        assert_eq!(session.writer_polls.load(Ordering::SeqCst), 4);
        assert_eq!(
            session.wakes.0.load(Ordering::SeqCst),
            0,
            "no turn end yields to the scheduler"
        );
    }

    #[test]
    fn the_task_yields_to_the_scheduler_after_its_turns_per_poll() {
        let session = Session::new();
        let mut task = pin!(session.task(TURNS_PER_POLL + 2));

        assert_eq!(session.poll(task.as_mut()), Poll::Pending);
        assert_eq!(session.completed_turns.load(Ordering::SeqCst), TURNS_PER_POLL);
        assert_eq!(session.writer_polls.load(Ordering::SeqCst), TURNS_PER_POLL);
        assert_eq!(
            session.wakes.0.load(Ordering::SeqCst),
            1,
            "the task wakes itself before yielding"
        );

        assert_eq!(session.poll(task.as_mut()), Poll::Ready(()));
        assert_eq!(session.completed_turns.load(Ordering::SeqCst), TURNS_PER_POLL + 2);
        assert_eq!(session.wakes.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_reader_pending_on_input_leaves_the_task_without_waking_it() {
        let session = Session::new();
        let handoff = TurnHandoff::default();
        let mut task = pin!(join_shared_task(
            handoff,
            std::future::pending::<()>(),
            writer(Arc::clone(&session.writer_polls), Arc::clone(&session.writer_done)),
        ));

        assert_eq!(session.poll(task.as_mut()), Poll::Pending);
        assert_eq!(session.writer_polls.load(Ordering::SeqCst), 1);
        assert_eq!(session.wakes.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_reader_with_a_task_of_its_own_never_ends_a_turn() {
        let mut read_turn = ReadTurn::new(None);
        for _ in 0..(ReadTurn::FRAMES * 4) {
            read_turn.record(ReadTurn::BYTES);
        }
        assert!(read_turn.take_exhausted().is_none());
    }

    #[test]
    fn a_turn_ends_after_its_frames_or_its_bytes() {
        let mut by_frames = ReadTurn::new(Some(TurnHandoff::default()));
        for _ in 0..(ReadTurn::FRAMES - 1) {
            by_frames.record(1);
        }
        assert!(by_frames.take_exhausted().is_none());
        by_frames.record(1);
        assert!(by_frames.take_exhausted().is_some());
        assert!(by_frames.take_exhausted().is_none(), "the next turn starts empty");

        let mut by_bytes = ReadTurn::new(Some(TurnHandoff::default()));
        by_bytes.record(ReadTurn::BYTES);
        assert!(by_bytes.take_exhausted().is_some());
    }
}
