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

use std::future::Future;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::sync::OnceCell;

use crate::shutdown_deadline::ShutdownDeadline;
use crate::shutdown_report::ShutdownReport;

#[derive(Debug)]
pub(super) struct ShutdownTimeout;

#[derive(Debug)]
pub(super) struct ShutdownCoordinator {
    deadline: Mutex<Option<ShutdownDeadline>>,
    changed: Notify,
    pub(super) report: OnceCell<ShutdownReport>,
}

impl ShutdownCoordinator {
    pub(super) fn new() -> Self {
        Self {
            deadline: Mutex::new(None),
            changed: Notify::new(),
            report: OnceCell::new(),
        }
    }

    pub(super) fn deadline(&self) -> Option<ShutdownDeadline> {
        *self.deadline.lock()
    }

    pub(super) fn tighten(&self, deadline: ShutdownDeadline) -> ShutdownDeadline {
        let (installed, changed) = {
            let mut current = self.deadline.lock();
            let installed = current.map_or(deadline, |existing| existing.earliest(deadline));
            let changed = current.is_none_or(|existing| installed.instant() < existing.instant());
            *current = Some(installed);
            (installed, changed)
        };

        if changed {
            self.changed.notify_waiters();
        }
        installed
    }

    pub(super) async fn run_until<F>(&self, future: F) -> Result<F::Output, ShutdownTimeout>
    where
        F: Future,
    {
        tokio::pin!(future);
        loop {
            let changed = self.changed.notified();
            let Some(deadline) = self.deadline() else {
                return Ok(future.await);
            };
            let deadline_elapsed = tokio::time::sleep(deadline.remaining());
            tokio::pin!(changed);
            tokio::pin!(deadline_elapsed);

            tokio::select! {
                biased;
                output = &mut future => return Ok(output),
                () = &mut changed => {}
                () = &mut deadline_elapsed => return Err(ShutdownTimeout),
            }
        }
    }
}
