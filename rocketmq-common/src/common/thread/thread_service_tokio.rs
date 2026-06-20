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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::common::thread::Runnable;

const SERVICE_THREAD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub struct ServiceThreadTokio {
    name: String,
    runnable: Arc<Mutex<dyn Runnable>>,
    thread: Option<JoinHandle<()>>,
    stopped: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
    notified: Arc<Notify>,
}

impl ServiceThreadTokio {
    pub fn new(name: String, runnable: Arc<Mutex<dyn Runnable>>) -> Self {
        ServiceThreadTokio {
            name,
            runnable,
            thread: None,
            stopped: Arc::new(AtomicBool::new(false)),
            started: Arc::new(AtomicBool::new(false)),
            notified: Arc::new(Notify::new()),
        }
    }

    pub fn start(&mut self) {
        let started = self.started.clone();
        let runnable = self.runnable.clone();
        let name = self.name.clone();
        if let Ok(value) = started.compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed) {
            if value {
                return;
            }
        } else {
            return;
        }
        let join_handle = tokio::task::spawn_blocking(move || {
            info!("Starting service thread: {}", name);
            let mut guard = runnable.blocking_lock();
            guard.run();
        });
        self.thread = Some(join_handle);
    }

    pub fn make_stop(&mut self) {
        if !self.started.load(Ordering::Acquire) {
            return;
        }
        self.stopped.store(true, Ordering::Release);
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    pub async fn shutdown(&mut self) {
        self.shutdown_interrupt(false).await;
    }

    pub async fn shutdown_interrupt(&mut self, interrupt: bool) {
        self.shutdown_interrupt_with_timeout(interrupt, SERVICE_THREAD_SHUTDOWN_TIMEOUT)
            .await;
    }

    pub async fn shutdown_interrupt_with_timeout(&mut self, interrupt: bool, join_timeout: Duration) {
        if let Ok(value) = self
            .started
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::Relaxed)
        {
            if !value {
                return;
            }
        } else {
            return;
        }
        self.stopped.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            info!("Shutting down service thread: {}", self.name);
            let mut thread = thread;
            if interrupt {
                thread.abort();
            }
            match timeout(join_timeout, &mut thread).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => {
                    error!("Failed to join service thread {}: {}", self.name, error);
                }
                Err(_) => {
                    warn!(
                        "Timed out joining service thread {} after {}ms; spawn_blocking work may still be running",
                        self.name,
                        join_timeout.as_millis()
                    );
                    thread.abort();
                }
            }
        } else {
            warn!("Service thread not started: {}", self.name);
        }
    }

    pub fn wakeup(&self) {
        self.notified.notify_waiters();
    }

    pub async fn wait_for_running(&self, interval: u64) {
        tokio::select! {
            _ = self.notified.notified() => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(interval)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use mockall::automock;
    use tokio::time;
    use tokio::time::timeout;

    use super::*;
    use std::time::Duration;
    use std::time::Instant;

    struct MockTestRunnable;
    impl MockTestRunnable {
        fn new() -> MockTestRunnable {
            MockTestRunnable
        }
    }
    impl Runnable for MockTestRunnable {
        fn run(&mut self) {
            println!("MockTestRunnable run================")
        }
    }

    struct BlockingRunnable {
        started: Arc<AtomicBool>,
    }

    impl Runnable for BlockingRunnable {
        fn run(&mut self) {
            self.started.store(true, Ordering::Release);
            std::thread::sleep(Duration::from_millis(150));
        }
    }

    struct BlockingUntilReleaseRunnable {
        started: Arc<AtomicBool>,
        finished: Arc<AtomicBool>,
        release_rx: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
    }

    impl Runnable for BlockingUntilReleaseRunnable {
        fn run(&mut self) {
            self.started.store(true, Ordering::Release);
            let _ = self
                .release_rx
                .lock()
                .expect("release receiver mutex should not be poisoned")
                .recv();
            self.finished.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn test_start_and_shutdown() {
        let mock_runnable = MockTestRunnable::new();

        let mut service_thread =
            ServiceThreadTokio::new("TestServiceThread".to_string(), Arc::new(Mutex::new(mock_runnable)));

        service_thread.start();
        assert!(service_thread.started.load(Ordering::SeqCst));
        assert!(!service_thread.stopped.load(Ordering::SeqCst));

        time::sleep(std::time::Duration::from_secs(1)).await;
        service_thread.shutdown_interrupt(false).await;
        assert!(!service_thread.started.load(Ordering::SeqCst));
        assert!(service_thread.stopped.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_start_does_not_block_current_thread_runtime() {
        let started = Arc::new(AtomicBool::new(false));
        let mut service_thread = ServiceThreadTokio::new(
            "BlockingServiceThread".to_string(),
            Arc::new(Mutex::new(BlockingRunnable {
                started: Arc::clone(&started),
            })),
        );

        service_thread.start();
        let elapsed = Instant::now();
        tokio::task::yield_now().await;
        assert!(
            elapsed.elapsed() < Duration::from_millis(100),
            "Runnable::run should execute on Tokio's blocking pool"
        );

        timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("blocking runnable should start");

        service_thread.shutdown_interrupt(false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_interrupt_with_timeout_bounds_spawn_blocking_join() {
        let started = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut service_thread = ServiceThreadTokio::new(
            "BlockingUntilReleaseServiceThread".to_string(),
            Arc::new(Mutex::new(BlockingUntilReleaseRunnable {
                started: Arc::clone(&started),
                finished: Arc::clone(&finished),
                release_rx: Arc::new(std::sync::Mutex::new(release_rx)),
            })),
        );

        service_thread.start();
        timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("blocking runnable should start");

        let elapsed = Instant::now();
        service_thread
            .shutdown_interrupt_with_timeout(false, Duration::from_millis(10))
            .await;

        assert!(
            elapsed.elapsed() < Duration::from_millis(250),
            "shutdown should not wait indefinitely for already-running spawn_blocking work"
        );
        assert!(!service_thread.started.load(Ordering::SeqCst));
        assert!(service_thread.stopped.load(Ordering::SeqCst));

        release_tx
            .send(())
            .expect("release signal should be delivered to blocking runnable");
        timeout(Duration::from_secs(1), async {
            while !finished.load(Ordering::Acquire) {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("blocking runnable should finish after release");
    }

    #[tokio::test]
    async fn test_make_stop() {
        let mock_runnable = MockTestRunnable::new();
        let mut service_thread =
            ServiceThreadTokio::new("TestServiceThread".to_string(), Arc::new(Mutex::new(mock_runnable)));

        service_thread.start();
        service_thread.make_stop();
        assert!(service_thread.is_stopped());
    }

    #[tokio::test]
    async fn test_wait_for_running() {
        let mock_runnable = MockTestRunnable::new();
        let mut service_thread =
            ServiceThreadTokio::new("TestServiceThread".to_string(), Arc::new(Mutex::new(mock_runnable)));

        service_thread.start();
        service_thread.wait_for_running(100).await;
        assert!(service_thread.started.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_wakeup() {
        let mock_runnable = MockTestRunnable::new();
        let mut service_thread =
            ServiceThreadTokio::new("TestServiceThread".to_string(), Arc::new(Mutex::new(mock_runnable)));

        service_thread.start();
        service_thread.wakeup();
        // We expect that the wakeup method is called successfully.
    }
}
