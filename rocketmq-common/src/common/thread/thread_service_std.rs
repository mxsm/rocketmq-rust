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
use std::thread::JoinHandle;

use parking_lot::Mutex;
use tracing::info;

use crate::common::thread::Runnable;
use crate::common::thread::ServiceThread;

pub struct ServiceThreadStd {
    name: String,
    runnable: Arc<Mutex<dyn Runnable>>,
    thread: Option<JoinHandle<()>>,
    stopped: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
    notified: (parking_lot::Mutex<()>, parking_lot::Condvar),
}

impl ServiceThreadStd {
    pub fn new<T: Runnable>(name: String, runnable: T) -> Self {
        ServiceThreadStd {
            name,
            runnable: Arc::new(Mutex::new(runnable)),
            thread: None,
            stopped: Arc::new(AtomicBool::new(false)),
            started: Arc::new(AtomicBool::new(false)),
            notified: (Default::default(), Default::default()),
        }
    }
}

impl ServiceThreadStd {
    pub fn start(&mut self) {
        if let Ok(value) = self
            .started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
        {
            if value {
                return;
            }
        } else {
            return;
        }
        let name = self.name.clone();
        let runnable = self.runnable.clone();
        let stopped = self.stopped.clone();
        let thread = std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                info!("Starting service thread: {}", name);
                if stopped.load(std::sync::atomic::Ordering::Relaxed) {
                    info!("Service thread stopped: {}", name);
                    return;
                }
                runnable.lock().run();
            })
            .expect("Failed to start service thread");
        self.thread = Some(thread);
    }

    pub fn shutdown(&mut self) {
        self.shutdown_interrupt(false);
    }

    pub fn shutdown_interrupt(&mut self, interrupt: bool) {
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
        self.stopped.store(true, Ordering::Relaxed);
        self.wakeup();
        if let Some(thread) = self.thread.take() {
            if interrupt {
                drop(thread);
            } else {
                thread.join().expect("Failed to join service thread");
            }
        }
    }

    pub fn make_stop(&mut self) {
        if !self.started.load(Ordering::Acquire) {
            return;
        }
        self.stopped.store(true, Ordering::Release);
    }

    pub fn wakeup(&mut self) {
        self.notified.1.notify_all();
    }

    pub fn wait_for_running(&mut self, interval: i64) {
        let mut guard = self.notified.0.lock();
        self.notified
            .1
            .wait_for(&mut guard, std::time::Duration::from_millis(interval as u64));
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub fn get_service_name(&self) -> String {
        self.name.clone()
    }
}

#[cfg(test)]
mod tests {
    /*use mockall::{automock, predicate::*};

    use super::*;

    struct MockTestRunnable;
    impl MockTestRunnable {
        fn new() -> MockTestRunnable {
            MockTestRunnable
        }
    }
    impl Runnable for MockTestRunnable {
        fn run(&mut self, service_thread: &dyn ServiceThread) {}
    }

    #[test]
    fn test_start_and_shutdown() {
        let mock_runnable = MockTestRunnable::new();

        let mut service_thread =
            ServiceThreadStd::new("TestServiceThread".to_string(), mock_runnable);

        service_thread.start();
        assert!(service_thread.started.load(Ordering::SeqCst));
        assert!(!service_thread.stopped.load(Ordering::SeqCst));

        service_thread.shutdown_interrupt(false);
        assert!(!service_thread.started.load(Ordering::SeqCst));
        assert!(service_thread.stopped.load(Ordering::SeqCst));
    }

    #[test]
    fn test_make_stop() {
        let mock_runnable = MockTestRunnable::new();
        let mut service_thread =
            ServiceThreadStd::new("TestServiceThread".to_string(), mock_runnable);

        service_thread.start();
        service_thread.make_stop();
        assert!(service_thread.is_stopped());
    }

    #[test]
    fn test_wait_for_running() {
        let mock_runnable = MockTestRunnable::new();
        let mut service_thread =
            ServiceThreadStd::new("TestServiceThread".to_string(), mock_runnable);

        service_thread.start();
        service_thread.wait_for_running(100);
        assert!(service_thread.started.load(Ordering::SeqCst));
    }

    #[test]
    fn test_wakeup() {
        let mock_runnable = MockTestRunnable::new();
        let mut service_thread =
            ServiceThreadStd::new("TestServiceThread".to_string(), mock_runnable);

        service_thread.start();
        service_thread.wakeup();
        // We expect that the wakeup method is called successfully.
    }*/
}
