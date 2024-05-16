/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::{sync::oneshot, task::JoinHandle};

pub trait Runnable {
    fn run(&mut self);
}

pub struct ServiceThread<T: Runnable + Send + 'static> {
    name: String,
    runnable: Arc<Mutex<T>>,
    thread: Option<JoinHandle<()>>,
    stopped: Arc<Mutex<bool>>,
}

impl<T: Runnable + Send + 'static> ServiceThread<T> {
    pub fn new(name: String, runnable: T) -> Self {
        ServiceThread {
            name,
            runnable: Arc::new(Mutex::new(runnable)),
            thread: None,
            stopped: Arc::new(Mutex::new(false)),
        }
    }

    pub async fn start(&mut self) {
        let (tx, rx) = oneshot::channel();
        let stopped = self.stopped.clone();
        let runnable = self.runnable.clone();
        let name = self.name.clone();
        let join_handle = tokio::spawn(async move {
            log::info!("Starting service thread: {}", name);
            let _ = tx.send(());

            loop {
                if *stopped.lock() {
                    log::info!("Service thread stopped: {}", name);
                    break;
                }
                {
                    let mut guard = runnable.lock();
                    guard.run();
                }
            }
        });

        // Wait for the thread to start
        rx.await.expect("Failed to receive start signal");

        self.thread = Some(join_handle);
    }

    pub async fn shutdown(&mut self, interrupt: bool) {
        if let Some(thread) = &mut self.thread {
            log::info!("Shutting down service thread: {}", self.name);
            *self.stopped.lock() = true;

            if interrupt {
                // Not implemented: interrupt the thread
            }

            let result = thread.await;
            if let Err(err) = result {
                log::error!("Error joining service thread: {}", err);
            }
        } else {
            log::warn!("Service thread not started: {}", self.name);
        }
    }
}

pub trait ThreadService: Send + Sync + 'static {
    fn start(&self);

    fn shutdown(&self);

    fn get_service_name(&self) -> String;
}
