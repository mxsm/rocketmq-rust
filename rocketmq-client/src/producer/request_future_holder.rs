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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::task;
use tokio::time::interval;

use crate::producer::request_response_future::RequestResponseFuture;

pub static REQUEST_FUTURE_HOLDER: Lazy<Arc<RequestFutureHolder>> = Lazy::new(|| Arc::new(RequestFutureHolder::new()));

pub struct RequestFutureHolder {
    request_future_table: Arc<RwLock<HashMap<String, Arc<RequestResponseFuture>>>>,
    producer_set: Arc<Mutex<HashSet<String>>>,
}

impl RequestFutureHolder {
    fn new() -> Self {
        Self {
            request_future_table: Arc::new(RwLock::new(HashMap::new())),
            producer_set: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn scan_expired_request(&self) {
        let mut rf_list = Vec::new();
        {
            let mut table = self.request_future_table.write().await;
            let mut expired_keys = Vec::new();

            for (key, future) in table.iter() {
                if future.is_timeout() {
                    expired_keys.push(key.clone());
                    rf_list.push(future.clone());
                }
            }

            for key in expired_keys {
                table.remove(&key);
            }
        }

        for rf in rf_list {
            let cause = Box::new(rocketmq_error::RocketMQError::Timeout {
                operation: "request_reply",
                timeout_ms: 3000, // default timeout
            });
            rf.set_cause(cause);
            rf.execute_request_callback().await;
        }
    }

    pub async fn start_scheduled_task(self: Arc<Self>) {
        let holder = self;
        task::spawn(async move {
            let mut interval = interval(Duration::from_millis(1000));
            loop {
                interval.tick().await;
                holder.scan_expired_request().await;
            }
        });
    }

    pub async fn shutdown(&self) {
        let mut producers = self.producer_set.lock().await;
        producers.clear(); // Simulating producer removal

        // Additional shutdown logic if needed
    }

    pub async fn put_request(&self, correlation_id: String, request: Arc<RequestResponseFuture>) {
        let mut table = self.request_future_table.write().await;
        table.insert(correlation_id, request);
    }

    pub async fn remove_request(&self, correlation_id: &str) {
        let mut table = self.request_future_table.write().await;
        table.remove(correlation_id);
    }

    pub async fn get_request(&self, correlation_id: &str) -> Option<Arc<RequestResponseFuture>> {
        let table = self.request_future_table.read().await;
        table.get(correlation_id).cloned()
    }
}
