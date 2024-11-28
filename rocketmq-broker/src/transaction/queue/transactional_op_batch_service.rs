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

use rocketmq_common::TimeUtils::get_current_millis;
use tokio::sync::Notify;
use tracing::info;

#[derive(Default, Clone)]
pub struct TransactionalOpBatchService {
    notify: Arc<Notify>,
}

impl TransactionalOpBatchService {
    pub fn new() -> Self {
        TransactionalOpBatchService {
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn wakeup(&self) {
        self.notify.notify_waiters();
    }

    pub fn run(&self, transaction_op_batch_interval: u64) {
        let this = self.clone();
        tokio::spawn(async move {
            info!("TransactionalOpBatchService started");
            let wakeup_timestamp = get_current_millis() + transaction_op_batch_interval;
            loop {
                let mut interval = (wakeup_timestamp as i64) - get_current_millis() as i64;
                if interval <= 0 {
                    interval = 0;
                    this.wakeup();
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(interval as u64)).await;
            }
        });
    }
}
