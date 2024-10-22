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
use std::time::Duration;

use once_cell::sync::Lazy;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_rust::Shutdown;
use tokio::select;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::info;
use tracing::warn;

use crate::factory::mq_client_instance::MQClientInstance;

static WAIT_INTERVAL: Lazy<Duration> = Lazy::new(|| {
    std::env::var("rocketmq.client.rebalance.waitInterval")
        .unwrap_or_else(|_| "20000".into())
        .parse::<u64>()
        .map_or(Duration::from_millis(20000), |value| {
            Duration::from_millis(value)
        })
});

static MIN_INTERVAL: Lazy<Duration> = Lazy::new(|| {
    std::env::var("rocketmq.client.rebalance.minInterval")
        .unwrap_or_else(|_| "1000".into())
        .parse::<u64>()
        .map_or(Duration::from_millis(1000), |value| {
            Duration::from_millis(value)
        })
});

#[derive(Clone)]
pub struct RebalanceService {
    notify: Arc<Notify>,
    tx_shutdown: Option<tokio::sync::broadcast::Sender<()>>,
}

impl RebalanceService {
    pub fn new() -> Self {
        RebalanceService {
            notify: Arc::new(Notify::new()),
            tx_shutdown: None,
        }
    }

    pub async fn start(&mut self, mut instance: ArcRefCellWrapper<MQClientInstance>) {
        let notify = self.notify.clone();
        let (mut shutdown, tx_shutdown) = Shutdown::new(1);
        self.tx_shutdown = Some(tx_shutdown);
        tokio::spawn(async move {
            let mut last_rebalance_timestamp = Instant::now();
            let min_interval = *MIN_INTERVAL;
            let mut real_wait_interval = *WAIT_INTERVAL;
            info!(">>>>>>>>>RebalanceService started<<<<<<<<<");
            loop {
                select! {
                    _ = notify.notified() => {}
                    _ = shutdown.recv() => {info!("RebalanceService shutdown");}
                    _ = tokio::time::sleep(real_wait_interval) => {}
                }
                if shutdown.is_shutdown() {
                    return;
                }
                let interval = Instant::now() - last_rebalance_timestamp;
                if interval < min_interval {
                    real_wait_interval = min_interval - interval;
                } else {
                    let balanced = instance.do_rebalance().await;
                    real_wait_interval = if balanced {
                        *WAIT_INTERVAL
                    } else {
                        min_interval
                    };
                    last_rebalance_timestamp = Instant::now();
                }
            }
        });
    }

    pub fn wakeup(&self) {
        self.notify.notify_waiters();
    }

    pub fn shutdown(&self) {
        if let Some(tx_shutdown) = &self.tx_shutdown {
            if let Err(e) = tx_shutdown.send(()) {
                warn!("Failed to send shutdown signal to RebalanceService, error: {:?}", e);
            }
        } else {
            warn!("Shutdown called before start; no shutdown signal sent");
        }
    }
}
