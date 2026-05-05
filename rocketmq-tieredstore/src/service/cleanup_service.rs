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

use std::sync::Arc;

use rocketmq_error::RocketMQError;
use tokio::time::interval;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::config::TieredStoreConfig;
use crate::file::TieredFlatFileStore;
use crate::provider::TieredStoreProvider;

pub struct CleanupService<P>
where
    P: TieredStoreProvider,
{
    config: Arc<TieredStoreConfig>,
    flat_file_store: Arc<TieredFlatFileStore<P>>,
    shutdown: CancellationToken,
}

impl<P> CleanupService<P>
where
    P: TieredStoreProvider,
{
    pub fn new(
        config: Arc<TieredStoreConfig>,
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            config,
            flat_file_store,
            shutdown,
        }
    }

    pub async fn run(self) -> Result<(), RocketMQError> {
        let mut ticker = interval(self.config.delete_file_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    break;
                }
                _ = ticker.tick() => {
                    self.flat_file_store.cleanup_expired(current_time_millis()).await?;
                }
            }
        }
        Ok(())
    }
}

fn current_time_millis() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}
