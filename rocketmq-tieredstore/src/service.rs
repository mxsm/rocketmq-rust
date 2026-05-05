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

pub mod cleanup_service;

use std::sync::Arc;

use rocketmq_error::RocketMQError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::TieredStoreConfig;
use crate::file::TieredFlatFileStore;
use crate::provider::TieredStoreProvider;
use crate::service::cleanup_service::CleanupService;

pub struct TieredServiceSet<P>
where
    P: TieredStoreProvider,
{
    cleanup_handle: tokio::sync::Mutex<Option<JoinHandle<Result<(), RocketMQError>>>>,
    _marker: std::marker::PhantomData<P>,
}

impl<P> TieredServiceSet<P>
where
    P: TieredStoreProvider,
{
    pub fn new() -> Self {
        Self {
            cleanup_handle: tokio::sync::Mutex::new(None),
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn start_cleanup(
        &self,
        config: Arc<TieredStoreConfig>,
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        shutdown: CancellationToken,
    ) -> Result<(), RocketMQError> {
        if !config.delete_file_enable {
            return Ok(());
        }
        let service = CleanupService::new(config, flat_file_store, shutdown);
        *self.cleanup_handle.lock().await = Some(tokio::spawn(service.run()));
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), RocketMQError> {
        if let Some(handle) = self.cleanup_handle.lock().await.take() {
            handle.await.map_err(|err| RocketMQError::Internal(err.to_string()))??;
        }
        Ok(())
    }
}

impl<P> Default for TieredServiceSet<P>
where
    P: TieredStoreProvider,
{
    fn default() -> Self {
        Self::new()
    }
}
