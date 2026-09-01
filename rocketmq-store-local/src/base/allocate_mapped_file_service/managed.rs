// Copyright 2026 The RocketMQ Rust Authors
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

use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_runtime::ResourcePermit;
use thiserror::Error;

use crate::base::transient_store_pool::TransientStorePool;
use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::ManagedIncarnationCreateRequest;
use crate::mapped_file::ManagedLifecycleRuntime;
use crate::mapped_file::ManagedMappedFileQueueGeneration;

#[derive(Clone)]
pub(super) struct ManagedAllocationContext {
    runtime: ManagedLifecycleRuntime,
    store_root: PathBuf,
}

impl ManagedAllocationContext {
    pub(super) const fn new(runtime: ManagedLifecycleRuntime, store_root: PathBuf) -> Self {
        Self { runtime, store_root }
    }
}

/// Failure returned by the Store-owned managed allocation worker.
#[doc(hidden)]
#[derive(Debug, Error)]
pub(crate) enum ManagedMappedFileAllocationFailure {
    #[error("managed lifecycle allocation worker is not running")]
    WorkerUnavailable,
    #[error("managed lifecycle authority is not installed on the allocation worker")]
    LifecycleUnavailable,
    #[error("managed mapped-file size must be positive, got {0}")]
    InvalidFileSize(u64),
    #[error("managed queue path is outside the reconciled Store root")]
    QueueOutsideStoreRoot,
    #[error("managed queue path contains a non-canonical component")]
    InvalidQueuePath,
    #[error("managed mapped-file allocation budget rejected the request: {0}")]
    Budget(#[source] rocketmq_runtime::BudgetAcquireError),
    #[error(transparent)]
    Store(#[from] rocketmq_store_api::StoreError),
}

pub(super) struct ManagedAllocationRequest {
    runtime: ManagedLifecycleRuntime,
    queue: ManagedMappedFileQueueGeneration<DefaultMappedFile>,
    request: Mutex<Option<ManagedIncarnationCreateRequest>>,
    result: Mutex<Option<Result<Arc<DefaultMappedFile>, ManagedMappedFileAllocationFailure>>>,
    completion: Condvar,
    _permit: ResourcePermit,
}

impl ManagedAllocationRequest {
    #[allow(
        clippy::too_many_arguments,
        reason = "the worker request binds one exact queue, lifecycle runtime, segment, nonce, pool, and budget permit"
    )]
    pub(super) fn new(
        context: ManagedAllocationContext,
        queue: ManagedMappedFileQueueGeneration<DefaultMappedFile>,
        queue_path: &Path,
        segment_offset: u64,
        file_size: u64,
        request_id: u64,
        transient_store_pool: Option<TransientStorePool>,
        permit: ResourcePermit,
    ) -> Result<Self, ManagedMappedFileAllocationFailure> {
        let directory = relative_directory(&context.store_root, queue_path)?;
        let mut request =
            ManagedIncarnationCreateRequest::new(&directory, segment_offset, file_size, creation_nonce(request_id))
                .ok_or(ManagedMappedFileAllocationFailure::InvalidFileSize(file_size))?;
        if let Some(pool) = transient_store_pool {
            request = request.with_transient_store_pool(pool);
        }
        Ok(Self {
            runtime: context.runtime,
            queue,
            request: Mutex::new(Some(request)),
            result: Mutex::new(None),
            completion: Condvar::new(),
            _permit: permit,
        })
    }

    pub(super) fn execute(&self) {
        let request = self
            .request
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let result = match request {
            Some(request) => match self.runtime.create_mapped_file(&self.queue, request) {
                Ok(Some(creation)) => Ok(creation.into_mapped_file()),
                Ok(None) => Err(ManagedMappedFileAllocationFailure::InvalidQueuePath),
                Err(error) => Err(ManagedMappedFileAllocationFailure::Store(error)),
            },
            None => Err(ManagedMappedFileAllocationFailure::WorkerUnavailable),
        };
        self.complete(result);
    }

    pub(super) fn cancel(&self) {
        self.complete(Err(ManagedMappedFileAllocationFailure::WorkerUnavailable));
    }

    pub(super) fn wait(
        &self,
        worker_completed: &AtomicBool,
    ) -> Result<Arc<DefaultMappedFile>, ManagedMappedFileAllocationFailure> {
        let mut result = self.result.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            if let Some(result) = result.take() {
                return result;
            }
            if worker_completed.load(Ordering::Acquire) {
                return Err(ManagedMappedFileAllocationFailure::WorkerUnavailable);
            }
            let waited = self
                .completion
                .wait_timeout(result, Duration::from_millis(100))
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            result = waited.0;
        }
    }

    fn complete(&self, value: Result<Arc<DefaultMappedFile>, ManagedMappedFileAllocationFailure>) {
        let mut result = self.result.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if result.is_none() {
            *result = Some(value);
            self.completion.notify_all();
        }
    }
}

fn relative_directory(store_root: &Path, queue_path: &Path) -> Result<String, ManagedMappedFileAllocationFailure> {
    let relative = queue_path
        .strip_prefix(store_root)
        .map_err(|_| ManagedMappedFileAllocationFailure::QueueOutsideStoreRoot)?;
    let mut components = Vec::new();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(ManagedMappedFileAllocationFailure::InvalidQueuePath);
        };
        let component = component
            .to_str()
            .filter(|component| !component.is_empty())
            .ok_or(ManagedMappedFileAllocationFailure::InvalidQueuePath)?;
        components.push(component);
    }
    if components.is_empty() {
        return Err(ManagedMappedFileAllocationFailure::InvalidQueuePath);
    }
    Ok(components.join("/"))
}

fn creation_nonce(request_id: u64) -> [u8; 16] {
    creation_nonce_from_seed(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        request_id,
    )
}

fn creation_nonce_from_seed(seed: u128, request_id: u64) -> [u8; 16] {
    let mut nonce = seed.to_le_bytes();
    for (target, source) in nonce[8..].iter_mut().zip(request_id.to_le_bytes()) {
        *target ^= source;
    }
    if nonce == [0; 16] {
        nonce[0] = 1;
    }
    nonce
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_queue_directory_is_canonical_and_beneath_store_root() {
        let root = Path::new("store-root");

        assert_eq!(
            relative_directory(root, &root.join("consumequeue").join("topic-a").join("3"))
                .expect("canonical queue path"),
            "consumequeue/topic-a/3"
        );
        assert!(matches!(
            relative_directory(root, Path::new("another-root/commitlog")),
            Err(ManagedMappedFileAllocationFailure::QueueOutsideStoreRoot)
        ));
        assert!(matches!(
            relative_directory(root, root),
            Err(ManagedMappedFileAllocationFailure::InvalidQueuePath)
        ));
    }

    #[test]
    fn managed_creation_nonce_is_nonzero_and_request_bound() {
        let first = creation_nonce_from_seed(73, 41);
        let second = creation_nonce_from_seed(73, 42);

        assert_ne!(first, [0; 16]);
        assert_ne!(second, [0; 16]);
        assert_ne!(first, second);
    }
}
