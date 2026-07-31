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

use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex as StdMutex;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use parking_lot::RwLock;
use rocketmq_error::RocketMQError;
use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::BudgetSnapshot;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourcePermit;
use tokio::sync::Notify;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::transient_store_pool::TransientStorePool;
use crate::mapped_file::allocation_policy::mapped_file_allocation_capacity;
use crate::mapped_file::allocation_policy::MappedFileAllocationPoolSnapshot;
use crate::mapped_file::allocation_policy::MappedFileWarmupConfig;
use crate::mapped_file::allocation_request::MappedFileAllocationRequestKey;
use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::MappedFile;

/// Timeout for waiting on file allocation (matches Java: 5 seconds)
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounded mapped-file allocation queue diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MappedFileAllocationQueueSnapshot {
    /// Requests currently holding mapped-file allocation budget.
    pub current_count: usize,
    /// File bytes charged to accepted requests.
    pub charged_bytes: usize,
    /// Requests waiting in the priority heap, including harmless stale keys.
    pub queued_count: usize,
    /// Age of the oldest request still owned by the table.
    pub oldest_age: Option<Duration>,
    /// Requests rejected by count or byte admission.
    pub rejected_count: u64,
    /// Accepted requests explicitly abandoned by timeout or shutdown.
    pub abandoned_count: u64,
}

struct WorkerCompletion {
    completed: Arc<AtomicBool>,
    notification: Arc<Notify>,
}

impl Drop for WorkerCompletion {
    fn drop(&mut self) {
        self.completed.store(true, Ordering::Release);
        self.notification.notify_one();
    }
}

/// Projects facade-owned configuration into the Local allocation service.
#[doc(hidden)]
pub trait AllocateMappedFileServiceConfig {
    fn mapped_file_warmup_config(&self) -> MappedFileWarmupConfig;
}

/// Background service for asynchronous MappedFile pre-allocation
///
/// Corresponds to Java's `AllocateMappedFileService`:
/// - Uses priority queue for ordered file allocation
/// - Supports TransientStorePool integration
/// - Pre-allocates next and next-next files
/// - Implements CountDownLatch-like synchronization
pub struct AllocateMappedFileService {
    /// Request table: file_path -> AllocateRequest
    request_table: Arc<RwLock<HashMap<String, Arc<AllocateRequest>>>>,

    /// Priority queue for ordered processing
    request_queue: Arc<RwLock<BinaryHeap<AllocationQueueEntry>>>,

    /// Count and charged-file-byte budget derived from the Store runtime.
    allocation_budget: ResourceBudget,

    /// Accepted requests removed before normal completion.
    abandoned_count: Arc<AtomicU64>,

    /// Exception flag (set when allocation fails)
    has_exception: Arc<AtomicBool>,

    /// Shutdown flag
    stopped: Arc<AtomicBool>,

    /// Notification for new requests
    notify: Arc<Notify>,

    /// Blocking worker wakeup, used instead of creating an internal Tokio runtime.
    worker_wakeup: Arc<(StdMutex<()>, Condvar)>,

    /// Background worker handle
    worker_handle: Arc<parking_lot::Mutex<Option<thread::JoinHandle<()>>>>,

    /// Completion state used to await the dedicated worker without a blocking join task.
    worker_completed: Arc<AtomicBool>,

    /// Notification emitted when the dedicated worker exits or unwinds.
    worker_completion: Arc<Notify>,

    /// TransientStorePool reference (optional)
    transient_store_pool: Option<Arc<TransientStorePool>>,

    /// Whether to enable TransientStorePool
    transient_store_pool_enable: bool,

    /// Whether to fast fail when no buffer available in pool
    fast_fail_if_no_buffer: bool,

    /// CommitLog warm-up behavior copied from MessageStoreConfig.
    warm_mapped_file_config: MappedFileWarmupConfig,

    #[cfg(feature = "observability")]
    store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder,
}

impl Clone for AllocateMappedFileService {
    fn clone(&self) -> Self {
        Self {
            request_table: self.request_table.clone(),
            request_queue: self.request_queue.clone(),
            allocation_budget: self.allocation_budget.clone(),
            abandoned_count: self.abandoned_count.clone(),
            has_exception: self.has_exception.clone(),
            stopped: self.stopped.clone(),
            notify: self.notify.clone(),
            worker_wakeup: self.worker_wakeup.clone(),
            worker_handle: self.worker_handle.clone(),
            worker_completed: self.worker_completed.clone(),
            worker_completion: self.worker_completion.clone(),
            transient_store_pool: self.transient_store_pool.clone(),
            transient_store_pool_enable: self.transient_store_pool_enable,
            fast_fail_if_no_buffer: self.fast_fail_if_no_buffer,
            warm_mapped_file_config: self.warm_mapped_file_config,
            #[cfg(feature = "observability")]
            store_metrics: self.store_metrics.clone(),
        }
    }
}

impl AllocateMappedFileService {
    /// Create a new AllocateMappedFileService with full configuration
    ///
    /// # Arguments
    /// * `transient_store_pool` - Optional TransientStorePool for zero-copy
    /// * `transient_store_pool_enable` - Whether TransientStorePool is enabled
    /// * `fast_fail_if_no_buffer` - Whether to fast fail when pool is exhausted
    pub fn new_with_config(
        transient_store_pool: Option<Arc<TransientStorePool>>,
        transient_store_pool_enable: bool,
        fast_fail_if_no_buffer: bool,
        allocation_budget: ResourceBudget,
    ) -> Self {
        let request_table = Arc::new(RwLock::new(HashMap::new()));
        let request_queue = Arc::new(RwLock::new(BinaryHeap::new()));
        let has_exception = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());
        let worker_wakeup = Arc::new((StdMutex::new(()), Condvar::new()));

        Self {
            request_table,
            request_queue,
            allocation_budget,
            abandoned_count: Arc::new(AtomicU64::new(0)),
            has_exception,
            stopped,
            notify,
            worker_wakeup,
            worker_handle: Arc::new(parking_lot::Mutex::new(None)),
            worker_completed: Arc::new(AtomicBool::new(true)),
            worker_completion: Arc::new(Notify::new()),
            transient_store_pool,
            transient_store_pool_enable,
            fast_fail_if_no_buffer,
            warm_mapped_file_config: MappedFileWarmupConfig::disabled(),
            #[cfg(feature = "observability")]
            store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder::noop(),
        }
    }

    /// Binds mapped-file allocation observations to the owning Store recorder.
    #[cfg(feature = "observability")]
    #[doc(hidden)]
    pub fn with_store_metrics(
        mut self,
        store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder,
    ) -> Self {
        self.store_metrics = store_metrics;
        self
    }

    pub fn new_with_message_store_config<C>(
        transient_store_pool: Option<Arc<TransientStorePool>>,
        transient_store_pool_enable: bool,
        fast_fail_if_no_buffer: bool,
        message_store_config: &C,
        allocation_budget: ResourceBudget,
    ) -> Self
    where
        C: AllocateMappedFileServiceConfig,
    {
        let mut service = Self::new_with_config(
            transient_store_pool,
            transient_store_pool_enable,
            fast_fail_if_no_buffer,
            allocation_budget,
        );
        service.warm_mapped_file_config = message_store_config.mapped_file_warmup_config();
        service
    }

    pub fn is_started(&self) -> bool {
        self.worker_handle.lock().is_some() && !self.stopped.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn should_warm_mapped_file(&self, file_size: u64) -> bool {
        self.warm_mapped_file_config.should_warm(file_size)
    }

    /// Start the background worker thread
    /// Corresponds to Java's ServiceThread.start()
    pub fn start(&self) {
        {
            let worker_handle = self.worker_handle.lock();
            if worker_handle.is_some() {
                return;
            }
        }

        self.stopped.store(false, Ordering::Relaxed);
        self.worker_completed.store(false, Ordering::Release);

        let request_table = self.request_table.clone();
        let request_queue = self.request_queue.clone();
        let has_exception = self.has_exception.clone();
        let stopped = self.stopped.clone();
        let transient_store_pool = self.transient_store_pool.clone();
        let worker_wakeup = self.worker_wakeup.clone();
        let warm_mapped_file_config = self.warm_mapped_file_config;
        #[cfg(feature = "observability")]
        let store_metrics = self.store_metrics.clone();
        let worker_completed = self.worker_completed.clone();
        let worker_completion = self.worker_completion.clone();

        match thread::Builder::new()
            .name("allocate-mapped-file-service".to_string())
            .spawn(move || {
                let _completion = WorkerCompletion {
                    completed: worker_completed,
                    notification: worker_completion,
                };
                Self::run_worker(
                    request_table,
                    request_queue,
                    has_exception,
                    stopped,
                    transient_store_pool,
                    worker_wakeup,
                    warm_mapped_file_config,
                    #[cfg(feature = "observability")]
                    store_metrics,
                );
            }) {
            Ok(handle) => {
                *self.worker_handle.lock() = Some(handle);
                info!("AllocateMappedFileService started");
            }
            Err(error) => {
                self.worker_completed.store(true, Ordering::Release);
                self.has_exception.store(true, Ordering::Relaxed);
                error!("AllocateMappedFileService failed to start worker thread: {}", error);
            }
        }
    }

    /// Main worker loop - corresponds to Java's run() method
    fn run_worker(
        request_table: Arc<RwLock<HashMap<String, Arc<AllocateRequest>>>>,
        request_queue: Arc<RwLock<BinaryHeap<AllocationQueueEntry>>>,
        has_exception: Arc<AtomicBool>,
        stopped: Arc<AtomicBool>,
        transient_store_pool: Option<Arc<TransientStorePool>>,
        worker_wakeup: Arc<(StdMutex<()>, Condvar)>,
        warm_mapped_file_config: MappedFileWarmupConfig,
        #[cfg(feature = "observability")] store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder,
    ) {
        info!("AllocateMappedFileService: service started");

        while !stopped.load(Ordering::Relaxed) {
            while !stopped.load(Ordering::Relaxed)
                && Self::mmap_operation(
                    &request_table,
                    &request_queue,
                    &has_exception,
                    &transient_store_pool,
                    warm_mapped_file_config,
                    #[cfg(feature = "observability")]
                    &store_metrics,
                )
            {}

            if stopped.load(Ordering::Relaxed) {
                break;
            }

            if request_queue.read().is_empty() {
                let (lock, condvar) = &*worker_wakeup;
                let guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                match condvar.wait_timeout(guard, Duration::from_millis(100)) {
                    Ok((_guard, _timeout)) => {}
                    Err(poisoned) => {
                        let (_guard, _timeout) = poisoned.into_inner();
                    }
                }
            }
        }

        info!("AllocateMappedFileService: service end");
    }

    /// Core file allocation operation - corresponds to Java's mmapOperation()
    ///
    /// Returns false if interrupted or no requests available
    fn mmap_operation(
        request_table: &Arc<RwLock<HashMap<String, Arc<AllocateRequest>>>>,
        request_queue: &Arc<RwLock<BinaryHeap<AllocationQueueEntry>>>,
        has_exception: &Arc<AtomicBool>,
        transient_store_pool: &Option<Arc<TransientStorePool>>,
        warm_mapped_file_config: MappedFileWarmupConfig,
        #[cfg(feature = "observability")] store_metrics: &rocketmq_observability::metrics::store::StoreMetricsRecorder,
    ) -> bool {
        // Pop request from priority queue
        let req = {
            let mut queue = request_queue.write();
            queue.pop()
        };

        let entry = match req {
            Some(entry) => entry,
            None => return false, // No requests available
        };

        // Check if request still valid in table
        let expected_request = {
            let table = request_table.read();
            table.get(entry.file_path()).cloned()
        };

        let req = match expected_request {
            Some(request) if request.file_size() == entry.file_size() => request,
            None => {
                warn!(
                    "this mmap request expired, maybe cause timeout {} {}",
                    entry.file_path(),
                    entry.file_size()
                );
                return true;
            }
            Some(_) => return true,
        };

        // Check if already allocated
        if req.mapped_file.read().is_some() {
            return true;
        }

        // Perform actual file allocation
        let result = Self::create_mapped_file(
            &req,
            transient_store_pool,
            warm_mapped_file_config,
            #[cfg(feature = "observability")]
            store_metrics,
        );

        match result {
            Ok(mapped_file) => {
                let request_is_owned = request_table
                    .read()
                    .get(req.file_path())
                    .is_some_and(|request| Arc::ptr_eq(request, &req));
                if !request_is_owned {
                    mapped_file.destroy(1000);
                    req.complete();
                    return true;
                }
                *req.mapped_file.write() = Some(mapped_file);
                let request_is_still_owned = request_table
                    .read()
                    .get(req.file_path())
                    .is_some_and(|request| Arc::ptr_eq(request, &req));
                if !request_is_still_owned {
                    if let Some(mapped_file) = req.mapped_file.write().take() {
                        mapped_file.destroy(1000);
                    }
                    req.complete();
                    return true;
                }
                has_exception.store(false, Ordering::Relaxed);

                // Signal completion (like CountDownLatch.countDown())
                req.complete();

                true
            }
            Err(e) => {
                error!(
                    "AllocateMappedFileService: failed to create mapped file {}: {}",
                    req.file_path(),
                    e
                );
                has_exception.store(true, Ordering::Relaxed);

                // Re-queue the request for retry
                request_queue.write().push(entry);

                // Small delay before retry
                thread::sleep(Duration::from_millis(1));

                false
            }
        }
    }

    /// Create a MappedFile with optional TransientStorePool
    ///
    /// Corresponds to Java's MappedFile creation logic in mmapOperation()
    fn create_mapped_file(
        req: &AllocateRequest,
        transient_store_pool: &Option<Arc<TransientStorePool>>,
        warm_mapped_file_config: MappedFileWarmupConfig,
        #[cfg(feature = "observability")] store_metrics: &rocketmq_observability::metrics::store::StoreMetricsRecorder,
    ) -> Result<Arc<DefaultMappedFile>, RocketMQError> {
        let start = std::time::Instant::now();
        let file_path = req.file_path().to_owned();
        let file_size = req.file_size() as u64;
        let transient_pool = transient_store_pool.clone();

        let mapped_file = if let Some(pool) = transient_pool {
            // With TransientStorePool (zero-copy)
            DefaultMappedFile::try_new_with_transient_store_pool(
                CheetahString::from_string(file_path.clone()),
                file_size,
                (*pool).clone(),
            )
        } else {
            // Standard mmap
            DefaultMappedFile::try_new(CheetahString::from_string(file_path.clone()), file_size)
        }
        .map_err(|error| RocketMQError::StorageWriteFailed {
            path: req.file_path().to_owned(),
            reason: error.to_string(),
        })?;
        #[cfg(feature = "observability")]
        let mapped_file = mapped_file.with_store_metrics(store_metrics.clone());

        if warm_mapped_file_config.should_warm(file_size) {
            mapped_file.warm_mapped_file(
                warm_mapped_file_config.flush_disk_type(),
                warm_mapped_file_config.flush_least_pages(),
            );
        }

        let elapsed = start.elapsed();
        if elapsed.as_millis() > 10 {
            let queue_size = 0; // TODO: pass queue size if needed
            warn!(
                "create mappedFile spent time(ms) {} queue size {} {} {}",
                elapsed.as_millis(),
                queue_size,
                req.file_path(),
                req.file_size()
            );
        }

        Ok(Arc::new(mapped_file))
    }

    fn notify_worker(&self) {
        self.notify.notify_one();
        let (_, condvar) = &*self.worker_wakeup;
        condvar.notify_one();
    }

    fn admit_request(&self, file_path: String, file_size: i32) -> AllocationAdmission {
        if let Some(existing) = self.request_table.read().get(&file_path) {
            return AllocationAdmission::Existing(existing.clone());
        }
        let charged_bytes = usize::try_from(file_size).unwrap_or_default().max(1);
        let permit = match self.allocation_budget.try_acquire(charged_bytes, BudgetClass::Data) {
            Ok(permit) => permit,
            Err(error) => {
                warn!(
                    queue = "mapped-file-allocation",
                    reason = %error,
                    charged_bytes,
                    "mapped-file allocation request rejected by Store budget"
                );
                return AllocationAdmission::Rejected;
            }
        };
        let mut table = self.request_table.write();
        if let Some(existing) = table.get(&file_path) {
            return AllocationAdmission::Existing(existing.clone());
        }
        let request = Arc::new(AllocateRequest::new(file_path.clone(), file_size, permit));
        table.insert(file_path, request.clone());
        drop(table);
        self.request_queue
            .write()
            .push(AllocationQueueEntry::from_request(&request));
        self.notify_worker();
        AllocationAdmission::Inserted(request)
    }

    fn remove_request_if_owned(&self, request: &Arc<AllocateRequest>, abandoned: bool) {
        let mut table = self.request_table.write();
        let removed = if table
            .get(request.file_path())
            .is_some_and(|current| Arc::ptr_eq(current, request))
        {
            table.remove(request.file_path());
            if abandoned {
                self.abandoned_count.fetch_add(1, Ordering::Relaxed);
            }
            true
        } else {
            false
        };
        drop(table);

        if removed && abandoned {
            if let Some(mapped_file) = request.mapped_file.write().take() {
                mapped_file.destroy(1000);
            }
        }
    }

    /// Returns count, charged bytes, oldest age, rejection, and abandonment
    /// diagnostics for the mapped-file allocation boundary.
    #[must_use]
    pub fn queue_snapshot(&self) -> MappedFileAllocationQueueSnapshot {
        let budget: BudgetSnapshot = self.allocation_budget.snapshot();
        let table = self.request_table.read();
        let oldest_age = table.values().map(|request| request.enqueued_at.elapsed()).max();
        MappedFileAllocationQueueSnapshot {
            current_count: budget.current_count,
            charged_bytes: budget.current_bytes,
            queued_count: self.request_queue.read().len(),
            oldest_age,
            rejected_count: budget.rejected_count,
            abandoned_count: self.abandoned_count.load(Ordering::Relaxed),
        }
    }

    /// Submit pre-allocation request and wait for result
    ///
    /// **This is the primary API - corresponds to Java's `putRequestAndReturnMappedFile()`**
    ///
    /// # Arguments
    /// * `next_file_path` - Path for the next file to allocate
    /// * `next_next_file_path` - Path for the file after next (pre-allocation)
    /// * `file_size` - Size of each file
    ///
    /// # Returns
    /// * `Ok(Some(MappedFile))` - Successfully allocated file
    /// * `Ok(None)` - Cannot allocate (pool exhausted, exception, etc.)
    /// * `Err(...)` - Error occurred
    pub async fn put_request_and_return_mapped_file(
        &self,
        next_file_path: String,
        next_next_file_path: String,
        file_size: i32,
    ) -> Result<Option<Arc<DefaultMappedFile>>, RocketMQError> {
        // Check available buffer capacity if using TransientStorePool
        let mut can_submit_requests = self.allocation_capacity(2);
        let existing_request = self.request_table.read().get(&next_file_path).cloned();
        if can_submit_requests == 0 && existing_request.is_none() {
            warn!(
                "[NOTIFYME]TransientStorePool is not enough, so create mapped file error, RequestQueueSize: {}, \
                 StorePoolSize: {}",
                self.request_queue.read().len(),
                self.transient_store_pool
                    .as_ref()
                    .map_or(0, |pool| pool.available_buffer_nums())
            );
            return Ok(None);
        }

        // Submit request for next file.
        let next_req = match existing_request {
            Some(request) => request,
            None => match self.admit_request(next_file_path.clone(), file_size) {
                AllocationAdmission::Inserted(request) => {
                    can_submit_requests -= 1;
                    request
                }
                AllocationAdmission::Existing(request) => request,
                AllocationAdmission::Rejected => return Ok(None),
            },
        };

        // Submit request for next-next file (pre-allocation)
        if !next_next_file_path.is_empty() {
            if can_submit_requests == 0 {
                warn!(
                    "[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, \
                     RequestQueueSize: {}, StorePoolSize: {}",
                    self.request_queue.read().len(),
                    self.transient_store_pool
                        .as_ref()
                        .map_or(0, |pool| pool.available_buffer_nums())
                );
            } else if let AllocationAdmission::Rejected = self.admit_request(next_next_file_path, file_size) {
                warn!("mapped-file preallocation skipped because its Store budget is exhausted");
            }
        }

        // Check for exceptions
        if self.has_exception.load(Ordering::Relaxed) {
            warn!("AllocateMappedFileService has exception, so return null");
            return Ok(None);
        }

        // Wait for allocation to complete (with timeout).
        let mut wait_guard = AllocationWaitGuard::new(self.clone(), next_req.clone());
        let wait_result = tokio::time::timeout(WAIT_TIMEOUT, next_req.wait()).await;

        match wait_result {
            Ok(()) => {
                // Remove from table and return result
                wait_guard.finish(false);
                let mapped_file = next_req.mapped_file.read().clone();
                Ok(mapped_file)
            }
            Err(_) => {
                warn!("create mmap timeout {} {}", next_req.file_path(), next_req.file_size());
                wait_guard.finish(true);
                Ok(None)
            }
        }
    }

    /// Simple allocation without pre-allocation (for single files)
    ///
    /// This is a compatibility method for tests and simple scenarios
    pub async fn submit_request(
        &self,
        file_path: String,
        file_size: u64,
    ) -> Result<Arc<DefaultMappedFile>, RocketMQError> {
        // Use empty string for next-next file (won't be allocated)
        let result = self
            .put_request_and_return_mapped_file(
                file_path.clone(),
                String::new(), // No pre-allocation
                file_size as i32,
            )
            .await?;

        result.ok_or_else(|| RocketMQError::StorageWriteFailed {
            path: file_path.clone(),
            reason: "Allocation failed or timed out".to_string(),
        })
    }

    /// Synchronous allocation method (compatibility wrapper)
    pub async fn allocate_mapped_file(
        &self,
        file_path: String,
        file_size: u64,
    ) -> Result<Arc<DefaultMappedFile>, RocketMQError> {
        self.submit_request(file_path, file_size).await
    }

    pub fn allocate_mapped_file_blocking(
        &self,
        file_path: String,
        file_size: u64,
    ) -> Result<Arc<DefaultMappedFile>, RocketMQError> {
        let result =
            self.put_request_and_return_mapped_file_blocking(file_path.clone(), String::new(), file_size as i32)?;

        result.ok_or_else(|| RocketMQError::StorageWriteFailed {
            path: file_path,
            reason: "Allocation failed or timed out".to_string(),
        })
    }

    fn put_request_and_return_mapped_file_blocking(
        &self,
        next_file_path: String,
        next_next_file_path: String,
        file_size: i32,
    ) -> Result<Option<Arc<DefaultMappedFile>>, RocketMQError> {
        let next_req = match self.admit_request(next_file_path.clone(), file_size) {
            AllocationAdmission::Inserted(request) | AllocationAdmission::Existing(request) => request,
            AllocationAdmission::Rejected => return Ok(None),
        };

        if !next_next_file_path.is_empty() {
            if let AllocationAdmission::Rejected = self.admit_request(next_next_file_path, file_size) {
                warn!("mapped-file blocking preallocation skipped because its Store budget is exhausted");
            }
        }

        if self.has_exception.load(Ordering::Relaxed) {
            warn!("AllocateMappedFileService has exception, so return null");
            return Ok(None);
        }

        if next_req.wait_blocking(WAIT_TIMEOUT) {
            self.remove_request_if_owned(&next_req, false);
            Ok(next_req.mapped_file.read().clone())
        } else {
            warn!("create mmap timeout {} {}", next_req.file_path(), next_req.file_size());
            self.remove_request_if_owned(&next_req, true);
            Ok(None)
        }
    }

    pub fn submit_request_in_background(&self, file_path: String, file_size: u64) {
        let can_submit_request = self.allocation_capacity(1) > 0;

        if !can_submit_request {
            warn!(
                "[NOTIFYME]TransientStorePool is not enough, so skip background preallocate mapped file, \
                 RequestQueueSize: {}, StorePoolSize: {}",
                self.request_queue.read().len(),
                self.transient_store_pool
                    .as_ref()
                    .map_or(0, |pool| pool.available_buffer_nums())
            );
            return;
        }

        if let AllocationAdmission::Rejected = self.admit_request(file_path, file_size as i32) {
            warn!("background mapped-file allocation rejected by Store budget");
        }
    }

    fn allocation_capacity(&self, default_capacity: usize) -> usize {
        let pool_snapshot = if self.transient_store_pool_enable && self.fast_fail_if_no_buffer {
            self.transient_store_pool.as_ref().map(|pool| {
                let queued_requests = self.request_queue.read().len();
                let available_buffers = pool.available_buffer_nums();
                MappedFileAllocationPoolSnapshot::new(available_buffers, queued_requests)
            })
        } else {
            None
        };

        mapped_file_allocation_capacity(
            default_capacity,
            self.transient_store_pool_enable,
            self.fast_fail_if_no_buffer,
            pool_snapshot,
        )
    }

    /// Shutdown the service - corresponds to Java's shutdown()
    pub async fn shutdown(&self) {
        info!("AllocateMappedFileService: shutting down");

        self.stopped.store(true, Ordering::Relaxed);
        self.notify_worker();
        let (_, condvar) = &*self.worker_wakeup;
        condvar.notify_all();

        // Wait for worker to complete
        let handle = self.worker_handle.lock().take();
        if let Some(handle) = handle {
            let completion = self.worker_completion.notified();
            if !self.worker_completed.load(Ordering::Acquire) {
                completion.await;
            }
            while !handle.is_finished() {
                tokio::task::yield_now().await;
            }
            if handle.join().is_err() {
                error!("AllocateMappedFileService worker panicked during shutdown");
            }
        }

        // Clean up pre-allocated files and release every request permit.
        let requests = {
            let mut table = self.request_table.write();
            std::mem::take(&mut *table)
        };
        self.request_queue.write().clear();
        self.abandoned_count.fetch_add(requests.len() as u64, Ordering::Relaxed);
        for req in requests.values() {
            if let Some(ref mapped_file) = *req.mapped_file.read() {
                info!("delete pre allocated mapped file, {}", req.file_path());
                mapped_file.destroy(1000);
            }
        }

        info!("AllocateMappedFileService: shutdown complete");
    }

    /// Get service name
    pub fn get_service_name(&self) -> &'static str {
        "AllocateMappedFileService"
    }

    /// Check if service has exception
    pub fn has_exception(&self) -> bool {
        self.has_exception.load(Ordering::Relaxed)
    }
}

enum AllocationAdmission {
    Inserted(Arc<AllocateRequest>),
    Existing(Arc<AllocateRequest>),
    Rejected,
}

struct AllocationWaitGuard {
    service: AllocateMappedFileService,
    request: Arc<AllocateRequest>,
    disarmed: bool,
}

impl AllocationWaitGuard {
    fn new(service: AllocateMappedFileService, request: Arc<AllocateRequest>) -> Self {
        Self {
            service,
            request,
            disarmed: false,
        }
    }

    fn finish(&mut self, abandoned: bool) {
        self.service.remove_request_if_owned(&self.request, abandoned);
        self.disarmed = true;
    }
}

impl Drop for AllocationWaitGuard {
    fn drop(&mut self) {
        if !self.disarmed {
            self.service.remove_request_if_owned(&self.request, true);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AllocationQueueEntry {
    key: MappedFileAllocationRequestKey,
}

impl AllocationQueueEntry {
    fn from_request(request: &AllocateRequest) -> Self {
        Self {
            key: request.key.clone(),
        }
    }

    fn file_path(&self) -> &str {
        self.key.file_path()
    }

    fn file_size(&self) -> i32 {
        self.key.file_size()
    }
}

impl PartialOrd for AllocationQueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AllocationQueueEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

/// Request to allocate a new MappedFile
///
/// Corresponds to Java's AllocateRequest inner class:
/// - Uses Notify + AtomicBool instead of CountDownLatch for async support
/// - Delegates request identity and priority ordering to the Local boundary
struct AllocateRequest {
    /// Runtime-neutral path, size, and ordering identity.
    key: MappedFileAllocationRequestKey,

    /// Count and charged-file-byte ownership for this canonical request.
    _permit: ResourcePermit,

    /// Monotonic enqueue time used by queue saturation diagnostics.
    enqueued_at: Instant,

    /// Completion notification (equivalent to Java's CountDownLatch)
    completion: Arc<Notify>,

    /// Blocking completion notification for synchronous callers.
    blocking_completion: Arc<(StdMutex<()>, Condvar)>,

    /// Completion flag
    completed: Arc<AtomicBool>,

    /// The allocated MappedFile (set when complete)
    mapped_file: Arc<RwLock<Option<Arc<DefaultMappedFile>>>>,
}

impl AllocateRequest {
    fn new(file_path: String, file_size: i32, permit: ResourcePermit) -> Self {
        Self {
            key: MappedFileAllocationRequestKey::new(file_path, file_size),
            _permit: permit,
            enqueued_at: Instant::now(),
            completion: Arc::new(Notify::new()),
            blocking_completion: Arc::new((StdMutex::new(()), Condvar::new())),
            completed: Arc::new(AtomicBool::new(false)),
            mapped_file: Arc::new(RwLock::new(None)),
        }
    }

    fn file_path(&self) -> &str {
        self.key.file_path()
    }

    fn file_size(&self) -> i32 {
        self.key.file_size()
    }

    /// Wait for allocation to complete (like CountDownLatch.await())
    async fn wait(&self) {
        if !self.completed.load(Ordering::Acquire) {
            self.completion.notified().await;
        }
    }

    fn wait_blocking(&self, timeout: Duration) -> bool {
        if self.completed.load(Ordering::Acquire) {
            return true;
        }

        let (lock, condvar) = &*self.blocking_completion;
        let guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        match condvar.wait_timeout_while(guard, timeout, |_| !self.completed.load(Ordering::Acquire)) {
            Ok((_guard, _timeout)) => {}
            Err(poisoned) => {
                let (_guard, _timeout) = poisoned.into_inner();
            }
        }
        self.completed.load(Ordering::Acquire)
    }

    /// Signal completion (like CountDownLatch.countDown())
    fn complete(&self) {
        self.completed.store(true, Ordering::Release);
        self.completion.notify_waiters();
        let (_, condvar) = &*self.blocking_completion;
        condvar.notify_all();
    }
}

impl Display for AllocateRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.key.fmt(f)
    }
}

// Implement Ord for priority queue (lower offsets have higher priority)
impl PartialEq for AllocateRequest {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for AllocateRequest {}

impl PartialOrd for AllocateRequest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AllocateRequest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_runtime::BudgetLimit;
    use rocketmq_runtime::FullPolicy;
    use rocketmq_runtime::ResourceBudget;
    use rocketmq_runtime::ResourceBudgetTree;
    use tempfile::tempdir;

    use super::*;

    fn test_allocation_budget() -> ResourceBudget {
        ResourceBudgetTree::new(
            "allocation-service-test",
            BudgetLimit::new(16, 64 * 1024, FullPolicy::Reject),
        )
        .expect("test allocation budget")
        .root()
    }

    fn test_request(file_path: String, file_size: i32) -> Arc<AllocateRequest> {
        let budget = ResourceBudgetTree::new(
            "allocation-request-test",
            BudgetLimit::new(4, 8_192, FullPolicy::Reject),
        )
        .expect("test budget")
        .root();
        let permit = budget
            .try_acquire_data(usize::try_from(file_size).expect("positive file size"))
            .expect("request budget");
        Arc::new(AllocateRequest::new(file_path, file_size, permit))
    }

    struct TestAllocationServiceConfig;

    impl AllocateMappedFileServiceConfig for TestAllocationServiceConfig {
        fn mapped_file_warmup_config(&self) -> MappedFileWarmupConfig {
            MappedFileWarmupConfig::new(true, crate::config::FlushDiskType::SyncFlush, 1024, 1)
        }
    }

    #[test]
    fn allocate_request_delegates_identity_display_and_priority_to_local_key() {
        let lower_path = std::path::Path::new("root").join("00000000000000000100");
        let higher_path = std::path::Path::new("root").join("00000000000000000200");
        let lower = test_request(lower_path.to_string_lossy().into_owned(), 1024);
        let higher = test_request(higher_path.to_string_lossy().into_owned(), 2048);
        let mut requests = BinaryHeap::from([higher, lower.clone()]);

        let first = requests.pop().expect("lower offset request");
        assert!(Arc::ptr_eq(&first, &lower));
        assert_eq!(lower.file_path(), lower_path.to_string_lossy().as_ref());
        assert_eq!(lower.file_size(), 1024);
        assert_eq!(
            lower.to_string(),
            format!(
                "AllocateRequest[file_path={},file_size=1024]",
                lower_path.to_string_lossy()
            )
        );
    }

    #[tokio::test]
    async fn allocate_mapped_file_blocking_works_inside_runtime() {
        let temp_dir = tempdir().expect("temp dir");
        let file_path = temp_dir.path().join("00000000000000000000");
        let service = AllocateMappedFileService::new_with_config(None, false, false, test_allocation_budget());
        assert!(!service.is_started());
        service.start();
        assert!(service.is_started());

        let mapped_file = service
            .allocate_mapped_file_blocking(file_path.to_string_lossy().to_string(), 1024)
            .expect("allocate mapped file");

        assert_eq!(mapped_file.get_file_size(), 1024);
        assert!(file_path.exists(), "mapped file should be created on disk");

        service.shutdown().await;
        assert!(!service.is_started());
        service.shutdown().await;
    }

    #[tokio::test]
    async fn worker_completion_guard_records_exit_and_notifies_waiter() {
        let completed = Arc::new(AtomicBool::new(false));
        let notification = Arc::new(Notify::new());
        let waiter = notification.notified();

        drop(WorkerCompletion {
            completed: completed.clone(),
            notification: notification.clone(),
        });

        waiter.await;
        assert!(completed.load(Ordering::Acquire));
    }

    #[test]
    fn warm_mapped_file_config_follows_commitlog_file_size_threshold() {
        let config = TestAllocationServiceConfig;
        let service = AllocateMappedFileService::new_with_message_store_config(
            None,
            false,
            false,
            &config,
            test_allocation_budget(),
        );

        assert!(!service.should_warm_mapped_file(1023));
        assert!(service.should_warm_mapped_file(1024));
    }

    #[test]
    fn allocation_capacity_delegates_runtime_snapshot_to_local_policy() {
        let pool = Arc::new(TransientStorePool::new(2, 16));
        pool.return_buffer(vec![0; 16]);
        pool.return_buffer(vec![0; 16]);
        let service = AllocateMappedFileService::new_with_config(Some(pool), true, true, test_allocation_budget());

        assert_eq!(service.allocation_capacity(2), 2);
        let request = test_request(
            std::path::Path::new("root")
                .join("00000000000000000100")
                .to_string_lossy()
                .into_owned(),
            16,
        );
        service
            .request_queue
            .write()
            .push(AllocationQueueEntry::from_request(&request));
        assert_eq!(service.allocation_capacity(2), 1);
    }
}
