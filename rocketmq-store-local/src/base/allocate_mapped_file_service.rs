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
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Display;
use std::fmt::Formatter;
use std::path::Path;
use std::path::PathBuf;
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
use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_error::RocketMQError;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::BudgetSnapshot;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourcePermit;
use rocketmq_runtime::ShutdownDeadline;
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

mod managed;

use managed::ManagedAllocationContext;
use managed::ManagedAllocationRequest;
pub(crate) use managed::ManagedMappedFileAllocationFailure;

/// Timeout for waiting on file allocation (matches Java: 5 seconds)
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_CLEANUP_TIMEOUT: Duration = Duration::from_millis(1_100);
const SHUTDOWN_CLEANUP_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const CLEANUP_RETRY_BATCH_MAX: usize = 16;

/// Bounded mapped-file allocation queue diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MappedFileAllocationQueueSnapshot {
    /// Requests and retained physical cleanup owners holding mapped-file allocation budget.
    pub current_count: usize,
    /// File bytes charged to accepted requests or retained physical cleanup owners.
    pub charged_bytes: usize,
    /// Requests waiting in the priority heap, including harmless stale keys.
    pub queued_count: usize,
    /// Age of the oldest request still owned by the table.
    pub oldest_age: Option<Duration>,
    /// Requests rejected by count or byte admission.
    pub rejected_count: u64,
    /// Accepted requests explicitly abandoned by timeout or shutdown.
    pub abandoned_count: u64,
    /// Abandoned mapped files whose namespace cleanup is still pending.
    pub pending_cleanup_count: usize,
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

type PendingCleanupKey = (String, u64);
type PendingCleanupRegistry = HashMap<PendingCleanupKey, PendingAllocationCleanup>;

enum PendingAllocationCleanup {
    AwaitingWorker(Option<ResourcePermit>),
    Retained(Arc<PendingMappedFileCleanup>),
    Cleaning(Arc<PendingMappedFileCleanup>),
}

struct PendingMappedFileCleanup {
    mapped_file: Arc<DefaultMappedFile>,
    _permit: Option<ResourcePermit>,
    attempts: AtomicU64,
}

struct PendingCleanupAttempt {
    registry: Arc<Mutex<PendingCleanupRegistry>>,
    key: PendingCleanupKey,
    owner: Arc<PendingMappedFileCleanup>,
    completed: bool,
}

impl PendingCleanupAttempt {
    fn finish(mut self, namespace_removed: bool) {
        let mut pending = self.registry.lock();
        let same_attempt = matches!(
            pending.get(&self.key),
            Some(PendingAllocationCleanup::Cleaning(current)) if Arc::ptr_eq(current, &self.owner)
        );
        if same_attempt {
            if namespace_removed {
                pending.remove(&self.key);
            } else {
                pending.insert(self.key.clone(), PendingAllocationCleanup::Retained(self.owner.clone()));
            }
        }
        self.completed = true;
    }
}

impl Drop for PendingCleanupAttempt {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut pending = self.registry.lock();
        let same_attempt = matches!(
            pending.get(&self.key),
            Some(PendingAllocationCleanup::Cleaning(current)) if Arc::ptr_eq(current, &self.owner)
        );
        if same_attempt {
            pending.insert(self.key.clone(), PendingAllocationCleanup::Retained(self.owner.clone()));
        }
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

    /// Process-local identity assigned to each admitted request.
    next_request_id: Arc<AtomicU64>,

    /// Path fences and retained owners for abandoned allocation requests.
    pending_cleanup: Arc<Mutex<PendingCleanupRegistry>>,

    /// Queue directories closed to new preallocation requests until retirement completes.
    retired_directories: Arc<RwLock<HashSet<PathBuf>>>,

    /// Runtime-owner-derived executor for bounded shutdown cleanup I/O.
    cleanup_executor: Option<BlockingExecutor>,

    /// Wave-B requests executed by the same Store-owned allocation worker.
    managed_requests: Arc<Mutex<VecDeque<Arc<ManagedAllocationRequest>>>>,

    /// Reconciled lifecycle authority installed before the allocation worker starts.
    managed_context: Arc<RwLock<Option<ManagedAllocationContext>>>,

    /// Exception flag (set when allocation fails)
    has_exception: Arc<AtomicBool>,

    /// Shutdown flag
    stopped: Arc<AtomicBool>,

    /// Records whether this service ever crossed into managed worker mode.
    ever_started: Arc<AtomicBool>,

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
            next_request_id: self.next_request_id.clone(),
            pending_cleanup: self.pending_cleanup.clone(),
            retired_directories: self.retired_directories.clone(),
            cleanup_executor: self.cleanup_executor.clone(),
            managed_requests: self.managed_requests.clone(),
            managed_context: self.managed_context.clone(),
            has_exception: self.has_exception.clone(),
            stopped: self.stopped.clone(),
            ever_started: self.ever_started.clone(),
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
        Self::new_with_config_inner(
            transient_store_pool,
            transient_store_pool_enable,
            fast_fail_if_no_buffer,
            allocation_budget,
            None,
        )
    }

    /// Creates a service whose shutdown cleanup is owned by the Store runtime.
    #[doc(hidden)]
    pub fn new_with_config_and_storage_io(
        transient_store_pool: Option<Arc<TransientStorePool>>,
        transient_store_pool_enable: bool,
        fast_fail_if_no_buffer: bool,
        allocation_budget: ResourceBudget,
        storage_io: BlockingExecutor,
    ) -> Self {
        Self::new_with_config_inner(
            transient_store_pool,
            transient_store_pool_enable,
            fast_fail_if_no_buffer,
            allocation_budget,
            Some(storage_io),
        )
    }

    fn new_with_config_inner(
        transient_store_pool: Option<Arc<TransientStorePool>>,
        transient_store_pool_enable: bool,
        fast_fail_if_no_buffer: bool,
        allocation_budget: ResourceBudget,
        cleanup_executor: Option<BlockingExecutor>,
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
            next_request_id: Arc::new(AtomicU64::new(1)),
            pending_cleanup: Arc::new(Mutex::new(HashMap::new())),
            retired_directories: Arc::new(RwLock::new(HashSet::new())),
            cleanup_executor,
            managed_requests: Arc::new(Mutex::new(VecDeque::new())),
            managed_context: Arc::new(RwLock::new(None)),
            has_exception,
            stopped,
            ever_started: Arc::new(AtomicBool::new(false)),
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

    #[doc(hidden)]
    pub fn new_with_message_store_config_and_storage_io<C>(
        transient_store_pool: Option<Arc<TransientStorePool>>,
        transient_store_pool_enable: bool,
        fast_fail_if_no_buffer: bool,
        message_store_config: &C,
        allocation_budget: ResourceBudget,
        storage_io: BlockingExecutor,
    ) -> Self
    where
        C: AllocateMappedFileServiceConfig,
    {
        let mut service = Self::new_with_config_and_storage_io(
            transient_store_pool,
            transient_store_pool_enable,
            fast_fail_if_no_buffer,
            allocation_budget,
            storage_io,
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

        self.ever_started.store(true, Ordering::Release);
        self.stopped.store(false, Ordering::Relaxed);
        self.worker_completed.store(false, Ordering::Release);

        let request_table = self.request_table.clone();
        let request_queue = self.request_queue.clone();
        let pending_cleanup = self.pending_cleanup.clone();
        let managed_requests = self.managed_requests.clone();
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
                    pending_cleanup,
                    managed_requests,
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
        pending_cleanup: Arc<Mutex<PendingCleanupRegistry>>,
        managed_requests: Arc<Mutex<VecDeque<Arc<ManagedAllocationRequest>>>>,
        has_exception: Arc<AtomicBool>,
        stopped: Arc<AtomicBool>,
        transient_store_pool: Option<Arc<TransientStorePool>>,
        worker_wakeup: Arc<(StdMutex<()>, Condvar)>,
        warm_mapped_file_config: MappedFileWarmupConfig,
        #[cfg(feature = "observability")] store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder,
    ) {
        info!("AllocateMappedFileService: service started");

        while !stopped.load(Ordering::Relaxed) {
            while !stopped.load(Ordering::Relaxed) {
                if let Some(request) = managed_requests.lock().pop_front() {
                    request.execute();
                    continue;
                }
                if !Self::mmap_operation(
                    &request_table,
                    &request_queue,
                    &pending_cleanup,
                    &has_exception,
                    &transient_store_pool,
                    warm_mapped_file_config,
                    #[cfg(feature = "observability")]
                    &store_metrics,
                ) {
                    break;
                }
            }

            if stopped.load(Ordering::Relaxed) {
                break;
            }

            if request_queue.read().is_empty() && managed_requests.lock().is_empty() {
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
        pending_cleanup: &Arc<Mutex<PendingCleanupRegistry>>,
        has_exception: &Arc<AtomicBool>,
        transient_store_pool: &Option<Arc<TransientStorePool>>,
        warm_mapped_file_config: MappedFileWarmupConfig,
        #[cfg(feature = "observability")] store_metrics: &rocketmq_observability::metrics::store::StoreMetricsRecorder,
    ) -> bool {
        Self::retry_pending_cleanup_list(pending_cleanup);
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
            table
                .get(entry.file_path())
                .filter(|request| request.id() == entry.request_id())
                .cloned()
        };

        let req = match expected_request {
            Some(request) if request.file_size() == entry.file_size() => request,
            None => {
                Self::clear_waiting_cleanup_fence(pending_cleanup, entry.file_path(), entry.request_id());
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
                    .is_some_and(|request| request.id() == req.id() && Arc::ptr_eq(request, &req));
                if !request_is_owned {
                    Self::retain_for_cleanup(
                        pending_cleanup,
                        req.file_path(),
                        req.id(),
                        mapped_file,
                        req.take_permit(),
                    );
                    req.complete();
                    return true;
                }
                *req.mapped_file.write() = Some(mapped_file);
                let request_is_still_owned = request_table
                    .read()
                    .get(req.file_path())
                    .is_some_and(|request| request.id() == req.id() && Arc::ptr_eq(request, &req));
                if !request_is_still_owned {
                    if let Some(mapped_file) = req.mapped_file.write().take() {
                        Self::retain_for_cleanup(
                            pending_cleanup,
                            req.file_path(),
                            req.id(),
                            mapped_file,
                            req.take_permit(),
                        );
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

                if !Self::requeue_failed_request_if_owned(request_table, request_queue, &req, entry) {
                    Self::clear_waiting_cleanup_fence(pending_cleanup, req.file_path(), req.id());
                    req.complete();
                    return true;
                }

                // Small delay before retry
                thread::sleep(Duration::from_millis(1));

                false
            }
        }
    }

    fn requeue_failed_request_if_owned(
        request_table: &Arc<RwLock<HashMap<String, Arc<AllocateRequest>>>>,
        request_queue: &Arc<RwLock<BinaryHeap<AllocationQueueEntry>>>,
        request: &Arc<AllocateRequest>,
        entry: AllocationQueueEntry,
    ) -> bool {
        Self::requeue_failed_request_if_owned_with_hook(request_table, request_queue, request, entry, || {})
    }

    fn requeue_failed_request_if_owned_with_hook<F>(
        request_table: &Arc<RwLock<HashMap<String, Arc<AllocateRequest>>>>,
        request_queue: &Arc<RwLock<BinaryHeap<AllocationQueueEntry>>>,
        request: &Arc<AllocateRequest>,
        entry: AllocationQueueEntry,
        before_heap_publication: F,
    ) -> bool
    where
        F: FnOnce(),
    {
        let table = request_table.read();
        let request_is_owned = table
            .get(request.file_path())
            .is_some_and(|current| current.id() == request.id() && Arc::ptr_eq(current, request));
        if !request_is_owned {
            return false;
        }

        // Publish the retry while the table identity remains read-locked. Shutdown and directory
        // retirement use the inverse side of the same table -> heap order, so they either drain
        // this entry or make the ownership check fail.
        before_heap_publication();
        request_queue.write().push(entry);
        true
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

    fn cleanup_key(file_path: &str, request_id: u64) -> PendingCleanupKey {
        (file_path.to_owned(), request_id)
    }

    fn has_pending_cleanup_for_path(pending: &PendingCleanupRegistry, file_path: &str) -> bool {
        pending.keys().any(|(candidate, _)| candidate == file_path)
    }

    fn path_belongs_to_directory(file_path: &str, directory: &Path) -> bool {
        Path::new(file_path).parent().is_some_and(|parent| parent == directory)
    }

    fn path_is_retired(&self, file_path: &str) -> bool {
        let retired = self.retired_directories.read();
        retired
            .iter()
            .any(|directory| Self::path_belongs_to_directory(file_path, directory))
    }

    /// Returns whether legacy synchronous creation is safe for a service that was never started.
    #[doc(hidden)]
    pub fn allows_synchronous_fallback(&self, file_path: &Path) -> bool {
        if self.ever_started.load(Ordering::Acquire)
            || self.stopped.load(Ordering::Acquire)
            || self.worker_handle.lock().is_some()
        {
            return false;
        }
        let file_path = file_path.to_string_lossy();
        !self.path_is_retired(file_path.as_ref())
            && !Self::has_pending_cleanup_for_path(&self.pending_cleanup.lock(), file_path.as_ref())
    }

    /// Closes one mapped-file queue directory to allocator admission and transfers every
    /// matching request to cleanup ownership.
    #[doc(hidden)]
    pub fn retire_directory(&self, directory: &Path) -> bool {
        let directory = directory.to_path_buf();
        let (requests, removed_queue_entries) = {
            let mut table = self.request_table.write();
            self.retired_directories.write().insert(directory.clone());
            let paths = table
                .iter()
                .filter(|(path, _)| Self::path_belongs_to_directory(path, &directory))
                .map(|(path, _)| path.clone())
                .collect::<Vec<_>>();
            for path in &paths {
                if let Some(request) = table.get(path) {
                    Self::register_waiting_cleanup_fence(
                        &self.pending_cleanup,
                        request.file_path(),
                        request.id(),
                        request.take_permit(),
                    );
                }
            }
            let requests = paths
                .into_iter()
                .filter_map(|path| table.remove(&path).map(|request| (path, request)))
                .collect::<Vec<_>>();
            let mut removed_queue_entries = Vec::new();
            self.request_queue.write().retain(|entry| {
                let retain = !Self::path_belongs_to_directory(entry.file_path(), &directory);
                if !retain {
                    removed_queue_entries.push((entry.file_path().to_owned(), entry.request_id()));
                }
                retain
            });
            (requests, removed_queue_entries)
        };

        self.abandoned_count.fetch_add(requests.len() as u64, Ordering::Relaxed);
        for (_, request) in requests {
            if let Some(mapped_file) = request.mapped_file.write().take() {
                Self::retain_for_cleanup(
                    &self.pending_cleanup,
                    request.file_path(),
                    request.id(),
                    mapped_file,
                    request.take_permit(),
                );
            }
            request.complete();
        }
        for (file_path, request_id) in removed_queue_entries {
            Self::clear_waiting_cleanup_fence(&self.pending_cleanup, &file_path, request_id);
        }
        self.notify_worker();
        if self.stopped.load(Ordering::Acquire) {
            self.retry_retired_directory_cleanup_once(&directory)
        } else {
            self.is_directory_retirement_complete(&directory)
        }
    }

    /// Retries a bounded batch of retained cleanup owners for one retired directory.
    ///
    /// This foreground path keeps queue destruction retryable after the allocation worker has
    /// stopped. It never scans or deletes owners outside `directory` and refuses directories that
    /// have not first crossed the retirement fence.
    #[doc(hidden)]
    pub fn retry_retired_directory_cleanup_once(&self, directory: &Path) -> bool {
        if !self.stopped.load(Ordering::Acquire) || !self.retired_directories.read().contains(directory) {
            return false;
        }
        let _ = Self::retry_pending_cleanup_list_until(
            &self.pending_cleanup,
            None,
            CLEANUP_RETRY_BATCH_MAX,
            Some(directory),
        );
        self.is_directory_retirement_complete(directory)
    }

    /// Returns whether a retired directory has no allocator request or cleanup owner remaining.
    #[doc(hidden)]
    pub fn is_directory_retirement_complete(&self, directory: &Path) -> bool {
        let has_request = self
            .request_table
            .read()
            .keys()
            .any(|path| Self::path_belongs_to_directory(path, directory));
        let has_cleanup = self
            .pending_cleanup
            .lock()
            .keys()
            .any(|(path, _)| Self::path_belongs_to_directory(path, directory));
        !has_request && !has_cleanup
    }

    /// Reopens allocator admission after the caller has proved the retired namespace absent.
    #[doc(hidden)]
    pub fn complete_directory_retirement(&self, directory: &Path) -> bool {
        let table = self.request_table.read();
        let mut retired = self.retired_directories.write();
        if !retired.contains(directory)
            || table
                .keys()
                .any(|path| Self::path_belongs_to_directory(path, directory))
            || self
                .pending_cleanup
                .lock()
                .keys()
                .any(|(path, _)| Self::path_belongs_to_directory(path, directory))
        {
            return false;
        }
        retired.remove(directory)
    }

    fn register_waiting_cleanup_fence(
        pending_cleanup: &Arc<Mutex<PendingCleanupRegistry>>,
        file_path: &str,
        request_id: u64,
        permit: Option<ResourcePermit>,
    ) {
        let mut permit = permit;
        let mut pending = pending_cleanup.lock();
        match pending.entry(Self::cleanup_key(file_path, request_id)) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(PendingAllocationCleanup::AwaitingWorker(permit.take()));
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if let PendingAllocationCleanup::AwaitingWorker(current) = entry.get_mut() {
                    if current.is_none() {
                        *current = permit.take();
                    }
                }
            }
        }
    }

    fn clear_waiting_cleanup_fence(
        pending_cleanup: &Arc<Mutex<PendingCleanupRegistry>>,
        file_path: &str,
        request_id: u64,
    ) {
        let key = Self::cleanup_key(file_path, request_id);
        let mut pending = pending_cleanup.lock();
        if matches!(pending.get(&key), Some(PendingAllocationCleanup::AwaitingWorker(_))) {
            pending.remove(&key);
        }
    }

    fn retain_for_cleanup(
        pending_cleanup: &Arc<Mutex<PendingCleanupRegistry>>,
        file_path: &str,
        request_id: u64,
        mapped_file: Arc<DefaultMappedFile>,
        permit: Option<ResourcePermit>,
    ) {
        let key = Self::cleanup_key(file_path, request_id);
        let mut pending = pending_cleanup.lock();
        let permit = match pending.remove(&key) {
            Some(PendingAllocationCleanup::AwaitingWorker(existing)) => existing.or(permit),
            Some(existing @ (PendingAllocationCleanup::Retained(_) | PendingAllocationCleanup::Cleaning(_))) => {
                pending.insert(key, existing);
                return;
            }
            None => permit,
        };
        pending.insert(
            key,
            PendingAllocationCleanup::Retained(Arc::new(PendingMappedFileCleanup {
                mapped_file,
                _permit: permit,
                attempts: AtomicU64::new(0),
            })),
        );
    }

    fn retry_pending_cleanup_list(pending_cleanup: &Arc<Mutex<PendingCleanupRegistry>>) {
        let _ = Self::retry_pending_cleanup_list_until(pending_cleanup, None, CLEANUP_RETRY_BATCH_MAX, None);
    }

    fn retry_pending_cleanup_list_until(
        pending_cleanup: &Arc<Mutex<PendingCleanupRegistry>>,
        deadline: Option<ShutdownDeadline>,
        max_attempts: usize,
        directory: Option<&Path>,
    ) -> usize {
        let attempts = {
            let mut pending = pending_cleanup.lock();
            let mut candidates = pending
                .iter()
                .filter_map(|(key, cleanup)| match cleanup {
                    PendingAllocationCleanup::Retained(owner)
                        if directory.is_none_or(|directory| Self::path_belongs_to_directory(&key.0, directory)) =>
                    {
                        Some((key.clone(), owner.clone()))
                    }
                    PendingAllocationCleanup::AwaitingWorker(_) | PendingAllocationCleanup::Cleaning(_) => None,
                    PendingAllocationCleanup::Retained(_) => None,
                })
                .collect::<Vec<_>>();
            candidates.sort_by_key(|(key, owner)| (owner.attempts.load(Ordering::Acquire), key.1));
            candidates.truncate(max_attempts);
            for (key, owner) in &candidates {
                pending.insert(key.clone(), PendingAllocationCleanup::Cleaning(owner.clone()));
            }
            candidates
                .into_iter()
                .map(|(key, owner)| PendingCleanupAttempt {
                    registry: pending_cleanup.clone(),
                    key,
                    owner,
                    completed: false,
                })
                .collect::<Vec<_>>()
        };

        let mut completed = 0;
        for attempt in attempts {
            if deadline.is_some_and(ShutdownDeadline::is_expired) {
                break;
            }
            attempt.owner.attempts.fetch_add(1, Ordering::AcqRel);
            let namespace_removed = attempt.owner.mapped_file.try_destroy(1000).is_namespace_removed();
            attempt.finish(namespace_removed);
            completed += 1;
        }
        completed
    }

    #[cfg(test)]
    fn retry_pending_cleanup(&self) {
        Self::retry_pending_cleanup_list(&self.pending_cleanup);
    }

    async fn drain_pending_cleanup_for_shutdown(&self, deadline: ShutdownDeadline) {
        let Some(executor) = self.cleanup_executor.clone() else {
            warn!(
                pending_cleanup_count = self.pending_cleanup.lock().len(),
                "mapped-file shutdown cleanup retained because no Store blocking executor was supplied"
            );
            return;
        };
        let pending_cleanup = self.pending_cleanup.clone();
        let result = executor
            .spawn_io_until("store.allocate-mapped-file-shutdown-cleanup", deadline, move || loop {
                if deadline.is_expired() || pending_cleanup.lock().is_empty() {
                    return pending_cleanup.lock().len();
                }
                let _ = Self::retry_pending_cleanup_list_until(
                    &pending_cleanup,
                    Some(deadline),
                    CLEANUP_RETRY_BATCH_MAX,
                    None,
                );
                if pending_cleanup.lock().is_empty() {
                    return 0;
                }
                let remaining = deadline.remaining();
                if remaining.is_zero() {
                    return pending_cleanup.lock().len();
                }
                thread::sleep(SHUTDOWN_CLEANUP_RETRY_INTERVAL.min(remaining));
            })
            .await;
        if let Err(error) = result {
            warn!(%error, "bounded mapped-file shutdown cleanup did not finish before its runtime deadline");
        }
    }

    fn notify_worker(&self) {
        self.notify.notify_one();
        let (_, condvar) = &*self.worker_wakeup;
        condvar.notify_one();
    }

    fn admit_request(&self, file_path: String, file_size: i32) -> AllocationAdmission {
        if file_size <= 0 {
            warn!(
                file_path,
                file_size, "mapped-file allocation rejected because its size is not positive"
            );
            return AllocationAdmission::Rejected;
        }
        if self.stopped.load(Ordering::Acquire) || self.path_is_retired(&file_path) {
            return AllocationAdmission::Rejected;
        }
        {
            let table = self.request_table.read();
            if let Some(existing) = table.get(&file_path) {
                return AllocationAdmission::Existing(existing.clone());
            }
            if Self::has_pending_cleanup_for_path(&self.pending_cleanup.lock(), &file_path) {
                warn!(
                    file_path,
                    "mapped-file allocation rejected while prior cleanup remains pending"
                );
                return AllocationAdmission::Rejected;
            }
        }
        let charged_bytes = usize::try_from(file_size).unwrap_or_default().max(1);
        let permit = match self.allocation_budget.try_acquire(charged_bytes, BudgetClass::Data) {
            Ok(permit) => permit,
            Err(error) => {
                warn!(
                    queue = "mapped-file-allocation",
                    ?error,
                    charged_bytes,
                    "mapped-file allocation request rejected by Store budget"
                );
                return AllocationAdmission::Rejected;
            }
        };
        let mut table = self.request_table.write();
        if self.stopped.load(Ordering::Acquire) || self.path_is_retired(&file_path) {
            warn!(
                file_path,
                "mapped-file allocation rejected because its queue directory is retired"
            );
            return AllocationAdmission::Rejected;
        }
        if let Some(existing) = table.get(&file_path) {
            return AllocationAdmission::Existing(existing.clone());
        }
        if Self::has_pending_cleanup_for_path(&self.pending_cleanup.lock(), &file_path) {
            warn!(
                file_path,
                "mapped-file allocation rejected while prior cleanup remains pending"
            );
            return AllocationAdmission::Rejected;
        }
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let request = Arc::new(AllocateRequest::new(request_id, file_path.clone(), file_size, permit));
        table.insert(file_path, request.clone());
        self.request_queue
            .write()
            .push(AllocationQueueEntry::from_request(&request));
        drop(table);
        self.notify_worker();
        AllocationAdmission::Inserted(request)
    }

    fn remove_request_if_owned(&self, request: &Arc<AllocateRequest>, abandoned: bool) {
        let mut table = self.request_table.write();
        let removed = if table
            .get(request.file_path())
            .is_some_and(|current| current.id() == request.id() && Arc::ptr_eq(current, request))
        {
            if abandoned {
                Self::register_waiting_cleanup_fence(
                    &self.pending_cleanup,
                    request.file_path(),
                    request.id(),
                    request.take_permit(),
                );
            }
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
                Self::retain_for_cleanup(
                    &self.pending_cleanup,
                    request.file_path(),
                    request.id(),
                    mapped_file,
                    request.take_permit(),
                );
                self.notify_worker();
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
            pending_cleanup_count: self.pending_cleanup.lock().len(),
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
        if file_size <= 0 {
            warn!(
                file_path = next_file_path,
                file_size, "mapped-file allocation rejected because its size is not positive"
            );
            return Ok(None);
        }
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
            self.remove_request_if_owned(&next_req, true);
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
        let file_size = Self::checked_public_file_size(&file_path, file_size)?;
        // Use empty string for next-next file (won't be allocated)
        let result = self
            .put_request_and_return_mapped_file(
                file_path.clone(),
                String::new(), // No pre-allocation
                file_size,
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
        let file_size = Self::checked_public_file_size(&file_path, file_size)?;
        let result = self.put_request_and_return_mapped_file_blocking(file_path.clone(), String::new(), file_size)?;

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
            self.remove_request_if_owned(&next_req, true);
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
        let file_size = match Self::checked_public_file_size(&file_path, file_size) {
            Ok(file_size) => file_size,
            Err(error) => {
                warn!(%error, "background mapped-file allocation rejected before admission");
                return;
            }
        };
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

        if let AllocationAdmission::Rejected = self.admit_request(file_path, file_size) {
            warn!("background mapped-file allocation rejected by Store budget");
        }
    }

    /// Installs the reconciled Wave-B lifecycle authority before this worker starts.
    ///
    /// This is a local capability handoff, not a cryptographic gate. A second installation or an
    /// installation after worker start is rejected so queues cannot switch lifecycle authorities
    /// while allocation is live.
    #[doc(hidden)]
    pub fn install_managed_lifecycle(
        &self,
        runtime: crate::mapped_file::ManagedLifecycleRuntime,
        store_root: PathBuf,
    ) -> bool {
        if self.ever_started.load(Ordering::Acquire) || self.worker_handle.lock().is_some() {
            return false;
        }
        let mut context = self.managed_context.write();
        if context.is_some() {
            return false;
        }
        *context = Some(ManagedAllocationContext::new(runtime, store_root));
        true
    }

    /// Creates exactly one managed segment on the Store-owned allocation worker.
    ///
    /// Managed requests are never speculative and are never automatically retried. A durable or
    /// namespace failure recovery-fences the lifecycle runtime and is returned to the caller.
    #[doc(hidden)]
    pub fn put_managed_request_and_return_mapped_file_blocking(
        &self,
        queue: crate::mapped_file::ManagedMappedFileQueueGeneration<DefaultMappedFile>,
        queue_path: &Path,
        segment_offset: u64,
        file_size: u64,
    ) -> Result<Option<Arc<DefaultMappedFile>>, rocketmq_store_api::StoreError> {
        match self.put_managed_request_and_return_mapped_file_blocking_checked(
            queue,
            queue_path,
            segment_offset,
            file_size,
        ) {
            Ok(mapped_file) => Ok(Some(mapped_file)),
            Err(
                ManagedMappedFileAllocationFailure::InvalidFileSize(_)
                | ManagedMappedFileAllocationFailure::QueueOutsideStoreRoot
                | ManagedMappedFileAllocationFailure::InvalidQueuePath,
            ) => Ok(None),
            Err(ManagedMappedFileAllocationFailure::Store(source)) => Err(source),
            Err(error) => {
                let descriptor = match &error {
                    ManagedMappedFileAllocationFailure::WorkerUnavailable
                    | ManagedMappedFileAllocationFailure::LifecycleUnavailable => {
                        &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE
                    }
                    ManagedMappedFileAllocationFailure::Budget(_) => &rocketmq_error::STORAGE_CAPACITY_EXHAUSTED,
                    ManagedMappedFileAllocationFailure::InvalidFileSize(_)
                    | ManagedMappedFileAllocationFailure::QueueOutsideStoreRoot
                    | ManagedMappedFileAllocationFailure::InvalidQueuePath
                    | ManagedMappedFileAllocationFailure::Store(_) => unreachable!(),
                };
                Err(
                    rocketmq_store_api::StoreError::new(descriptor, rocketmq_store_api::StoreOperation::Append)
                        .in_component(rocketmq_store_api::StoreComponent::MappedFile)
                        .with_detail("managed mapped-file allocation failed")
                        .with_source(error),
                )
            }
        }
    }

    fn put_managed_request_and_return_mapped_file_blocking_checked(
        &self,
        queue: crate::mapped_file::ManagedMappedFileQueueGeneration<DefaultMappedFile>,
        queue_path: &Path,
        segment_offset: u64,
        file_size: u64,
    ) -> Result<Arc<DefaultMappedFile>, ManagedMappedFileAllocationFailure> {
        if !self.is_started() {
            return Err(ManagedMappedFileAllocationFailure::WorkerUnavailable);
        }
        let charged_bytes = usize::try_from(file_size)
            .ok()
            .filter(|size| *size > 0)
            .ok_or(ManagedMappedFileAllocationFailure::InvalidFileSize(file_size))?;
        let permit = self
            .allocation_budget
            .try_acquire(charged_bytes, BudgetClass::Data)
            .map_err(ManagedMappedFileAllocationFailure::Budget)?;
        let context = self
            .managed_context
            .read()
            .clone()
            .ok_or(ManagedMappedFileAllocationFailure::LifecycleUnavailable)?;
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let request = Arc::new(ManagedAllocationRequest::new(
            context,
            queue,
            queue_path,
            segment_offset,
            file_size,
            request_id,
            self.transient_store_pool
                .as_ref()
                .filter(|_| self.transient_store_pool_enable)
                .map(|pool| (**pool).clone()),
            permit,
        )?);
        self.managed_requests.lock().push_back(Arc::clone(&request));
        self.notify_worker();
        request.wait(&self.worker_completed)
    }

    fn checked_public_file_size(file_path: &str, file_size: u64) -> Result<i32, RocketMQError> {
        i32::try_from(file_size)
            .ok()
            .filter(|file_size| *file_size > 0)
            .ok_or_else(|| RocketMQError::StorageWriteFailed {
                path: file_path.to_owned(),
                reason: format!("mapped-file size must be in 1..={} bytes, got {file_size}", i32::MAX),
            })
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
        let deadline = ShutdownDeadline::after(SHUTDOWN_CLEANUP_TIMEOUT);

        self.stopped.store(true, Ordering::Relaxed);
        for request in self.managed_requests.lock().drain(..) {
            request.cancel();
        }
        self.notify_worker();
        let (_, condvar) = &*self.worker_wakeup;
        condvar.notify_all();

        // Revoke table and heap ownership before waiting for the worker. A request already
        // claimed by the worker keeps its AwaitingWorker fence; a late mapping therefore enters
        // the cleanup registry before the managed join task performs its final cleanup pass.
        let (requests, removed_queue_entries) = {
            let mut table = self.request_table.write();
            for request in table.values() {
                Self::register_waiting_cleanup_fence(
                    &self.pending_cleanup,
                    request.file_path(),
                    request.id(),
                    request.take_permit(),
                );
            }
            let requests = std::mem::take(&mut *table);
            let removed_queue_entries = self
                .request_queue
                .write()
                .drain()
                .map(|entry| (entry.file_path().to_owned(), entry.request_id()))
                .collect::<Vec<_>>();
            (requests, removed_queue_entries)
        };
        self.abandoned_count.fetch_add(requests.len() as u64, Ordering::Relaxed);
        for req in requests.values() {
            if let Some(mapped_file) = req.mapped_file.write().take() {
                info!("delete pre allocated mapped file, {}", req.file_path());
                Self::retain_for_cleanup(
                    &self.pending_cleanup,
                    req.file_path(),
                    req.id(),
                    mapped_file,
                    req.take_permit(),
                );
            }
            req.complete();
        }
        for (file_path, request_id) in removed_queue_entries {
            Self::clear_waiting_cleanup_fence(&self.pending_cleanup, &file_path, request_id);
        }

        // Wait for the worker through the Store-owned blocking boundary. The same absolute
        // deadline covers both join observation and the foreground cleanup drain.
        let handle = self.worker_handle.lock().take();
        if let Some(handle) = handle {
            if let Some(executor) = self.cleanup_executor.clone() {
                let join_owner = Arc::new(Mutex::new(Some(handle)));
                let task_owner = join_owner.clone();
                let pending_cleanup = self.pending_cleanup.clone();
                match executor
                    .spawn_io_until("store.allocate-mapped-file-worker-join", deadline, move || {
                        let handle = task_owner.lock().take();
                        let join_result = handle.map(thread::JoinHandle::join).unwrap_or(Ok(()));
                        if join_result.is_ok() {
                            let _ = Self::retry_pending_cleanup_list_until(
                                &pending_cleanup,
                                Some(deadline),
                                CLEANUP_RETRY_BATCH_MAX,
                                None,
                            );
                        }
                        join_result
                    })
                    .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => error!("AllocateMappedFileService worker panicked during shutdown"),
                    Err(error) => {
                        warn!(%error, "AllocateMappedFileService worker join exceeded the shutdown deadline");
                        if let Some(handle) = join_owner.lock().take() {
                            *self.worker_handle.lock() = Some(handle);
                        }
                    }
                }
            } else {
                let completed = async {
                    loop {
                        let notified = self.worker_completion.notified();
                        tokio::pin!(notified);
                        let _ = notified.as_mut().enable();
                        if self.worker_completed.load(Ordering::Acquire) {
                            return;
                        }
                        notified.await;
                    }
                };
                if tokio::time::timeout(deadline.remaining(), completed).await.is_err() {
                    warn!("AllocateMappedFileService worker did not stop before the shutdown deadline");
                    *self.worker_handle.lock() = Some(handle);
                } else if !handle.is_finished() {
                    warn!("AllocateMappedFileService worker completion was signalled before the thread fully exited");
                    *self.worker_handle.lock() = Some(handle);
                } else if handle.join().is_err() {
                    error!("AllocateMappedFileService worker panicked during shutdown");
                }
            }
        }
        self.drain_pending_cleanup_for_shutdown(deadline).await;

        let pending_cleanup_count = self.pending_cleanup.lock().len();
        if pending_cleanup_count == 0 {
            info!("AllocateMappedFileService: shutdown complete");
        } else {
            warn!(
                pending_cleanup_count,
                "AllocateMappedFileService: shutdown completed with mapped-file cleanup pending"
            );
        }
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
    request_id: u64,
}

impl AllocationQueueEntry {
    fn from_request(request: &AllocateRequest) -> Self {
        Self {
            key: request.key.clone(),
            request_id: request.id(),
        }
    }

    fn file_path(&self) -> &str {
        self.key.file_path()
    }

    fn file_size(&self) -> i32 {
        self.key.file_size()
    }

    fn request_id(&self) -> u64 {
        self.request_id
    }
}

impl PartialOrd for AllocationQueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AllocationQueueEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.request_id.cmp(&other.request_id))
    }
}

/// Request to allocate a new MappedFile
///
/// Corresponds to Java's AllocateRequest inner class:
/// - Uses Notify + AtomicBool instead of CountDownLatch for async support
/// - Delegates request identity and priority ordering to the Local boundary
struct AllocateRequest {
    /// Process-local identity used to fence stale queue entries.
    request_id: u64,

    /// Runtime-neutral path, size, and ordering identity.
    key: MappedFileAllocationRequestKey,

    /// Count and charged-file-byte ownership for this canonical request.
    permit: Mutex<Option<ResourcePermit>>,

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
    fn new(request_id: u64, file_path: String, file_size: i32, permit: ResourcePermit) -> Self {
        Self {
            request_id,
            key: MappedFileAllocationRequestKey::new(file_path, file_size),
            permit: Mutex::new(Some(permit)),
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

    fn id(&self) -> u64 {
        self.request_id
    }

    fn file_size(&self) -> i32 {
        self.key.file_size()
    }

    fn take_permit(&self) -> Option<ResourcePermit> {
        self.permit.lock().take()
    }

    /// Wait for allocation to complete (like CountDownLatch.await())
    async fn wait(&self) {
        loop {
            let notified = self.completion.notified();
            tokio::pin!(notified);
            let _ = notified.as_mut().enable();
            if self.completed.load(Ordering::Acquire) {
                return;
            }
            notified.await;
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
        let (lock, condvar) = &*self.blocking_completion;
        let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if !self.completed.swap(true, Ordering::AcqRel) {
            self.completion.notify_waiters();
            condvar.notify_all();
        }
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
    use rocketmq_runtime::ProcessMemoryLimit;
    use rocketmq_runtime::ResourceBudget;
    use rocketmq_runtime::ResourceBudgetTree;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use tempfile::tempdir;

    use super::*;

    fn test_allocation_budget() -> ResourceBudget {
        test_allocation_budget_with_limit(16)
    }

    fn test_allocation_budget_with_limit(item_limit: usize) -> ResourceBudget {
        ResourceBudgetTree::new(
            "allocation-service-test",
            BudgetLimit::new(item_limit, 64 * 1024, FullPolicy::Reject),
        )
        .expect("test allocation budget")
        .root()
    }

    fn test_request(file_path: String, file_size: i32) -> Arc<AllocateRequest> {
        static NEXT_TEST_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
        let budget = ResourceBudgetTree::new(
            "allocation-request-test",
            BudgetLimit::new(4, 8_192, FullPolicy::Reject),
        )
        .expect("test budget")
        .root();
        let permit = budget
            .try_acquire_data(usize::try_from(file_size).expect("positive file size"))
            .expect("request budget");
        Arc::new(AllocateRequest::new(
            NEXT_TEST_REQUEST_ID.fetch_add(1, Ordering::Relaxed),
            file_path,
            file_size,
            permit,
        ))
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

    #[tokio::test]
    async fn allocate_request_completion_is_sticky_for_async_and_blocking_waiters() {
        let request = test_request("completion-test".to_owned(), 1024);
        request.complete();

        tokio::time::timeout(Duration::from_millis(100), request.wait())
            .await
            .expect("completion before async registration must remain observable");
        assert!(request.wait_blocking(Duration::from_millis(100)));
    }

    #[tokio::test]
    async fn synchronous_fallback_is_limited_to_never_started_unfenced_service() {
        let temp_dir = tempdir().expect("temp dir");
        let queue_dir = temp_dir.path().join("queue");
        let file_path = queue_dir.join("00000000000000000000");
        let service = AllocateMappedFileService::new_with_config(None, false, false, test_allocation_budget());

        assert!(service.allows_synchronous_fallback(&file_path));
        assert!(service.retire_directory(&queue_dir));
        assert!(!service.allows_synchronous_fallback(&file_path));
        assert!(service.complete_directory_retirement(&queue_dir));
        assert!(service.allows_synchronous_fallback(&file_path));

        service.start();
        assert!(!service.allows_synchronous_fallback(&file_path));
        service.shutdown().await;
        assert!(!service.allows_synchronous_fallback(&file_path));
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

    #[test]
    fn abandoned_ready_request_retains_cleanup_owner_until_retry() {
        let temp_dir = tempdir().expect("temp dir");
        let file_path = temp_dir.path().join("00000000000000000000");
        let service = AllocateMappedFileService::new_with_config(None, false, false, test_allocation_budget());
        let request = match service.admit_request(file_path.to_string_lossy().into_owned(), 1024) {
            AllocationAdmission::Inserted(request) => request,
            _ => panic!("new request must be inserted"),
        };
        let mapped_file = Arc::new(
            DefaultMappedFile::try_new(
                CheetahString::from_string(file_path.to_string_lossy().into_owned()),
                1024,
            )
            .expect("mapped file"),
        );
        assert!(mapped_file.hold());
        *request.mapped_file.write() = Some(mapped_file.clone());

        service.remove_request_if_owned(&request, true);

        let snapshot = service.queue_snapshot();
        assert_eq!(snapshot.pending_cleanup_count, 1);
        assert_eq!(
            snapshot.current_count, 1,
            "retained physical owner keeps its allocation permit"
        );
        assert!(file_path.exists());
        assert!(matches!(
            service.admit_request(file_path.to_string_lossy().into_owned(), 1024),
            AllocationAdmission::Rejected
        ));
        let synchronous_fallback = crate::mapped_file::queue_io::create_mapped_file_for_queue(
            Some(&service),
            &file_path,
            &temp_dir.path().join("00000000000000001024"),
            1024,
            false,
        );
        assert!(synchronous_fallback.is_none());

        mapped_file.release();
        service.retry_pending_cleanup();
        let snapshot = service.queue_snapshot();
        assert_eq!(snapshot.pending_cleanup_count, 0);
        assert_eq!(snapshot.current_count, 0);
        assert!(!file_path.exists());
        assert!(matches!(
            service.admit_request(file_path.to_string_lossy().into_owned(), 1024),
            AllocationAdmission::Inserted(_)
        ));
    }

    #[test]
    fn abandoned_queued_request_fences_path_until_stale_entry_is_observed() {
        let temp_dir = tempdir().expect("temp dir");
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_path = file_path.to_string_lossy().into_owned();
        let service = AllocateMappedFileService::new_with_config(None, false, false, test_allocation_budget());
        let request = match service.admit_request(file_path.clone(), 1024) {
            AllocationAdmission::Inserted(request) => request,
            _ => panic!("new request must be inserted"),
        };

        service.remove_request_if_owned(&request, true);
        drop(request);

        let snapshot = service.queue_snapshot();
        assert_eq!(
            snapshot.current_count, 1,
            "stale heap keys remain inside the bounded queue budget"
        );
        assert_eq!(snapshot.pending_cleanup_count, 1);
        assert!(matches!(
            service.admit_request(file_path.clone(), 1024),
            AllocationAdmission::Rejected
        ));

        assert!(AllocateMappedFileService::mmap_operation(
            &service.request_table,
            &service.request_queue,
            &service.pending_cleanup,
            &service.has_exception,
            &service.transient_store_pool,
            service.warm_mapped_file_config,
            #[cfg(feature = "observability")]
            &service.store_metrics,
        ));

        assert_eq!(service.queue_snapshot().pending_cleanup_count, 0);
        assert!(matches!(
            service.admit_request(file_path, 1024),
            AllocationAdmission::Inserted(_)
        ));
    }

    #[test]
    fn failed_request_requeue_keeps_table_identity_locked_until_heap_publication() {
        let temp_dir = tempdir().expect("temp dir");
        let file_path = temp_dir.path().join("00000000000000000000");
        let service = AllocateMappedFileService::new_with_config(None, false, false, test_allocation_budget());
        let request = match service.admit_request(file_path.to_string_lossy().into_owned(), 1024) {
            AllocationAdmission::Inserted(request) => request,
            _ => panic!("request must be inserted"),
        };
        let entry = service.request_queue.write().pop().expect("queued request");
        let heap_guard = service.request_queue.write();
        let request_table = service.request_table.clone();
        let request_queue = service.request_queue.clone();
        let request_for_worker = request.clone();
        let (owned_tx, owned_rx) = std::sync::mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            AllocateMappedFileService::requeue_failed_request_if_owned_with_hook(
                &request_table,
                &request_queue,
                &request_for_worker,
                entry,
                || owned_tx.send(()).expect("signal owned request"),
            )
        });

        owned_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("worker must observe the owned table identity");
        assert!(service.request_table.try_write().is_none());
        drop(heap_guard);

        assert!(worker.join().expect("requeue worker"));
        assert_eq!(service.request_queue.read().len(), 1);
        service.remove_request_if_owned(&request, false);
        service.request_queue.write().clear();
    }

    #[test]
    fn retained_cleanup_owner_holds_budget_until_namespace_is_removed() {
        let temp_dir = tempdir().expect("temp dir");
        let first_path = temp_dir.path().join("00000000000000000000");
        let second_path = temp_dir.path().join("00000000000000001024");
        let service =
            AllocateMappedFileService::new_with_config(None, false, false, test_allocation_budget_with_limit(1));
        let request = match service.admit_request(first_path.to_string_lossy().into_owned(), 1024) {
            AllocationAdmission::Inserted(request) => request,
            _ => panic!("first request must be inserted"),
        };
        let mapped_file = Arc::new(
            DefaultMappedFile::try_new(CheetahString::from(first_path.to_string_lossy().into_owned()), 1024)
                .expect("mapped file"),
        );
        assert!(mapped_file.hold());
        *request.mapped_file.write() = Some(mapped_file.clone());
        service.remove_request_if_owned(&request, true);

        assert!(matches!(
            service.admit_request(second_path.to_string_lossy().into_owned(), 1024),
            AllocationAdmission::Rejected
        ));
        assert_eq!(service.queue_snapshot().current_count, 1);

        mapped_file.release();
        service.retry_pending_cleanup();
        assert!(matches!(
            service.admit_request(second_path.to_string_lossy().into_owned(), 1024),
            AllocationAdmission::Inserted(_)
        ));
    }

    #[test]
    fn cleanup_batch_attempts_each_retained_owner_before_retrying_a_failure() {
        let temp_dir = tempdir().expect("temp dir");
        let first_path = temp_dir.path().join("00000000000000000000");
        let second_path = temp_dir.path().join("00000000000000001024");
        let service = AllocateMappedFileService::new_with_config(None, false, false, test_allocation_budget());

        let mut retained = Vec::new();
        for path in [&first_path, &second_path] {
            let request = match service.admit_request(path.to_string_lossy().into_owned(), 1024) {
                AllocationAdmission::Inserted(request) => request,
                _ => panic!("request must be inserted"),
            };
            let mapped_file = Arc::new(
                DefaultMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), 1024)
                    .expect("mapped file"),
            );
            *request.mapped_file.write() = Some(mapped_file.clone());
            service.remove_request_if_owned(&request, true);
            retained.push(mapped_file);
        }
        assert!(retained[0].hold());

        service.retry_pending_cleanup();

        assert!(first_path.exists(), "the held owner must remain retryable");
        assert!(!second_path.exists(), "a prior failure must not starve the next owner");
        assert_eq!(service.queue_snapshot().pending_cleanup_count, 1);

        retained[0].release();
        service.retry_pending_cleanup();
        assert!(!first_path.exists());
        assert_eq!(service.queue_snapshot().pending_cleanup_count, 0);
    }

    #[test]
    fn dropped_cleanup_attempt_restores_retryable_owner() {
        let temp_dir = tempdir().expect("temp dir");
        let file_path = temp_dir.path().join("00000000000000000000");
        let registry = Arc::new(Mutex::new(HashMap::new()));
        let key = AllocateMappedFileService::cleanup_key(file_path.to_string_lossy().as_ref(), 7);
        let owner = Arc::new(PendingMappedFileCleanup {
            mapped_file: Arc::new(
                DefaultMappedFile::try_new(CheetahString::from(file_path.to_string_lossy().into_owned()), 1024)
                    .expect("mapped file"),
            ),
            _permit: None,
            attempts: AtomicU64::new(0),
        });
        registry
            .lock()
            .insert(key.clone(), PendingAllocationCleanup::Cleaning(owner.clone()));

        drop(PendingCleanupAttempt {
            registry: registry.clone(),
            key: key.clone(),
            owner,
            completed: false,
        });

        assert!(matches!(
            registry.lock().get(&key),
            Some(PendingAllocationCleanup::Retained(_))
        ));
    }

    #[test]
    fn directory_retirement_fences_late_allocation_and_keeps_inflight_identity() {
        let temp_dir = tempdir().expect("temp dir");
        let queue_dir = temp_dir.path().join("queue");
        std::fs::create_dir(&queue_dir).expect("queue dir");
        let file_path = queue_dir.join("00000000000000000000");
        let file_path_text = file_path.to_string_lossy().into_owned();
        let service = AllocateMappedFileService::new_with_config(None, false, false, test_allocation_budget());
        let request = match service.admit_request(file_path_text.clone(), 1024) {
            AllocationAdmission::Inserted(request) => request,
            _ => panic!("request must be inserted"),
        };
        let claimed = service.request_queue.write().pop().expect("simulate claimed request");
        assert_eq!(claimed.request_id(), request.id());

        assert!(!service.retire_directory(&queue_dir));
        assert!(matches!(
            service.admit_request(file_path_text.clone(), 1024),
            AllocationAdmission::Rejected
        ));
        assert_eq!(service.queue_snapshot().current_count, 1);

        let late_mapping = Arc::new(
            DefaultMappedFile::try_new(CheetahString::from(file_path_text.clone()), 1024).expect("late mapping"),
        );
        AllocateMappedFileService::retain_for_cleanup(
            &service.pending_cleanup,
            &file_path_text,
            request.id(),
            late_mapping,
            request.take_permit(),
        );
        request.complete();
        service.retry_pending_cleanup();

        assert!(service.is_directory_retirement_complete(&queue_dir));
        assert!(!file_path.exists());
        assert_eq!(service.queue_snapshot().current_count, 0);
        assert!(matches!(
            service.admit_request(file_path_text.clone(), 1024),
            AllocationAdmission::Rejected
        ));
        assert!(service.complete_directory_retirement(&queue_dir));
        assert!(matches!(
            service.admit_request(file_path_text, 1024),
            AllocationAdmission::Inserted(_)
        ));
    }

    #[test]
    fn exception_after_admission_releases_request_ownership_and_fences_fallback() {
        let temp_dir = tempdir().expect("temp dir");
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_path = file_path.to_string_lossy().into_owned();
        let service = AllocateMappedFileService::new_with_config(None, false, false, test_allocation_budget());
        service.has_exception.store(true, Ordering::Release);

        assert!(service.allocate_mapped_file_blocking(file_path.clone(), 1024).is_err());

        let snapshot = service.queue_snapshot();
        assert_eq!(
            snapshot.current_count, 1,
            "the queued cleanup fence must retain the original bounded permit"
        );
        assert_eq!(snapshot.pending_cleanup_count, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn non_positive_file_size_is_rejected_without_admission_side_effects() {
        let temp_dir = tempdir().expect("temp dir");
        let service = AllocateMappedFileService::new_with_config(None, false, false, test_allocation_budget());
        let existing_path = temp_dir.path().join("00000000000000000000");
        assert!(matches!(
            service.admit_request(existing_path.to_string_lossy().into_owned(), 1024),
            AllocationAdmission::Inserted(_)
        ));

        for (index, file_size) in [0, -1].into_iter().enumerate() {
            let file_path = temp_dir.path().join(format!("invalid-{index}"));
            assert!(matches!(
                service.admit_request(file_path.to_string_lossy().into_owned(), file_size),
                AllocationAdmission::Rejected
            ));
            assert!(service
                .put_request_and_return_mapped_file(
                    existing_path.to_string_lossy().into_owned(),
                    String::new(),
                    file_size,
                )
                .await
                .expect("invalid size is a rejected request")
                .is_none());
            assert!(!file_path.exists());
        }

        let snapshot = service.queue_snapshot();
        assert_eq!(snapshot.current_count, 1);
        assert_eq!(snapshot.charged_bytes, 1024);
        assert_eq!(snapshot.queued_count, 1);
        assert_eq!(snapshot.pending_cleanup_count, 0);
        service.shutdown().await;
    }

    #[tokio::test]
    async fn public_u64_file_sizes_are_checked_before_admission() {
        let temp_dir = tempdir().expect("temp dir");
        let service = AllocateMappedFileService::new_with_config(None, false, false, test_allocation_budget());
        let invalid_size = i32::MAX as u64 + 1;
        let async_path = temp_dir.path().join("00000000000000000000");
        let blocking_path = temp_dir.path().join("00000000000000000001");
        let background_path = temp_dir.path().join("00000000000000000002");

        assert!(service
            .submit_request(async_path.to_string_lossy().into_owned(), invalid_size)
            .await
            .is_err());
        assert!(service
            .allocate_mapped_file_blocking(blocking_path.to_string_lossy().into_owned(), invalid_size)
            .is_err());
        service.submit_request_in_background(background_path.to_string_lossy().into_owned(), invalid_size);

        let snapshot = service.queue_snapshot();
        assert_eq!(snapshot.current_count, 0);
        assert_eq!(snapshot.charged_bytes, 0);
        assert_eq!(snapshot.queued_count, 0);
        assert_eq!(snapshot.pending_cleanup_count, 0);
        assert!(!async_path.exists());
        assert!(!blocking_path.exists());
        assert!(!background_path.exists());
    }

    #[test]
    fn stopped_service_retries_retained_cleanup_for_one_retired_directory() {
        let temp_dir = tempdir().expect("temp dir");
        let other_temp_dir = tempdir().expect("other temp dir");
        let file_path = temp_dir.path().join("00000000000000000000");
        let other_file_path = other_temp_dir.path().join("00000000000000000000");
        let service = AllocateMappedFileService::new_with_config(None, false, false, test_allocation_budget());
        let request = match service.admit_request(file_path.to_string_lossy().into_owned(), 1024) {
            AllocationAdmission::Inserted(request) => request,
            _ => panic!("request must be inserted"),
        };
        let other_request = match service.admit_request(other_file_path.to_string_lossy().into_owned(), 1024) {
            AllocationAdmission::Inserted(request) => request,
            _ => panic!("other request must be inserted"),
        };
        let mapped_file = Arc::new(
            DefaultMappedFile::try_new(
                CheetahString::from_string(file_path.to_string_lossy().into_owned()),
                1024,
            )
            .expect("mapped file"),
        );
        let other_mapped_file = Arc::new(
            DefaultMappedFile::try_new(
                CheetahString::from_string(other_file_path.to_string_lossy().into_owned()),
                1024,
            )
            .expect("other mapped file"),
        );
        assert!(mapped_file.hold());
        *request.mapped_file.write() = Some(mapped_file.clone());
        *other_request.mapped_file.write() = Some(other_mapped_file);
        service.remove_request_if_owned(&request, true);
        service.remove_request_if_owned(&other_request, true);
        service.stopped.store(true, Ordering::Release);

        assert!(!service.retire_directory(temp_dir.path()));
        assert_eq!(service.queue_snapshot().pending_cleanup_count, 2);
        assert_eq!(service.queue_snapshot().current_count, 2);
        assert!(other_file_path.exists());

        mapped_file.release();
        assert!(service.retry_retired_directory_cleanup_once(temp_dir.path()));
        assert_eq!(service.queue_snapshot().pending_cleanup_count, 1);
        assert_eq!(service.queue_snapshot().current_count, 1);
        assert!(!file_path.exists());
        assert!(other_file_path.exists());

        assert!(service.retire_directory(other_temp_dir.path()));
        assert_eq!(service.queue_snapshot().pending_cleanup_count, 0);
        assert_eq!(service.queue_snapshot().current_count, 0);
        assert!(!other_file_path.exists());
    }

    #[test]
    fn shutdown_drains_releasable_pending_cleanup() {
        let temp_dir = tempdir().expect("temp dir");
        let file_path = temp_dir.path().join("00000000000000000000");
        let runtime_owner = RuntimeOwner::plan(RuntimeConfig::default())
            .expect("default runtime configuration is valid")
            .with_memory_limit(ProcessMemoryLimit::configured(64 * 1024 * 1024).expect("test memory limit"))
            .build()
            .expect("test runtime owner");
        let runtime_context = runtime_owner.root_context().component("allocation-cleanup-test");
        let service = AllocateMappedFileService::new_with_config_and_storage_io(
            None,
            false,
            false,
            test_allocation_budget(),
            runtime_context.storage_io().clone(),
        );
        let request = match service.admit_request(file_path.to_string_lossy().into_owned(), 1024) {
            AllocationAdmission::Inserted(request) => request,
            _ => panic!("new request must be inserted"),
        };
        let mapped_file = Arc::new(
            DefaultMappedFile::try_new(
                CheetahString::from_string(file_path.to_string_lossy().into_owned()),
                1024,
            )
            .expect("mapped file"),
        );
        assert!(mapped_file.hold());
        *request.mapped_file.write() = Some(mapped_file.clone());
        service.remove_request_if_owned(&request, true);
        mapped_file.release();

        runtime_owner.block_on(service.shutdown());

        assert_eq!(service.queue_snapshot().pending_cleanup_count, 0);
        assert!(!file_path.exists());
        runtime_owner
            .shutdown_runtime_blocking()
            .expect("test runtime shutdown");
    }

    #[test]
    fn shutdown_fences_claimed_request_before_managed_worker_join() {
        let temp_dir = tempdir().expect("temp dir");
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_path_text = file_path.to_string_lossy().into_owned();
        let runtime_owner = RuntimeOwner::plan(RuntimeConfig::default())
            .expect("default runtime configuration is valid")
            .with_memory_limit(ProcessMemoryLimit::configured(64 * 1024 * 1024).expect("test memory limit"))
            .build()
            .expect("test runtime owner");
        let runtime_context = runtime_owner
            .root_context()
            .component("allocation-claimed-shutdown-test");
        let service = AllocateMappedFileService::new_with_config_and_storage_io(
            None,
            false,
            false,
            test_allocation_budget(),
            runtime_context.storage_io().clone(),
        );
        let request = match service.admit_request(file_path_text.clone(), 1024) {
            AllocationAdmission::Inserted(request) => request,
            _ => panic!("request must be inserted"),
        };
        let claimed = service.request_queue.write().pop().expect("claimed request");
        assert_eq!(claimed.request_id(), request.id());

        let pending_cleanup = service.pending_cleanup.clone();
        let request_for_worker = request.clone();
        let worker = thread::spawn(move || {
            assert!(request_for_worker.wait_blocking(Duration::from_secs(2)));
            let mapped_file = Arc::new(
                DefaultMappedFile::try_new(CheetahString::from(file_path_text), 1024).expect("late mapped file"),
            );
            AllocateMappedFileService::retain_for_cleanup(
                &pending_cleanup,
                request_for_worker.file_path(),
                request_for_worker.id(),
                mapped_file,
                request_for_worker.take_permit(),
            );
        });
        *service.worker_handle.lock() = Some(worker);

        runtime_owner.block_on(service.shutdown());

        assert_eq!(service.queue_snapshot().pending_cleanup_count, 0);
        assert_eq!(service.queue_snapshot().current_count, 0);
        assert!(!file_path.exists());
        runtime_owner
            .shutdown_runtime_blocking()
            .expect("test runtime shutdown");
    }
}
