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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::RwLock;
use rocketmq_error::RocketMQError;
use tokio::sync::Notify;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::transient_store_pool::TransientStorePool;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;

/// Timeout for waiting on file allocation (matches Java: 5 seconds)
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

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
    request_queue: Arc<RwLock<BinaryHeap<Arc<AllocateRequest>>>>,

    /// Exception flag (set when allocation fails)
    has_exception: Arc<AtomicBool>,

    /// Shutdown flag
    stopped: Arc<AtomicBool>,

    /// Notification for new requests
    notify: Arc<Notify>,

    /// Background worker handle
    worker_handle: Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,

    /// TransientStorePool reference (optional)
    transient_store_pool: Option<Arc<TransientStorePool>>,

    /// Whether to enable TransientStorePool
    transient_store_pool_enable: bool,

    /// Whether to fast fail when no buffer available in pool
    fast_fail_if_no_buffer: bool,
}

impl Default for AllocateMappedFileService {
    fn default() -> Self {
        Self::new()
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
    ) -> Self {
        let request_table = Arc::new(RwLock::new(HashMap::new()));
        let request_queue = Arc::new(RwLock::new(BinaryHeap::new()));
        let has_exception = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());

        Self {
            request_table,
            request_queue,
            has_exception,
            stopped,
            notify,
            worker_handle: Arc::new(parking_lot::Mutex::new(None)),
            transient_store_pool,
            transient_store_pool_enable,
            fast_fail_if_no_buffer,
        }
    }

    /// Create a new AllocateMappedFileService with default configuration
    /// (no TransientStorePool)
    pub fn new() -> Self {
        Self::new_with_config(None, false, false)
    }

    /// Start the background worker thread
    /// Corresponds to Java's ServiceThread.start()
    pub fn start(&self) {
        let request_table = self.request_table.clone();
        let request_queue = self.request_queue.clone();
        let has_exception = self.has_exception.clone();
        let stopped = self.stopped.clone();
        let notify = self.notify.clone();
        let transient_store_pool = self.transient_store_pool.clone();

        let handle = tokio::spawn(async move {
            Self::run_worker(
                request_table,
                request_queue,
                has_exception,
                stopped,
                notify,
                transient_store_pool,
            )
            .await;
        });

        *self.worker_handle.lock() = Some(handle);
        info!("AllocateMappedFileService started");
    }

    /// Main worker loop - corresponds to Java's run() method
    async fn run_worker(
        request_table: Arc<RwLock<HashMap<String, Arc<AllocateRequest>>>>,
        request_queue: Arc<RwLock<BinaryHeap<Arc<AllocateRequest>>>>,
        has_exception: Arc<AtomicBool>,
        stopped: Arc<AtomicBool>,
        notify: Arc<Notify>,
        transient_store_pool: Option<Arc<TransientStorePool>>,
    ) {
        info!("AllocateMappedFileService: service started");

        while !stopped.load(Ordering::Relaxed) {
            // Wait for notification or timeout
            tokio::select! {
                _ = notify.notified() => {
                    // Process available requests
                    while !stopped.load(Ordering::Relaxed) {
                        if !Self::mmap_operation(
                            &request_table,
                            &request_queue,
                            &has_exception,
                            &transient_store_pool,
                        ).await {
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Periodic check
                }
            }
        }

        info!("AllocateMappedFileService: service end");
    }

    /// Core file allocation operation - corresponds to Java's mmapOperation()
    ///
    /// Returns false if interrupted or no requests available
    async fn mmap_operation(
        request_table: &Arc<RwLock<HashMap<String, Arc<AllocateRequest>>>>,
        request_queue: &Arc<RwLock<BinaryHeap<Arc<AllocateRequest>>>>,
        has_exception: &Arc<AtomicBool>,
        transient_store_pool: &Option<Arc<TransientStorePool>>,
    ) -> bool {
        // Pop request from priority queue
        let req = {
            let mut queue = request_queue.write();
            queue.pop()
        };

        let req = match req {
            Some(r) => r,
            None => return false, // No requests available
        };

        // Check if request still valid in table
        let expected_request = {
            let table = request_table.read();
            table.get(&req.file_path).cloned()
        };

        let expected_request = match expected_request {
            Some(r) => r,
            None => {
                warn!(
                    "this mmap request expired, maybe cause timeout {} {}",
                    req.file_path, req.file_size
                );
                return true;
            }
        };

        // Verify it's the same request object
        if !Arc::ptr_eq(&expected_request, &req) {
            warn!(
                "never expected here, maybe cause timeout {} {}",
                req.file_path, req.file_size
            );
            return true;
        }

        // Check if already allocated
        if req.mapped_file.read().is_some() {
            return true;
        }

        // Perform actual file allocation
        let result = Self::create_mapped_file(&req, transient_store_pool).await;

        match result {
            Ok(mapped_file) => {
                *req.mapped_file.write() = Some(mapped_file);
                has_exception.store(false, Ordering::Relaxed);

                // Signal completion (like CountDownLatch.countDown())
                req.complete();

                true
            }
            Err(e) => {
                error!(
                    "AllocateMappedFileService: failed to create mapped file {}: {}",
                    req.file_path, e
                );
                has_exception.store(true, Ordering::Relaxed);

                // Re-queue the request for retry
                request_queue.write().push(req);

                // Small delay before retry
                tokio::time::sleep(Duration::from_millis(1)).await;

                false
            }
        }
    }

    /// Create a MappedFile with optional TransientStorePool
    ///
    /// Corresponds to Java's MappedFile creation logic in mmapOperation()
    async fn create_mapped_file(
        req: &AllocateRequest,
        transient_store_pool: &Option<Arc<TransientStorePool>>,
    ) -> Result<Arc<DefaultMappedFile>, RocketMQError> {
        let start = std::time::Instant::now();
        let file_path = req.file_path.clone();
        let file_size = req.file_size as u64;
        let transient_pool = transient_store_pool.clone();

        // Perform blocking I/O in separate thread
        let mapped_file: DefaultMappedFile =
            tokio::task::spawn_blocking(move || -> Result<DefaultMappedFile, RocketMQError> {
                if let Some(pool) = transient_pool {
                    // With TransientStorePool (zero-copy)
                    Ok(DefaultMappedFile::new_with_transient_store_pool(
                        CheetahString::from_string(file_path.clone()),
                        file_size,
                        (*pool).clone(),
                    ))
                } else {
                    // Standard mmap
                    Ok(DefaultMappedFile::new(
                        CheetahString::from_string(file_path.clone()),
                        file_size,
                    ))
                }
            })
            .await
            .map_err(|e| RocketMQError::StorageWriteFailed {
                path: req.file_path.clone(),
                reason: e.to_string(),
            })??;

        let elapsed = start.elapsed();
        if elapsed.as_millis() > 10 {
            let queue_size = 0; // TODO: pass queue size if needed
            warn!(
                "create mappedFile spent time(ms) {} queue size {} {} {}",
                elapsed.as_millis(),
                queue_size,
                req.file_path,
                req.file_size
            );
        }

        // TODO: Pre-warm mapped file if configured
        // if file_size >= commitlog_size && warm_mapped_file_enable {
        //     mapped_file.warm_mapped_file(...);
        // }

        Ok(Arc::new(mapped_file))
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
        let mut can_submit_requests = 2;

        if self.transient_store_pool_enable {
            if let Some(ref pool) = self.transient_store_pool {
                if self.fast_fail_if_no_buffer {
                    let queue_size = self.request_queue.read().len();
                    can_submit_requests = pool.available_buffer_nums().saturating_sub(queue_size);
                }
            }
        }

        // Submit request for next file
        let next_req = Arc::new(AllocateRequest::new(next_file_path.clone(), file_size));
        let next_put_ok = {
            let mut table = self.request_table.write();
            if table.contains_key(&next_file_path) {
                false
            } else {
                table.insert(next_file_path.clone(), next_req.clone());
                true
            }
        };

        if next_put_ok {
            if can_submit_requests == 0 {
                warn!(
                    "[NOTIFYME]TransientStorePool is not enough, so create mapped file error, RequestQueueSize: {}, \
                     StorePoolSize: {}",
                    self.request_queue.read().len(),
                    self.transient_store_pool
                        .as_ref()
                        .map_or(0, |p| p.available_buffer_nums())
                );
                self.request_table.write().remove(&next_file_path);
                return Ok(None);
            }

            self.request_queue.write().push(next_req.clone());
            self.notify.notify_one();
            can_submit_requests -= 1;
        }

        // Submit request for next-next file (pre-allocation)
        let next_next_req = Arc::new(AllocateRequest::new(next_next_file_path.clone(), file_size));
        let next_next_put_ok = {
            let mut table = self.request_table.write();
            if table.contains_key(&next_next_file_path) {
                false
            } else {
                table.insert(next_next_file_path.clone(), next_next_req.clone());
                true
            }
        };

        if next_next_put_ok {
            if can_submit_requests == 0 {
                warn!(
                    "[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, RequestQueueSize: \
                     {}, StorePoolSize: {}",
                    self.request_queue.read().len(),
                    self.transient_store_pool
                        .as_ref()
                        .map_or(0, |p| p.available_buffer_nums())
                );
                self.request_table.write().remove(&next_next_file_path);
            } else {
                self.request_queue.write().push(next_next_req);
                self.notify.notify_one();
            }
        }

        // Check for exceptions
        if self.has_exception.load(Ordering::Relaxed) {
            warn!("AllocateMappedFileService has exception, so return null");
            return Ok(None);
        }

        // Wait for the next file to be allocated
        let result = {
            let table = self.request_table.read();
            table.get(&next_file_path).cloned()
        };

        if let Some(req) = result {
            // Wait for allocation to complete (with timeout)
            let wait_result = tokio::time::timeout(WAIT_TIMEOUT, req.wait()).await;

            match wait_result {
                Ok(()) => {
                    // Remove from table and return result
                    self.request_table.write().remove(&next_file_path);
                    let mapped_file = req.mapped_file.read().clone();
                    Ok(mapped_file)
                }
                Err(_) => {
                    warn!("create mmap timeout {} {}", req.file_path, req.file_size);
                    Ok(None)
                }
            }
        } else {
            error!("find preallocate mmap failed, this never happen");
            Ok(None)
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

    /// Shutdown the service - corresponds to Java's shutdown()
    pub async fn shutdown(&self) {
        info!("AllocateMappedFileService: shutting down");

        self.stopped.store(true, Ordering::Relaxed);
        self.notify.notify_one();

        // Wait for worker to complete
        let handle = self.worker_handle.lock().take();
        if let Some(handle) = handle {
            let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
        }

        // Clean up pre-allocated files
        let table = self.request_table.read();
        for req in table.values() {
            if let Some(ref mapped_file) = *req.mapped_file.read() {
                info!("delete pre allocated mapped file, {}", req.file_path);
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

/// Request to allocate a new MappedFile
///
/// Corresponds to Java's AllocateRequest inner class:
/// - Uses Notify + AtomicBool instead of CountDownLatch for async support
/// - Implements Ord for priority queue ordering (by file offset)
struct AllocateRequest {
    /// Full file path
    file_path: String,

    /// File size in bytes
    file_size: i32,

    /// Completion notification (equivalent to Java's CountDownLatch)
    completion: Arc<Notify>,

    /// Completion flag
    completed: Arc<AtomicBool>,

    /// The allocated MappedFile (set when complete)
    mapped_file: Arc<RwLock<Option<Arc<DefaultMappedFile>>>>,
}

impl AllocateRequest {
    fn new(file_path: String, file_size: i32) -> Self {
        Self {
            file_path,
            file_size,
            completion: Arc::new(Notify::new()),
            completed: Arc::new(AtomicBool::new(false)),
            mapped_file: Arc::new(RwLock::new(None)),
        }
    }

    /// Wait for allocation to complete (like CountDownLatch.await())
    async fn wait(&self) {
        if !self.completed.load(Ordering::Acquire) {
            self.completion.notified().await;
        }
    }

    /// Signal completion (like CountDownLatch.countDown())
    fn complete(&self) {
        self.completed.store(true, Ordering::Release);
        self.completion.notify_waiters();
    }

    /// Extract file offset from path for priority ordering
    fn file_offset(&self) -> i64 {
        if let Some(separator_idx) = self.file_path.rfind(std::path::MAIN_SEPARATOR) {
            if let Ok(offset) = self.file_path[(separator_idx + 1)..].parse::<i64>() {
                return offset;
            }
        }
        0
    }
}

impl Display for AllocateRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AllocateRequest[file_path={},file_size={}]",
            self.file_path, self.file_size
        )
    }
}

// Implement Ord for priority queue (lower offsets have higher priority)
impl PartialEq for AllocateRequest {
    fn eq(&self, other: &Self) -> bool {
        self.file_path == other.file_path && self.file_size == other.file_size
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
        // Reverse ordering: smaller offsets come out first (min-heap behavior)
        other.file_offset().cmp(&self.file_offset())
    }
}
