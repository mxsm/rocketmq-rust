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

use std::path::Path;

use cheetah_string::CheetahString;
use rocketmq_error::STORAGE_IO_FAILED;
use rocketmq_error::STORAGE_REQUEST_INVALID;

use super::default_mapped_file_impl::DefaultMappedFile;
use super::FlushStrategy;
use crate::store_error::StoreComponent;
use crate::store_error::StoreError;
use crate::store_error::StoreOperation;

fn invalid_configuration(detail: impl Into<String>) -> StoreError {
    StoreError::new(&STORAGE_REQUEST_INVALID, StoreOperation::Start)
        .in_component(StoreComponent::Configuration)
        .with_detail(detail)
}

fn mapped_file_load_failure(source: impl std::error::Error + Send + Sync + 'static) -> StoreError {
    StoreError::new(&STORAGE_IO_FAILED, StoreOperation::Load)
        .in_component(StoreComponent::MappedFile)
        .with_detail("mapped-file construction failed")
        .with_source(source)
}

/// Builder for configuring and creating mapped file instances.
///
/// Provides a fluent API for constructing mapped files with custom settings.
/// This pattern enables clear, self-documenting configuration while enforcing
/// required parameters at compile time.
///
/// # Required Parameters
///
/// - File path (via `new()`)
/// - File size (via `size()` or `size_mb()`)
///
/// # Optional Parameters
///
/// - Flush strategy (only `FlushStrategy::Async` is currently supported)
/// - Transient store pool usage (must remain disabled)
/// - Metrics collection (must remain enabled)
/// - Warmup on creation (must remain disabled)
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_store::{FlushStrategy, MappedFileBuilder};
///
/// // Basic configuration
/// let file = MappedFileBuilder::new("data/commitlog/00000000000000000000")
///     .size_mb(1024)  // 1 GB file
///     .build()?;
///
/// Unsupported options return a configuration error before the file is created.
/// ```
#[derive(Debug, Clone)]
pub struct MappedFileBuilder {
    /// Path to the file to map
    file_path: String,

    /// Size of the mapped file in bytes (required)
    file_size: Option<u64>,

    /// Flush strategy to use
    flush_strategy: FlushStrategy,

    /// Whether to use transient store pool for writes
    use_transient_store_pool: bool,

    /// Whether to enable metrics collection
    enable_metrics: bool,

    /// Whether to warmup the file on creation (touch all pages)
    warmup: bool,

    /// Starting offset for the file name
    file_from_offset: Option<u64>,
}

impl MappedFileBuilder {
    /// Creates a new builder for the specified file path.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the file to memory-map
    ///
    /// # Returns
    ///
    /// A new builder with default settings
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let builder = MappedFileBuilder::new("data/commitlog/00000000000000000000");
    /// ```
    pub fn new<P: AsRef<Path>>(file_path: P) -> Self {
        Self {
            file_path: file_path.as_ref().to_string_lossy().to_string(),
            file_size: None,
            flush_strategy: FlushStrategy::Async,
            use_transient_store_pool: false,
            enable_metrics: true,
            warmup: false,
            file_from_offset: None,
        }
    }

    /// Sets the file size in bytes.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Size of the mapped file in bytes
    ///
    /// # Returns
    ///
    /// Self for method chaining
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let builder = MappedFileBuilder::new("data/file")
    ///     .size(1024 * 1024 * 1024);  // 1 GB
    /// ```
    pub fn size(mut self, bytes: u64) -> Self {
        self.file_size = Some(bytes);
        self
    }

    /// Sets the file size in megabytes.
    ///
    /// # Arguments
    ///
    /// * `mb` - Size of the mapped file in megabytes
    ///
    /// # Returns
    ///
    /// Self for method chaining
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let builder = MappedFileBuilder::new("data/file")
    ///     .size_mb(1024);  // 1 GB
    /// ```
    pub fn size_mb(self, mb: u64) -> Self {
        self.size(mb.saturating_mul(1024 * 1024))
    }

    /// Sets the file size in gigabytes.
    ///
    /// # Arguments
    ///
    /// * `gb` - Size of the mapped file in gigabytes
    ///
    /// # Returns
    ///
    /// Self for method chaining
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let builder = MappedFileBuilder::new("data/file")
    ///     .size_gb(1);  // 1 GB
    /// ```
    pub fn size_gb(self, gb: u64) -> Self {
        self.size(gb.saturating_mul(1024 * 1024 * 1024))
    }

    /// Sets the flush strategy.
    ///
    /// # Arguments
    ///
    /// * `strategy` - The flush strategy to use
    ///
    /// # Returns
    ///
    /// Self for method chaining
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use std::time::Duration;
    ///
    /// let builder = MappedFileBuilder::new("data/file")
    ///     .flush_strategy(FlushStrategy::periodic(Duration::from_millis(500)));
    /// ```
    pub fn flush_strategy(mut self, strategy: FlushStrategy) -> Self {
        self.flush_strategy = strategy;
        self
    }

    /// Enables the use of transient store pool for writes.
    ///
    /// Transient store pool provides a temporary write buffer that can improve
    /// write performance by batching and reducing direct mmap writes.
    ///
    /// # Returns
    ///
    /// Self for method chaining
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let builder = MappedFileBuilder::new("data/file")
    ///     .enable_transient_store_pool();
    /// ```
    pub fn enable_transient_store_pool(mut self) -> Self {
        self.use_transient_store_pool = true;
        self
    }

    /// Disables metrics collection.
    ///
    /// By default, metrics are enabled. Disabling them reduces overhead
    /// (approximately 1-2 ns per operation) at the cost of observability.
    ///
    /// # Returns
    ///
    /// Self for method chaining
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let builder = MappedFileBuilder::new("data/file")
    ///     .disable_metrics();
    /// ```
    pub fn disable_metrics(mut self) -> Self {
        self.enable_metrics = false;
        self
    }

    /// Enables file warmup on creation.
    ///
    /// Warmup touches all pages in the mapped file to pre-fault them into
    /// physical memory. This eliminates page faults during operation at the
    /// cost of longer initialization time.
    ///
    /// # Performance Impact
    ///
    /// - Initialization time: +100-500 ms per GB (depends on I/O speed)
    /// - First-access latency: Eliminated
    /// - Total runtime: Usually faster for files accessed sequentially
    ///
    /// # Arguments
    ///
    /// * `enabled` - Whether to warmup the file
    ///
    /// # Returns
    ///
    /// Self for method chaining
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let builder = MappedFileBuilder::new("data/file")
    ///     .warmup(true);
    /// ```
    pub fn warmup(mut self, enabled: bool) -> Self {
        self.warmup = enabled;
        self
    }

    /// Sets the file starting offset.
    ///
    /// This is typically extracted from the filename in RocketMQ's naming
    /// convention (e.g., "00000000000000000000" = offset 0).
    ///
    /// # Arguments
    ///
    /// * `offset` - Starting offset for this file
    ///
    /// # Returns
    ///
    /// Self for method chaining
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let builder = MappedFileBuilder::new("data/commitlog/00000000001073741824")
    ///     .file_from_offset(1073741824);  // 1 GB offset
    /// ```
    pub fn file_from_offset(mut self, offset: u64) -> Self {
        self.file_from_offset = Some(offset);
        self
    }

    /// Validates the builder configuration.
    ///
    /// # Returns
    ///
    /// `Ok(())` if configuration is valid, `Err` otherwise
    fn validate(&self) -> Result<(), StoreError> {
        let Some(size) = self.file_size else {
            return Err(invalid_configuration("file size must be specified"));
        };
        if size == 0 {
            return Err(invalid_configuration("file size must be greater than zero"));
        }

        // Validate size is reasonable (not too large to cause issues)
        const MAX_FILE_SIZE: u64 = 16 * 1024 * 1024 * 1024; // 16 GB
        if size > MAX_FILE_SIZE {
            return Err(invalid_configuration(format!(
                "file size {} exceeds maximum {} bytes",
                size, MAX_FILE_SIZE
            )));
        }

        if self.flush_strategy != FlushStrategy::Async {
            return Err(invalid_configuration(
                "mapped file builder only supports the async flush strategy",
            ));
        }
        if self.use_transient_store_pool {
            return Err(invalid_configuration(
                "mapped file builder does not support transient store pool allocation",
            ));
        }
        if !self.enable_metrics {
            return Err(invalid_configuration(
                "mapped file builder does not support disabling mapped-file metrics",
            ));
        }
        if self.warmup {
            return Err(invalid_configuration(
                "mapped file builder does not support eager mapped-file warmup",
            ));
        }

        let file_name = Path::new(&self.file_path)
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid_configuration("mapped file name is missing"))?;
        let parsed_offset = file_name.parse::<u64>().map_err(|_| {
            invalid_configuration(format!(
                "mapped file name must be a numeric starting offset: {file_name}"
            ))
        })?;
        match self.file_from_offset {
            Some(configured_offset) if configured_offset != parsed_offset => {
                return Err(invalid_configuration(format!(
                    "configured starting offset does not match mapped file name: configured={configured_offset}, \
                     parsed={parsed_offset}"
                )));
            }
            _ => {}
        }

        Ok(())
    }

    /// Builds the configured mapped file.
    ///
    /// # Returns
    ///
    /// A new mapped file instance or an error if:
    /// - Required parameters are missing
    /// - File cannot be created or opened
    /// - Memory mapping fails
    ///
    /// # Errors
    ///
    /// Returns a configuration error if validation fails, or a mapped-file I/O error if file
    /// creation or memory mapping fails.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let file = MappedFileBuilder::new("data/commitlog/00000000000000000000")
    ///     .size_mb(1024)
    ///     .build()?;
    /// ```
    pub fn build(self) -> Result<DefaultMappedFile, StoreError> {
        self.validate()?;
        let file_size = self
            .file_size
            .ok_or_else(|| invalid_configuration("file size must be specified"))?;
        DefaultMappedFile::try_new(CheetahString::from_string(self.file_path), file_size)
            .map_err(mapped_file_load_failure)
    }

    /// Returns the configured file size in bytes.
    ///
    /// # Returns
    ///
    /// `Some(size)` if size was set, `None` otherwise
    pub fn get_file_size(&self) -> Option<u64> {
        self.file_size
    }

    /// Returns the configured flush strategy.
    pub fn get_flush_strategy(&self) -> &FlushStrategy {
        &self.flush_strategy
    }

    /// Returns whether transient store pool is enabled.
    pub fn is_transient_store_pool_enabled(&self) -> bool {
        self.use_transient_store_pool
    }

    /// Returns whether metrics collection is enabled.
    pub fn is_metrics_enabled(&self) -> bool {
        self.enable_metrics
    }

    /// Returns whether warmup is enabled.
    pub fn is_warmup_enabled(&self) -> bool {
        self.warmup
    }

    /// Returns the configured file starting offset.
    pub fn get_file_from_offset(&self) -> Option<u64> {
        self.file_from_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_basic() {
        let builder = MappedFileBuilder::new("test/file")
            .size(1024 * 1024)
            .flush_strategy(FlushStrategy::Sync);

        assert_eq!(builder.get_file_size(), Some(1024 * 1024));
        assert!(matches!(builder.get_flush_strategy(), FlushStrategy::Sync));
    }

    #[test]
    fn test_builder_size_helpers() {
        let builder_mb = MappedFileBuilder::new("test/file").size_mb(10);
        assert_eq!(builder_mb.get_file_size(), Some(10 * 1024 * 1024));

        let builder_gb = MappedFileBuilder::new("test/file").size_gb(1);
        assert_eq!(builder_gb.get_file_size(), Some(1024 * 1024 * 1024));
    }

    #[test]
    fn test_builder_options() {
        let builder = MappedFileBuilder::new("test/file")
            .size(1024)
            .enable_transient_store_pool()
            .disable_metrics()
            .warmup(true)
            .file_from_offset(1000);

        assert!(builder.is_transient_store_pool_enabled());
        assert!(!builder.is_metrics_enabled());
        assert!(builder.is_warmup_enabled());
        assert_eq!(builder.get_file_from_offset(), Some(1000));
    }

    #[test]
    fn test_validation_missing_size() {
        let builder = MappedFileBuilder::new("test/file");
        assert!(builder.validate().is_err());
    }

    #[test]
    fn test_validation_zero_size() {
        let builder = MappedFileBuilder::new("test/file").size(0);
        assert!(builder.validate().is_err());
    }

    #[test]
    fn test_validation_too_large() {
        let builder = MappedFileBuilder::new("test/file").size(20 * 1024 * 1024 * 1024);
        assert!(builder.validate().is_err());
    }

    #[test]
    fn test_validation_success() {
        let builder = MappedFileBuilder::new("test/file").size(1024 * 1024);
        assert!(builder.validate().is_ok());
    }
}
