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

/*
//! MappedFile Factory
//!
//! Provides factory methods to create the optimal MappedFile implementation
//! based on platform and feature configuration.

use std::io;
use std::sync::Arc;

use cheetah_string::CheetahString;

use crate::base::transient_store_pool::TransientStorePool;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use crate::log_file::mapped_file::io_uring_impl::IoUringMappedFile;

/// MappedFile implementation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MappedFileType {
    /// Standard mmap-based implementation (default)
    #[default]
    Default,

    /// io_uring-based implementation (Linux 5.1+ only)
    /// Enable with: cargo build --features io_uring
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    IoUring,
}

/// Configuration for MappedFile creation
pub struct MappedFileConfig {
    /// Implementation type to use
    pub file_type: MappedFileType,

    /// Optional transient store pool
    pub transient_store_pool: Option<Arc<TransientStorePool>>,

    /// Whether to enable metrics collection
    pub enable_metrics: bool,
}

impl Default for MappedFileConfig {
    fn default() -> Self {
        Self {
            file_type: MappedFileType::default(),
            transient_store_pool: None,
            enable_metrics: true,
        }
    }
}

/// Enum wrapper for different MappedFile implementations
pub enum MappedFileImpl {
    /// Default mmap implementation
    Default(DefaultMappedFile),

    /// io_uring implementation (Linux only)
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    IoUring(IoUringMappedFile),
}

// MappedFile trait is not dyn-compatible due to generic methods
// We use enum dispatch instead of trait objects

/// MappedFile factory
pub struct MappedFileFactory;

impl MappedFileFactory {
    /// Create a new MappedFile with the specified configuration
    ///
    /// # Arguments
    /// * `file_name` - Path to the file
    /// * `file_size` - Size of the file
    /// * `config` - Configuration for the MappedFile
    ///
    /// # Returns
    /// A boxed MappedFile implementation
    ///
    /// # Example
    /// ```ignore
    /// let config = MappedFileConfig {
    ///     file_type: MappedFileType::IoUring,
    ///     ..Default::default()
    /// };
    ///
    /// let mapped_file = MappedFileFactory::create(
    ///     CheetahString::from("commitlog_0000000000000000000"),
    ///     1024 * 1024 * 1024, // 1GB
    ///     config,
    /// ).await?;
    /// ```
    pub async fn create(
        file_name: CheetahString,
        file_size: u64,
        _config: MappedFileConfig,
    ) -> io::Result<MappedFileImpl> {
        match _config.file_type {
            MappedFileType::Default => {
                let mapped_file = DefaultMappedFile::new(file_name, file_size);
                Ok(MappedFileImpl::Default(mapped_file))
            }

            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            MappedFileType::IoUring => {
                let mapped_file = IoUringMappedFile::new(file_name, file_size).await?;
                Ok(MappedFileImpl::IoUring(mapped_file))
            }
        }
    }

    /// Create a MappedFile with default configuration
    pub async fn create_default(
        file_name: CheetahString,
        file_size: u64,
    ) -> io::Result<MappedFileImpl> {
        Self::create(file_name, file_size, MappedFileConfig::default()).await
    }

    /// Create a MappedFile with transient store pool (currently ignored - future enhancement)
    pub async fn create_with_pool(
        file_name: CheetahString,
        file_size: u64,
        _transient_store_pool: Arc<TransientStorePool>,
    ) -> io::Result<MappedFileImpl> {
        Self::create(
            file_name,
            file_size,
            MappedFileConfig {
                transient_store_pool: Some(_transient_store_pool),
                ..Default::default()
            },
        )
        .await
    }

    /// Get the recommended MappedFile type for the current platform
    pub fn recommended_type() -> MappedFileType {
        MappedFileType::default()
    }

    /// Check if io_uring is available
    pub fn is_io_uring_available() -> bool {
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        {
            // Check kernel version (requires 5.1+)
            if let Ok(uname) = sys_info::os_release() {
                let parts: Vec<&str> = uname.split('.').collect();
                if let Some(major) = parts.first().and_then(|s| s.parse::<i32>().ok()) {
                    if major >= 5 {
                        if let Some(minor) = parts.get(1).and_then(|s| s.parse::<i32>().ok()) {
                            return major > 5 || minor >= 1;
                        }
                    }
                }
            }
            false
        }

        #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
        {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_factory_create_default() {
        let file_name = CheetahString::from("/tmp/test_mapped_file_factory");
        let file_size = 1024 * 1024; // 1MB

        let result = MappedFileFactory::create_default(file_name, file_size).await;
        assert!(
            result.is_ok(),
            "Factory creation failed: {:?}",
            result.err()
        );

        // Cleanup
        let _ = std::fs::remove_file("/tmp/test_mapped_file_factory");
    }

    #[test]
    fn test_recommended_type() {
        let recommended = MappedFileFactory::recommended_type();

        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        {
            assert_eq!(recommended, MappedFileType::IoUring);
        }

        #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
        {
            assert_eq!(recommended, MappedFileType::Default);
        }
    }

    #[test]
    fn test_io_uring_availability_check() {
        let is_available = MappedFileFactory::is_io_uring_available();

        // Should always return false on Windows or without feature
        #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
        {
            assert!(!is_available);
        }
    }
}
*/
