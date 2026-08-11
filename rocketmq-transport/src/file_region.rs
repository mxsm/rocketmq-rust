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

use std::fs::File;
use std::sync::Arc;
#[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
use std::sync::OnceLock;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use serde::Deserialize;
use serde::Serialize;

/// Owns the file descriptor and storage-generation lease behind a [`FileRegion`].
///
/// Storage implementations must keep any generation pin or segment guard in this value so that
/// truncation, overwrite, compaction, and segment reuse cannot affect a live region. The transport
/// retains the lease until writer completion. The blanket [`File`] implementation pins only the
/// descriptor; callers using it remain responsible for external immutability.
pub trait FileRegionLease: Send + Sync + 'static {
    /// Returns the stable file descriptor protected by this lease.
    fn file(&self) -> &File;
}

impl FileRegionLease for File {
    fn file(&self) -> &File {
        self
    }
}

/// Immutable, cloneable file-body lease with a validated byte range.
#[derive(Clone)]
pub struct FileRegion {
    lease: Arc<dyn FileRegionLease>,
    offset: u64,
    len: u64,
    regular_file: bool,
    #[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
    sendfile_supported: Arc<OnceLock<bool>>,
}

/// Ordered external body regions written as one RocketMQ frame.
#[derive(Clone, Debug)]
pub struct FileRegionSequence {
    regions: Vec<FileRegion>,
    len: u64,
}

impl FileRegionSequence {
    /// Validates a non-empty ordered region sequence and its aggregate length.
    ///
    /// # Errors
    ///
    /// Returns a typed argument error when no region is supplied or the aggregate length
    /// overflows `u64`.
    pub fn try_new(regions: Vec<FileRegion>) -> RocketMQResult<Self> {
        if regions.is_empty() {
            return Err(RocketMQError::illegal_argument(
                "file region sequence must contain at least one region",
            ));
        }
        let len = regions.iter().try_fold(0_u64, |total, region| {
            total
                .checked_add(region.len())
                .ok_or_else(|| RocketMQError::illegal_argument("file region sequence length overflowed u64"))
        })?;
        Ok(Self { regions, len })
    }

    pub(crate) fn single(region: FileRegion) -> Self {
        Self {
            len: region.len(),
            regions: vec![region],
        }
    }

    #[inline]
    pub const fn len(&self) -> u64 {
        self.len
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub(crate) fn regions(&self) -> &[FileRegion] {
        &self.regions
    }
}

impl std::fmt::Debug for FileRegion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileRegion")
            .field("offset", &self.offset)
            .field("len", &self.len)
            .field("regular_file", &self.regular_file)
            .field("sendfile_supported", &{
                #[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
                {
                    self.sendfile_supported.get().copied()
                }
                #[cfg(not(all(target_os = "linux", feature = "linux-sendfile")))]
                {
                    Option::<bool>::None
                }
            })
            .finish_non_exhaustive()
    }
}

impl FileRegion {
    /// Creates a region after validating its range against the leased file length.
    ///
    /// The range must remain immutable and readable until every clone has been released. A storage
    /// engine should satisfy that invariant with a custom [`FileRegionLease`] implementation.
    ///
    /// # Errors
    ///
    /// Returns a typed error for a zero length, arithmetic overflow, a range beyond the current
    /// file length, or a metadata failure.
    pub fn try_new(lease: Arc<dyn FileRegionLease>, offset: u64, len: u64) -> RocketMQResult<Self> {
        if len == 0 {
            return Err(RocketMQError::illegal_argument(
                "file region length must be greater than zero",
            ));
        }
        let end = offset
            .checked_add(len)
            .ok_or_else(|| RocketMQError::illegal_argument("file region offset plus length overflowed u64"))?;
        let metadata = lease
            .file()
            .metadata()
            .map_err(|error| RocketMQError::network_connection_failed("file-region-metadata", error.to_string()))?;
        if end > metadata.len() {
            return Err(RocketMQError::illegal_argument(format!(
                "file region end {end} exceeds leased file length {}",
                metadata.len()
            )));
        }
        Ok(Self {
            lease,
            offset,
            len,
            regular_file: metadata.is_file(),
            #[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
            sendfile_supported: Arc::new(OnceLock::new()),
        })
    }

    /// Returns the inclusive starting byte offset.
    #[inline]
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the number of bytes in this region.
    #[inline]
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Returns whether the region has zero bytes.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn lease(&self) -> &Arc<dyn FileRegionLease> {
        &self.lease
    }

    #[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
    pub(crate) const fn is_regular_file(&self) -> bool {
        self.regular_file
    }

    #[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
    pub(crate) fn cached_sendfile_support(&self) -> Option<bool> {
        self.sendfile_supported.get().copied()
    }

    #[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
    pub(crate) fn cache_sendfile_support(&self, supported: bool) {
        let _ = self.sendfile_supported.set(supported);
    }
}

/// Runtime strategy for sending an external file body.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FileTransferMode {
    /// Use Linux `sendfile` for eligible plaintext TCP connections and portable I/O otherwise.
    #[default]
    Auto,
    /// Always read bounded chunks and pass them through the negotiated `AsyncWrite` path.
    Portable,
    /// Require Linux `sendfile`; unsupported transports or builds return an error before the head.
    Sendfile,
}
