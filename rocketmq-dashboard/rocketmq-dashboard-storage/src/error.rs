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

use std::{error::Error, fmt, io, path::PathBuf};

use crate::StorageEngine;

/// Safe, actionable classification. No variant contains a connection string or payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageErrorKind {
    Io,
    InvalidDocument,
    Validation,
    UnsupportedSchema { found: u32, supported: u32 },
    DriverUnavailable { engine: StorageEngine },
    Connection,
    Query,
    Conflict,
    WriterUnavailable,
    Runtime,
    EmptySource,
    AlreadyInitialized,
    Uninitialized,
    RevisionExhausted,
    LeaseLost,
    Closed,
}

/// Recoverable storage error whose formatting and source chain never reveal credentials.
///
/// Driver errors can contain URLs, SQL parameters and server-controlled text, so they are
/// deliberately classified at the persistence boundary instead of exposed as an error source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageError {
    kind: StorageErrorKind,
    io_operation: Option<StorageIoOperation>,
}

/// Safe evidence of the filesystem phase that failed, without paths or raw I/O errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageIoOperation {
    Read,
    Write,
}

impl StorageError {
    pub(crate) const fn new(kind: StorageErrorKind) -> Self {
        Self {
            kind,
            io_operation: None,
        }
    }

    pub const fn kind(&self) -> StorageErrorKind {
        self.kind
    }

    pub const fn io_operation(&self) -> Option<StorageIoOperation> {
        self.io_operation
    }

    pub(crate) fn validation(_source: impl Error + Send + Sync + 'static) -> Self {
        Self::new(StorageErrorKind::Validation)
    }

    pub(crate) fn io(operation: &'static str, _path: PathBuf, _source: io::Error) -> Self {
        Self {
            kind: StorageErrorKind::Io,
            io_operation: Some(if operation.starts_with("read") {
                StorageIoOperation::Read
            } else {
                StorageIoOperation::Write
            }),
        }
    }

    pub(crate) fn invalid_document(_path: PathBuf, _source: serde_json::Error) -> Self {
        Self::new(StorageErrorKind::InvalidDocument)
    }

    pub(crate) const fn unsupported_schema(found: u32, supported: u32) -> Self {
        Self::new(StorageErrorKind::UnsupportedSchema { found, supported })
    }

    pub(crate) const fn conflict() -> Self {
        Self::new(StorageErrorKind::Conflict)
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            StorageErrorKind::Io => "Dashboard storage could not be read or written.",
            StorageErrorKind::InvalidDocument => {
                "Dashboard storage contains an invalid document; the original is preserved."
            }
            StorageErrorKind::Validation => "Dashboard storage input failed validation.",
            StorageErrorKind::UnsupportedSchema { .. } => {
                "Dashboard storage uses an unsupported schema; the original is preserved."
            }
            StorageErrorKind::DriverUnavailable { .. } => {
                "The selected Dashboard storage engine was not compiled into this application."
            }
            StorageErrorKind::Connection => "The selected Dashboard database could not be connected.",
            StorageErrorKind::Query => "The selected Dashboard database could not complete the operation.",
            StorageErrorKind::Conflict => "Dashboard storage changed in another operation; reload authoritative state.",
            StorageErrorKind::WriterUnavailable => "Another Dashboard instance owns this file data directory.",
            StorageErrorKind::Runtime => "The owned Dashboard storage runtime is unavailable.",
            StorageErrorKind::EmptySource => "The import source has no native Dashboard configuration.",
            StorageErrorKind::AlreadyInitialized => "The import destination already contains native Dashboard data.",
            StorageErrorKind::Uninitialized => {
                "The selected database has no native Dashboard data; initialize it or import explicitly."
            }
            StorageErrorKind::RevisionExhausted => "Dashboard storage revisions are exhausted.",
            StorageErrorKind::LeaseLost => {
                "History collection ownership expired or changed; the sample was not committed."
            }
            StorageErrorKind::Closed => "Dashboard storage is closed.",
        })
    }
}

impl Error for StorageError {}
