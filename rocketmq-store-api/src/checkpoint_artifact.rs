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

//! Backend-neutral checkpoint artifact utilities.

use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use sha2::Digest;
use sha2::Sha256;
use thiserror::Error;

/// Manifest stored beside a checkpoint payload.
pub const RELEASE_CHECKPOINT_MANIFEST_FILE: &str = ".rocketmq-release-checkpoint.json";

/// Deterministic digest and payload length for one checkpoint directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointDirectoryDigest {
    /// Number of payload bytes covered by the digest.
    pub length_bytes: u64,
    /// Lowercase SHA-256 over stable path and file content ordering.
    pub sha256: String,
}

use crate::StoreContractViolation;
use crate::StoreError;
use crate::StoreErrorKind;
use crate::StoreOperation;

/// Private filesystem failure while reading a checkpoint artifact.
#[derive(Debug, Error)]
#[error("{operation} failed for {path}: {source}")]
struct CheckpointArtifactIoError {
    operation: &'static str,
    path: PathBuf,
    #[source]
    source: std::io::Error,
}

/// Hashes all regular files in stable relative-path order.
///
/// The embedded manifest is excluded because it contains the digest itself.
/// Symlinks and special files are rejected, and the byte budget is enforced
/// while reading.
///
/// # Errors
///
/// Returns [`StoreError`] when the artifact cannot be traversed safely, read,
/// or kept within `max_checkpoint_bytes`. Deterministic shape violations are
/// preserved as a typed [`StoreContractViolation`] source; filesystem failures
/// retain their private typed source and underlying [`std::io::Error`].
pub fn hash_checkpoint_directory(
    checkpoint_root: &Path,
    max_checkpoint_bytes: u64,
) -> Result<CheckpointDirectoryDigest, StoreError> {
    let checkpoint_root = checkpoint_root
        .canonicalize()
        .map_err(|source| io_error("canonicalize checkpoint", checkpoint_root, source))?;
    let files = collect_regular_files(&checkpoint_root)?;
    let mut hasher = Sha256::new();
    let mut length_bytes = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];

    for (relative, path) in files {
        if relative == Path::new(RELEASE_CHECKPOINT_MANIFEST_FILE) {
            continue;
        }
        hash_relative_path(&mut hasher, &relative);
        let mut input =
            BufReader::new(File::open(&path).map_err(|source| io_error("open checkpoint file", &path, source))?);
        loop {
            let read = input
                .read(&mut buffer)
                .map_err(|source| io_error("read checkpoint file", &path, source))?;
            if read == 0 {
                break;
            }
            length_bytes = length_bytes.checked_add(read as u64).ok_or_else(|| {
                artifact_contract(StoreContractViolation::CheckpointArtifactTooLarge {
                    actual: u64::MAX,
                    maximum: max_checkpoint_bytes,
                })
            })?;
            if length_bytes > max_checkpoint_bytes {
                return Err(artifact_contract(StoreContractViolation::CheckpointArtifactTooLarge {
                    actual: length_bytes,
                    maximum: max_checkpoint_bytes,
                }));
            }
            hasher.update(&buffer[..read]);
        }
    }

    if length_bytes == 0 {
        return Err(artifact_contract(StoreContractViolation::CheckpointArtifactEmpty));
    }
    Ok(CheckpointDirectoryDigest {
        length_bytes,
        sha256: hex::encode(hasher.finalize()),
    })
}

/// Converts a local path to the file URI emitted in checkpoint manifests.
pub fn path_to_file_uri(path: &Path) -> String {
    let raw = path.to_string_lossy();
    #[cfg(windows)]
    let raw = raw.strip_prefix(r"\\?\").map_or_else(|| raw.to_string(), str::to_owned);
    #[cfg(not(windows))]
    let raw = raw.into_owned();
    let portable = raw.replace('\\', "/");
    if portable.starts_with('/') {
        format!("file://{portable}")
    } else {
        format!("file:///{portable}")
    }
}

/// Resolves a checkpoint URI created by [`path_to_file_uri`].
///
/// # Errors
///
/// Returns [`StoreContractViolation::CheckpointArtifactUnsupportedUri`] when
/// `uri` is not a local file URI.
pub fn file_uri_to_path(uri: &str) -> Result<PathBuf, StoreContractViolation> {
    let raw = uri
        .strip_prefix("file:///")
        .ok_or_else(|| StoreContractViolation::CheckpointArtifactUnsupportedUri(uri.to_string()))?;
    #[cfg(windows)]
    let path = PathBuf::from(raw.replace('/', "\\"));
    #[cfg(not(windows))]
    let path = PathBuf::from(format!("/{raw}"));
    Ok(path)
}

fn collect_regular_files(root: &Path) -> Result<Vec<(PathBuf, PathBuf)>, StoreError> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        let entries =
            fs::read_dir(&directory).map_err(|source| io_error("read checkpoint directory", &directory, source))?;
        for entry in entries {
            let entry = entry.map_err(|source| io_error("read checkpoint entry", &directory, source))?;
            let path = entry.path();
            let metadata =
                fs::symlink_metadata(&path).map_err(|source| io_error("inspect checkpoint entry", &path, source))?;
            if metadata.file_type().is_symlink() {
                return Err(artifact_contract(
                    StoreContractViolation::CheckpointArtifactSymbolicLink(path),
                ));
            }
            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file() {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|_| {
                        artifact_contract(StoreContractViolation::CheckpointArtifactPathEscaped(path.clone()))
                    })?
                    .to_path_buf();
                files.push((relative, path));
            } else {
                return Err(artifact_contract(
                    StoreContractViolation::CheckpointArtifactUnsupportedFileType(path),
                ));
            }
        }
    }
    files.sort_by_cached_key(|entry| portable_relative_path(&entry.0));
    Ok(files)
}

fn hash_relative_path(hasher: &mut Sha256, relative: &Path) {
    let portable = portable_relative_path(relative);
    hasher.update((portable.len() as u64).to_le_bytes());
    hasher.update(portable.as_bytes());
}

fn portable_relative_path(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn artifact_contract(source: StoreContractViolation) -> StoreError {
    StoreError::new(StoreErrorKind::InvalidRequest, StoreOperation::Read).with_source(source)
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> StoreError {
    StoreError::new(StoreErrorKind::Io, StoreOperation::Read).with_source(CheckpointArtifactIoError {
        operation,
        path: path.to_path_buf(),
        source,
    })
}
