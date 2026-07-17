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

use std::path::Path;
use std::path::PathBuf;

use rocketmq_error::RocketMQError;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::progress::TieredRetryEntry;

const PROGRESS_MAGIC: [u8; 8] = *b"RMQTPRG\0";
const PROGRESS_VERSION: u16 = 1;
const HEADER_LEN: usize = 20;
const CHECKSUM_LEN: usize = 4;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub(crate) struct PersistedTieredProgress {
    pub(crate) checkpoint: Vec<u8>,
    pub(crate) retries: Vec<TieredRetryEntry>,
}

pub(crate) struct TieredProgressPersistence {
    path: PathBuf,
    persist_lock: tokio::sync::Mutex<()>,
}

impl TieredProgressPersistence {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            path: root.join("config").join("tieredDispatchProgress.bin"),
            persist_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub(crate) async fn load(&self) -> Result<Option<PersistedTieredProgress>, RocketMQError> {
        let encoded = match fs::read(&self.path).await {
            Ok(encoded) => encoded,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(read_failed(&self.path, error)),
        };
        decode(&encoded, &self.path).map(Some)
    }

    pub(crate) async fn persist(&self, progress: &PersistedTieredProgress) -> Result<(), RocketMQError> {
        let _guard = self.persist_lock.lock().await;
        let parent = self
            .path
            .parent()
            .ok_or_else(|| RocketMQError::storage_write_failed(path_string(&self.path), "missing parent directory"))?;
        fs::create_dir_all(parent)
            .await
            .map_err(|error| write_failed(parent, error))?;
        let encoded = encode(progress)?;
        let temporary = self.path.with_extension("bin.tmp");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(|error| write_failed(&temporary, error))?;
        file.write_all(&encoded)
            .await
            .map_err(|error| write_failed(&temporary, error))?;
        file.sync_all().await.map_err(|error| write_failed(&temporary, error))?;
        drop(file);
        fs::rename(&temporary, &self.path)
            .await
            .map_err(|error| write_failed(&self.path, error))?;
        sync_parent_directory(parent).await?;
        Ok(())
    }

    pub(crate) async fn destroy(&self) -> Result<(), RocketMQError> {
        match fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(write_failed(&self.path, error)),
        }
    }
}

fn encode(progress: &PersistedTieredProgress) -> Result<Vec<u8>, RocketMQError> {
    let payload = serde_json::to_vec(progress)
        .map_err(|error| RocketMQError::storage_write_failed("tiered progress", error.to_string()))?;
    let payload_len = u64::try_from(payload.len())
        .map_err(|_| RocketMQError::storage_write_failed("tiered progress", "payload length overflow"))?;
    let mut encoded = Vec::with_capacity(HEADER_LEN + payload.len() + CHECKSUM_LEN);
    encoded.extend_from_slice(&PROGRESS_MAGIC);
    encoded.extend_from_slice(&PROGRESS_VERSION.to_be_bytes());
    encoded.extend_from_slice(&0_u16.to_be_bytes());
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(&payload);
    encoded.extend_from_slice(&crc32(&encoded).to_be_bytes());
    Ok(encoded)
}

fn decode(encoded: &[u8], path: &Path) -> Result<PersistedTieredProgress, RocketMQError> {
    if encoded.len() < HEADER_LEN + CHECKSUM_LEN || encoded[..8] != PROGRESS_MAGIC {
        return Err(corrupted(path));
    }
    let version = u16::from_be_bytes([encoded[8], encoded[9]]);
    let reserved = u16::from_be_bytes([encoded[10], encoded[11]]);
    if version != PROGRESS_VERSION || reserved != 0 {
        return Err(corrupted(path));
    }
    let payload_len = u64::from_be_bytes([
        encoded[12],
        encoded[13],
        encoded[14],
        encoded[15],
        encoded[16],
        encoded[17],
        encoded[18],
        encoded[19],
    ]);
    let payload_len = usize::try_from(payload_len).map_err(|_| corrupted(path))?;
    let expected_len = HEADER_LEN
        .checked_add(payload_len)
        .and_then(|length| length.checked_add(CHECKSUM_LEN))
        .ok_or_else(|| corrupted(path))?;
    if encoded.len() != expected_len {
        return Err(corrupted(path));
    }
    let checksum_offset = expected_len - CHECKSUM_LEN;
    let expected_checksum = u32::from_be_bytes([
        encoded[checksum_offset],
        encoded[checksum_offset + 1],
        encoded[checksum_offset + 2],
        encoded[checksum_offset + 3],
    ]);
    if crc32(&encoded[..checksum_offset]) != expected_checksum {
        return Err(corrupted(path));
    }
    serde_json::from_slice(&encoded[HEADER_LEN..checksum_offset]).map_err(|_| corrupted(path))
}

#[cfg(unix)]
async fn sync_parent_directory(parent: &Path) -> Result<(), RocketMQError> {
    fs::File::open(parent)
        .await
        .map_err(|error| write_failed(parent, error))?
        .sync_all()
        .await
        .map_err(|error| write_failed(parent, error))
}

#[cfg(not(unix))]
async fn sync_parent_directory(_parent: &Path) -> Result<(), RocketMQError> {
    // The temporary file is synced before the atomic replacement. Windows does not expose a
    // portable async directory handle through Tokio; rename durability follows the volume's
    // metadata guarantees.
    Ok(())
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

fn corrupted(path: &Path) -> RocketMQError {
    crate::error::storage_corrupted(path_string(path))
}

fn read_failed(path: &Path, error: std::io::Error) -> RocketMQError {
    RocketMQError::storage_read_failed(path_string(path), error.to_string())
}

fn write_failed(path: &Path, error: std::io::Error) -> RocketMQError {
    RocketMQError::storage_write_failed(path_string(path), error.to_string())
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
