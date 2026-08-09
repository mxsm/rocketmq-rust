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

use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

const ROLE_RECORD_MAGIC: u32 = 0x5449_4D52;
const ROLE_RECORD_VERSION: u32 = 1;
const ROLE_RECORD_SIZE: usize = 24;
const ROLE_FILE_SIZE: u64 = (ROLE_RECORD_SIZE * 2) as u64;
const CHECKSUM_SALT: u64 = 0xA96E_54D3_C271_8B0F;

/// Durable delivery lease used to fence timer batches across Broker role changes.
pub(crate) struct TimerRoleState {
    path: PathBuf,
    epoch: AtomicU64,
    active: AtomicBool,
}

impl TimerRoleState {
    pub(crate) fn new(store_root: &str) -> Self {
        Self {
            path: Path::new(store_root).join("config").join("timer-role.meta"),
            epoch: AtomicU64::new(0),
            active: AtomicBool::new(false),
        }
    }

    pub(crate) fn load(&self) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&self.path)?;
        if file.metadata()?.len() < ROLE_FILE_SIZE {
            file.set_len(ROLE_FILE_SIZE)?;
            file.sync_data()?;
        }

        let mut buffer = [0u8; ROLE_RECORD_SIZE * 2];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut buffer)?;
        let recovered_epoch = buffer
            .chunks_exact(ROLE_RECORD_SIZE)
            .filter_map(decode_record)
            .max()
            .unwrap_or_default();
        self.epoch.store(recovered_epoch, Ordering::Release);
        self.active.store(false, Ordering::Release);
        Ok(())
    }

    pub(crate) fn transition(&self, active: bool) -> std::io::Result<u64> {
        if self.active.load(Ordering::Acquire) == active {
            return Ok(self.epoch.load(Ordering::Acquire));
        }

        // Closing the lease is always the first downgrade action. A failed durable epoch write
        // therefore fails closed and still fences every in-flight batch in this process.
        self.active.store(false, Ordering::Release);
        let next_epoch = self
            .epoch
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("timer role epoch is exhausted"))?;
        self.persist_epoch(next_epoch)?;
        self.epoch.store(next_epoch, Ordering::Release);
        self.active.store(active, Ordering::Release);
        Ok(next_epoch)
    }

    pub(crate) fn capture_delivery_epoch(&self) -> Option<u64> {
        self.active
            .load(Ordering::Acquire)
            .then(|| self.epoch.load(Ordering::Acquire))
    }

    pub(crate) fn is_current_delivery_epoch(&self, epoch: u64) -> bool {
        self.active.load(Ordering::Acquire) && self.epoch.load(Ordering::Acquire) == epoch
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    fn persist_epoch(&self, epoch: u64) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&self.path)?;
        if file.metadata()?.len() < ROLE_FILE_SIZE {
            file.set_len(ROLE_FILE_SIZE)?;
        }
        let slot = (epoch as usize) % 2;
        file.seek(SeekFrom::Start((slot * ROLE_RECORD_SIZE) as u64))?;
        file.write_all(&encode_record(epoch))?;
        file.sync_data()
    }
}

fn encode_record(epoch: u64) -> [u8; ROLE_RECORD_SIZE] {
    let mut record = [0u8; ROLE_RECORD_SIZE];
    record[0..4].copy_from_slice(&ROLE_RECORD_MAGIC.to_be_bytes());
    record[4..8].copy_from_slice(&ROLE_RECORD_VERSION.to_be_bytes());
    record[8..16].copy_from_slice(&epoch.to_be_bytes());
    record[16..24].copy_from_slice(&record_checksum(epoch).to_be_bytes());
    record
}

fn decode_record(record: &[u8]) -> Option<u64> {
    let magic = u32::from_be_bytes(record.get(0..4)?.try_into().ok()?);
    let version = u32::from_be_bytes(record.get(4..8)?.try_into().ok()?);
    let epoch = u64::from_be_bytes(record.get(8..16)?.try_into().ok()?);
    let checksum = u64::from_be_bytes(record.get(16..24)?.try_into().ok()?);
    (magic == ROLE_RECORD_MAGIC && version == ROLE_RECORD_VERSION && checksum == record_checksum(epoch))
        .then_some(epoch)
}

fn record_checksum(epoch: u64) -> u64 {
    epoch ^ ((ROLE_RECORD_MAGIC as u64) << 32 | ROLE_RECORD_VERSION as u64) ^ CHECKSUM_SALT
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn role_epoch_is_durable_monotonic_and_fail_closed_on_restart() {
        let directory = tempdir().unwrap();
        let root = directory.path().to_string_lossy();
        let role = TimerRoleState::new(&root);
        role.load().unwrap();
        assert_eq!(role.transition(true).unwrap(), 1);
        assert_eq!(role.capture_delivery_epoch(), Some(1));
        assert_eq!(role.transition(false).unwrap(), 2);
        assert!(!role.is_active());

        let recovered = TimerRoleState::new(&root);
        recovered.load().unwrap();
        assert_eq!(recovered.epoch(), 2);
        assert!(!recovered.is_active());
        assert_eq!(recovered.transition(true).unwrap(), 3);
    }

    #[test]
    fn torn_newest_role_record_recovers_previous_epoch() {
        let directory = tempdir().unwrap();
        let root = directory.path().to_string_lossy();
        let role = TimerRoleState::new(&root);
        role.load().unwrap();
        role.transition(true).unwrap();
        role.transition(false).unwrap();

        let mut file = OpenOptions::new().write(true).open(&role.path).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0u8; ROLE_RECORD_SIZE]).unwrap();
        file.sync_data().unwrap();

        let recovered = TimerRoleState::new(&root);
        recovered.load().unwrap();
        assert_eq!(recovered.epoch(), 1);
    }
}
