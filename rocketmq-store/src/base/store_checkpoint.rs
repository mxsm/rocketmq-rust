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

use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use memmap2::MmapMut;
use rocketmq_common::UtilAll::ensure_dir_ok;
use tracing::info;

use crate::log_file::mapped_file::default_mapped_file_impl::OS_PAGE_SIZE;

pub struct StoreCheckpoint {
    file: File,
    mmap: parking_lot::Mutex<MmapMut>,
    physic_msg_timestamp: AtomicU64,
    logics_msg_timestamp: AtomicU64,
    index_msg_timestamp: AtomicU64,
    master_flushed_offset: AtomicU64,
    confirm_phy_offset: AtomicU64,
}

impl StoreCheckpoint {
    #[inline]
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        ensure_dir_ok(path.as_ref().parent().unwrap().to_str().unwrap());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref())?;
        let _ = file.set_len(OS_PAGE_SIZE);
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        if file.metadata()?.len() > 0 {
            let buffer = &mmap[..8];
            let physic_msg_timestamp = u64::from_be_bytes(buffer.try_into().unwrap());
            let logics_msg_timestamp = u64::from_be_bytes(mmap[8..16].try_into().unwrap());
            let index_msg_timestamp = u64::from_be_bytes(mmap[16..24].try_into().unwrap());
            let master_flushed_offset = u64::from_be_bytes(mmap[24..32].try_into().unwrap());
            let confirm_phy_offset = u64::from_be_bytes(mmap[32..40].try_into().unwrap());

            info!("store checkpoint file exists, {}", path.as_ref().display());
            info!("physicMsgTimestamp: {}", physic_msg_timestamp);
            info!("logicsMsgTimestamp: {}", logics_msg_timestamp);
            info!("indexMsgTimestamp: {}", index_msg_timestamp);
            info!("masterFlushedOffset: {}", master_flushed_offset);
            info!("confirmPhyOffset: {}", confirm_phy_offset);

            Ok(Self {
                file,
                mmap: parking_lot::Mutex::new(mmap),
                physic_msg_timestamp: AtomicU64::new(physic_msg_timestamp),
                logics_msg_timestamp: AtomicU64::new(logics_msg_timestamp),
                index_msg_timestamp: AtomicU64::new(index_msg_timestamp),
                master_flushed_offset: AtomicU64::new(master_flushed_offset),
                confirm_phy_offset: AtomicU64::new(confirm_phy_offset),
            })
        } else {
            //info!("store checkpoint file not exists, {}", path.as_ref());
            Ok(Self {
                file,
                mmap: parking_lot::Mutex::new(mmap),
                physic_msg_timestamp: AtomicU64::new(0),
                logics_msg_timestamp: AtomicU64::new(0),
                index_msg_timestamp: AtomicU64::new(0),
                master_flushed_offset: AtomicU64::new(0),
                confirm_phy_offset: AtomicU64::new(0),
            })
        }
    }

    #[inline]
    pub fn flush(&self) -> std::io::Result<()> {
        let mut buffer = &mut self.mmap.lock()[..8];
        buffer.write_all(self.physic_msg_timestamp.load(Ordering::Relaxed).to_be_bytes().as_ref())?;
        buffer.write_all(self.logics_msg_timestamp.load(Ordering::Relaxed).to_be_bytes().as_ref())?;
        buffer.write_all(self.index_msg_timestamp.load(Ordering::Relaxed).to_be_bytes().as_ref())?;
        buffer.write_all(
            self.master_flushed_offset
                .load(Ordering::Relaxed)
                .to_be_bytes()
                .as_ref(),
        )?;
        buffer.write_all(self.confirm_phy_offset.load(Ordering::Relaxed).to_be_bytes().as_ref())?;
        self.mmap.lock().flush()?;
        Ok(())
    }

    #[inline]
    pub fn shutdown(&self) -> std::io::Result<()> {
        self.flush()
    }

    #[inline]
    pub fn set_physic_msg_timestamp(&self, physic_msg_timestamp: u64) {
        self.physic_msg_timestamp.store(physic_msg_timestamp, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_logics_msg_timestamp(&self, logics_msg_timestamp: u64) {
        self.logics_msg_timestamp.store(logics_msg_timestamp, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_index_msg_timestamp(&self, index_msg_timestamp: u64) {
        self.index_msg_timestamp.store(index_msg_timestamp, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_master_flushed_offset(&self, master_flushed_offset: u64) {
        self.master_flushed_offset
            .store(master_flushed_offset, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_confirm_phy_offset(&self, confirm_phy_offset: u64) {
        self.confirm_phy_offset.store(confirm_phy_offset, Ordering::Relaxed);
    }

    #[inline]
    pub fn physic_msg_timestamp(&self) -> u64 {
        self.physic_msg_timestamp.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn logics_msg_timestamp(&self) -> u64 {
        self.logics_msg_timestamp.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn index_msg_timestamp(&self) -> u64 {
        self.index_msg_timestamp.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn master_flushed_offset(&self) -> u64 {
        self.master_flushed_offset.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn confirm_phy_offset(&self) -> u64 {
        self.confirm_phy_offset.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn get_min_timestamp(&self) -> u64 {
        let min = self
            .physic_msg_timestamp
            .load(Ordering::Relaxed)
            .min(self.logics_msg_timestamp.load(Ordering::Relaxed)) as i64;
        let min = min - 1000 * 3;
        min.max(0) as u64
    }

    #[inline]
    pub fn get_min_timestamp_index(&self) -> u64 {
        self.get_min_timestamp()
            .min(self.index_msg_timestamp.load(Ordering::Relaxed))
    }
}
