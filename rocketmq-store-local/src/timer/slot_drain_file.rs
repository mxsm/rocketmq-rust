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
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use parking_lot::Mutex;

use crate::timer::storage_format::crc32c;

const HEADER_MAGIC: u32 = 0x5444_4832;
const RECORD_MAGIC: u32 = 0x5444_5232;
const VERSION: u16 = 2;
const HEADER_SIZE: usize = 64;
const RECORD_SIZE: usize = 56;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotDrainLocator {
    pub timer_log_position: i64,
    pub commit_log_offset: i64,
    pub size: i32,
    pub magic: i32,
    pub queue_offset: i64,
    pub generation: u64,
}

impl SlotDrainLocator {
    fn encode(self) -> [u8; RECORD_SIZE] {
        let mut bytes = [0u8; RECORD_SIZE];
        bytes[0..4].copy_from_slice(&RECORD_MAGIC.to_be_bytes());
        bytes[4..6].copy_from_slice(&VERSION.to_be_bytes());
        bytes[6..8].copy_from_slice(&(RECORD_SIZE as u16).to_be_bytes());
        bytes[8..16].copy_from_slice(&self.timer_log_position.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.commit_log_offset.to_be_bytes());
        bytes[24..28].copy_from_slice(&self.size.to_be_bytes());
        bytes[28..32].copy_from_slice(&self.magic.to_be_bytes());
        bytes[32..40].copy_from_slice(&self.queue_offset.to_be_bytes());
        bytes[40..48].copy_from_slice(&self.generation.to_be_bytes());
        let checksum = crc32c(&bytes[..52]);
        bytes[52..56].copy_from_slice(&checksum.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> std::io::Result<Self> {
        if bytes.len() != RECORD_SIZE
            || read_u32(bytes, 0) != RECORD_MAGIC
            || read_u16(bytes, 4) != VERSION
            || read_u16(bytes, 6) as usize != RECORD_SIZE
            || crc32c(&bytes[..52]) != read_u32(bytes, 52)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "slot drain locator is invalid",
            ));
        }
        Ok(Self {
            timer_log_position: read_i64(bytes, 8),
            commit_log_offset: read_i64(bytes, 16),
            size: read_i32(bytes, 24),
            magic: read_i32(bytes, 28),
            queue_offset: read_i64(bytes, 32),
            generation: read_u64(bytes, 40),
        })
    }
}

pub struct SlotDrainFileBuilder {
    temporary_path: PathBuf,
    final_path: PathBuf,
    file: File,
    slot_time_ms: i64,
    generation: u64,
    record_count: u64,
}

impl SlotDrainFileBuilder {
    pub fn create(path: impl AsRef<Path>, slot_time_ms: i64, generation: u64) -> std::io::Result<Self> {
        let final_path = path.as_ref().to_path_buf();
        if let Some(parent) = final_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temporary_path = final_path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&temporary_path)?;
        file.write_all(&[0u8; HEADER_SIZE])?;
        Ok(Self {
            temporary_path,
            final_path,
            file,
            slot_time_ms,
            generation,
            record_count: 0,
        })
    }

    /// Appends locators while the TimerLog chain is traversed newest-to-oldest.
    pub fn push_reverse(&mut self, locator: SlotDrainLocator) -> std::io::Result<()> {
        self.file.write_all(&locator.encode())?;
        self.record_count = self
            .record_count
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("slot drain locator count overflow"))?;
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<SlotDrainFile> {
        let header = encode_header(self.slot_time_ms, self.generation, self.record_count);
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header)?;
        self.file.sync_data()?;
        drop(self.file);
        if self.final_path.exists() {
            std::fs::remove_file(&self.final_path)?;
        }
        std::fs::rename(&self.temporary_path, &self.final_path)?;
        SlotDrainFile::open(self.final_path)
    }
}

pub struct SlotDrainFile {
    path: PathBuf,
    file: Mutex<File>,
    slot_time_ms: i64,
    generation: u64,
    record_count: u64,
}

impl SlotDrainFile {
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new().read(true).open(&path)?;
        let mut header = [0u8; HEADER_SIZE];
        file.read_exact(&mut header)?;
        let (slot_time_ms, generation, record_count) = decode_header(&header)?;
        let expected_len = HEADER_SIZE as u64
            + record_count
                .checked_mul(RECORD_SIZE as u64)
                .ok_or_else(|| std::io::Error::other("slot drain file length overflow"))?;
        if file.metadata()?.len() != expected_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "slot drain file length does not match its manifest",
            ));
        }
        Ok(Self {
            path,
            file: Mutex::new(file),
            slot_time_ms,
            generation,
            record_count,
        })
    }

    pub fn read_batch(&self, cursor: usize, max_records: usize) -> std::io::Result<Vec<SlotDrainLocator>> {
        if cursor >= self.record_count as usize || max_records == 0 {
            return Ok(Vec::new());
        }
        let count = max_records.min(self.record_count as usize - cursor);
        let mut file = self.file.lock();
        let first_physical_index = self.record_count as usize - cursor - count;
        file.seek(SeekFrom::Start(
            (HEADER_SIZE + first_physical_index * RECORD_SIZE) as u64,
        ))?;
        let mut bytes = vec![0u8; count * RECORD_SIZE];
        file.read_exact(&mut bytes)?;
        let mut locators = Vec::with_capacity(count);
        for record in bytes.chunks_exact(RECORD_SIZE).rev() {
            locators.push(SlotDrainLocator::decode(record)?);
        }
        Ok(locators)
    }

    pub fn record_count(&self) -> usize {
        self.record_count as usize
    }

    pub fn slot_time_ms(&self) -> i64 {
        self.slot_time_ms
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn physical_bytes(&self) -> u64 {
        HEADER_SIZE as u64 + self.record_count * RECORD_SIZE as u64
    }

    pub fn remove(self) -> std::io::Result<()> {
        drop(self.file);
        if self.path.exists() {
            std::fs::remove_file(self.path)?;
        }
        Ok(())
    }
}

fn encode_header(slot_time_ms: i64, generation: u64, record_count: u64) -> [u8; HEADER_SIZE] {
    let mut bytes = [0u8; HEADER_SIZE];
    bytes[0..4].copy_from_slice(&HEADER_MAGIC.to_be_bytes());
    bytes[4..6].copy_from_slice(&VERSION.to_be_bytes());
    bytes[6..8].copy_from_slice(&(HEADER_SIZE as u16).to_be_bytes());
    bytes[8..16].copy_from_slice(&slot_time_ms.to_be_bytes());
    bytes[16..24].copy_from_slice(&generation.to_be_bytes());
    bytes[24..32].copy_from_slice(&record_count.to_be_bytes());
    bytes[32..36].copy_from_slice(&1u32.to_be_bytes());
    let checksum = crc32c(&bytes[..60]);
    bytes[60..64].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

fn decode_header(bytes: &[u8]) -> std::io::Result<(i64, u64, u64)> {
    if bytes.len() != HEADER_SIZE
        || read_u32(bytes, 0) != HEADER_MAGIC
        || read_u16(bytes, 4) != VERSION
        || read_u16(bytes, 6) as usize != HEADER_SIZE
        || read_u32(bytes, 32) != 1
        || crc32c(&bytes[..60]) != read_u32(bytes, 60)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "slot drain header is invalid or incomplete",
        ));
    }
    Ok((read_i64(bytes, 8), read_u64(bytes, 16), read_u64(bytes, 24)))
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(bytes[offset..offset + 2].try_into().expect("fixed u16 field"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("fixed u32 field"))
}

fn read_i32(bytes: &[u8], offset: usize) -> i32 {
    i32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("fixed i32 field"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("fixed u64 field"))
}

fn read_i64(bytes: &[u8], offset: usize) -> i64 {
    i64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("fixed i64 field"))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn reverse_builder_reads_locators_in_delivery_order() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("drain");
        let mut builder = SlotDrainFileBuilder::create(&path, 1_000, 7).unwrap();
        for position in [120, 80, 40, 0] {
            builder
                .push_reverse(SlotDrainLocator {
                    timer_log_position: position,
                    commit_log_offset: position * 10,
                    size: 64,
                    magic: 1,
                    queue_offset: position / 40,
                    generation: 7,
                })
                .unwrap();
        }
        let drain = builder.finish().unwrap();
        assert_eq!(
            drain
                .read_batch(1, 2)
                .unwrap()
                .iter()
                .map(|locator| locator.timer_log_position)
                .collect::<Vec<_>>(),
            vec![40, 80]
        );
        assert_eq!(drain.record_count(), 4);
    }
}
