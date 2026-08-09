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

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use thiserror::Error;

use crate::timer::metrics::TimerStorageMetrics;
use crate::timer::slot::Slot;
use crate::timer::storage_format::crc32c;

const PAGE_MAGIC: u32 = 0x5457_5032;
const PAGE_VERSION: u16 = 2;
const PAGE_HEADER_SIZE: usize = 32;
const PAGES_A: &str = "pages.a";
const PAGES_B: &str = "pages.b";
const GENERATION_JOURNAL: &str = "generations";
const FORMAT_MARKER: &str = "FORMAT";
const GENERATION_MAGIC: u32 = 0x5457_4732;
const GENERATION_VERSION: u16 = 2;
const GENERATION_HEADER_SIZE: usize = 24;
const GENERATION_TRAILER_SIZE: usize = 4;

pub struct PagedTimerWheel {
    directory: PathBuf,
    slots_total: usize,
    slots_per_page: usize,
    page_size: usize,
    page_count: usize,
    metrics: Arc<TimerStorageMetrics>,
    state: Mutex<PagedTimerWheelState>,
}

struct PagedTimerWheelState {
    slots: Vec<Slot>,
    dirty_pages: HashSet<usize>,
    page_versions: Vec<u64>,
    non_empty_pages: Vec<bool>,
    committed_generation: u64,
    max_generation: u64,
}

impl PagedTimerWheel {
    pub fn new(
        directory: impl AsRef<Path>,
        slots_total: usize,
        page_size: usize,
        metrics: Arc<TimerStorageMetrics>,
    ) -> Result<Self, PagedTimerWheelError> {
        if page_size <= PAGE_HEADER_SIZE || !(page_size - PAGE_HEADER_SIZE).is_multiple_of(Slot::SIZE as usize) {
            return Err(PagedTimerWheelError::InvalidPageSize(page_size));
        }
        let slots_per_page = (page_size - PAGE_HEADER_SIZE) / Slot::SIZE as usize;
        let page_count = slots_total.div_ceil(slots_per_page);
        Ok(Self {
            directory: directory.as_ref().to_path_buf(),
            slots_total,
            slots_per_page,
            page_size,
            page_count,
            metrics,
            state: Mutex::new(PagedTimerWheelState {
                slots: vec![empty_slot(); slots_total],
                dirty_pages: HashSet::new(),
                page_versions: vec![0; page_count],
                non_empty_pages: vec![false; page_count],
                committed_generation: 0,
                max_generation: 0,
            }),
        })
    }

    pub fn load(&self, committed_generation: u64) -> Result<(), PagedTimerWheelError> {
        std::fs::create_dir_all(&self.directory)?;
        if !self.directory.join(FORMAT_MARKER).exists() {
            self.initialize_files()?;
        }
        let expected_generations = self.load_expected_generations(committed_generation)?;
        let mut pages_a = OpenOptions::new().read(true).open(self.directory.join(PAGES_A))?;
        let mut pages_b = OpenOptions::new().read(true).open(self.directory.join(PAGES_B))?;
        let mut state = self.state.lock();
        state.committed_generation = 0;
        state.max_generation = 0;
        for (page_id, expected_generation) in expected_generations.iter().copied().enumerate() {
            let source = if expected_generation.is_multiple_of(2) {
                &mut pages_a
            } else {
                &mut pages_b
            };
            let selected = self.read_page(source, page_id, expected_generation)?;
            let range = self.page_slot_range(page_id);
            state.slots[range.clone()].copy_from_slice(&selected.slots[..range.len()]);
            state.page_versions[page_id] = selected.generation;
            state.non_empty_pages[page_id] = selected.non_empty;
            state.max_generation = state.max_generation.max(selected.generation);
        }
        state.committed_generation = if committed_generation == u64::MAX {
            state.max_generation
        } else {
            committed_generation
        };
        state.dirty_pages.clear();
        self.metrics.set_dirty_pages(0);
        Ok(())
    }

    pub fn import_legacy_slots(&self, slots: &[Slot]) -> Result<(), PagedTimerWheelError> {
        if slots.len() != self.slots_total {
            return Err(PagedTimerWheelError::InvalidSlotCount {
                actual: slots.len(),
                expected: self.slots_total,
            });
        }
        let mut state = self.state.lock();
        state.slots.copy_from_slice(slots);
        for page_id in 0..self.page_count {
            state.dirty_pages.insert(page_id);
            state.page_versions[page_id] = state.page_versions[page_id].saturating_add(1);
            let range = self.page_slot_range(page_id);
            state.non_empty_pages[page_id] = state.slots[range].iter().any(|slot| slot.num > 0);
        }
        self.metrics.set_dirty_pages(self.page_count as u64);
        Ok(())
    }

    pub fn reset_for_repair(&self, committed_generation: u64) -> Result<(), PagedTimerWheelError> {
        std::fs::create_dir_all(&self.directory)?;
        if !self.directory.join(FORMAT_MARKER).exists() {
            self.initialize_files()?;
        }
        let expected_len = (self.page_count * self.page_size) as u64;
        for file_name in [PAGES_A, PAGES_B] {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.directory.join(file_name))?
                .set_len(expected_len)?;
        }
        let mut state = self.state.lock();
        state.slots.fill(empty_slot());
        state.dirty_pages.clear();
        state.page_versions.fill(committed_generation);
        state.non_empty_pages.fill(false);
        state.committed_generation = committed_generation;
        state.max_generation = committed_generation;
        Ok(())
    }

    pub fn get_slot(&self, index: usize) -> Option<Slot> {
        self.state.lock().slots.get(index).copied()
    }

    pub fn put_slot(&self, index: usize, slot: Slot) -> Result<(), PagedTimerWheelError> {
        let page_id = index / self.slots_per_page;
        let mut state = self.state.lock();
        let current = state
            .slots
            .get_mut(index)
            .ok_or(PagedTimerWheelError::SlotOutOfRange(index))?;
        if *current == slot {
            return Ok(());
        }
        *current = slot;
        state.dirty_pages.insert(page_id);
        state.page_versions[page_id] = state.page_versions[page_id].saturating_add(1);
        let range = self.page_slot_range(page_id);
        state.non_empty_pages[page_id] = state.slots[range].iter().any(|value| value.num > 0);
        self.metrics.set_dirty_pages(state.dirty_pages.len() as u64);
        Ok(())
    }

    pub fn revise_slots(&self, mut revise: impl FnMut(Slot) -> Slot) {
        let mut state = self.state.lock();
        for index in 0..state.slots.len() {
            let previous = state.slots[index];
            let revised = revise(previous);
            if previous != revised {
                state.slots[index] = revised;
                let page_id = index / self.slots_per_page;
                state.dirty_pages.insert(page_id);
                state.page_versions[page_id] = state.page_versions[page_id].saturating_add(1);
            }
        }
        let dirty: Vec<_> = state.dirty_pages.iter().copied().collect();
        for page_id in dirty {
            let range = self.page_slot_range(page_id);
            state.non_empty_pages[page_id] = state.slots[range].iter().any(|value| value.num > 0);
        }
        self.metrics.set_dirty_pages(state.dirty_pages.len() as u64);
    }

    pub fn flush_dirty(&self) -> Result<u64, PagedTimerWheelError> {
        let (generation, pages) = {
            let state = self.state.lock();
            if state.dirty_pages.is_empty() {
                return Ok(state.committed_generation);
            }
            let generation = state.max_generation.saturating_add(1);
            let mut page_ids = state.dirty_pages.iter().copied().collect::<Vec<_>>();
            page_ids.sort_unstable();
            let pages = page_ids
                .into_iter()
                .map(|page_id| {
                    let range = self.page_slot_range(page_id);
                    FrozenPage {
                        page_id,
                        version: state.page_versions[page_id],
                        slots: state.slots[range].to_vec(),
                    }
                })
                .collect::<Vec<_>>();
            (generation, pages)
        };

        let target = if generation.is_multiple_of(2) {
            self.directory.join(PAGES_A)
        } else {
            self.directory.join(PAGES_B)
        };
        let started = Instant::now();
        let mut file = OpenOptions::new().read(true).write(true).open(target)?;
        for page in &pages {
            let encoded = self.encode_page(page.page_id, generation, &page.slots);
            file.seek(SeekFrom::Start((page.page_id * self.page_size) as u64))?;
            file.write_all(&encoded)?;
            self.metrics.record_physical_write(encoded.len() as u64);
        }
        file.sync_data()?;
        self.metrics.record_fsync(started.elapsed().as_nanos() as u64);
        self.append_generation(generation, pages.iter().map(|page| page.page_id))?;

        let mut state = self.state.lock();
        state.max_generation = state.max_generation.max(generation);
        for page in pages {
            if state.page_versions[page.page_id] == page.version {
                state.dirty_pages.remove(&page.page_id);
                state.page_versions[page.page_id] = generation;
            }
        }
        self.metrics.set_dirty_pages(state.dirty_pages.len() as u64);
        Ok(generation)
    }

    pub fn commit_generation(&self, generation: u64) -> Result<(), PagedTimerWheelError> {
        let mut state = self.state.lock();
        if generation > state.max_generation {
            return Err(PagedTimerWheelError::GenerationNotWritten(generation));
        }
        state.committed_generation = state.committed_generation.max(generation);
        Ok(())
    }

    pub fn committed_generation(&self) -> u64 {
        self.state.lock().committed_generation
    }

    pub fn dirty_page_count(&self) -> usize {
        self.state.lock().dirty_pages.len()
    }

    pub fn non_empty_page_count(&self) -> usize {
        self.state
            .lock()
            .non_empty_pages
            .iter()
            .filter(|non_empty| **non_empty)
            .count()
    }

    pub fn slots_snapshot(&self) -> Vec<Slot> {
        self.state.lock().slots.clone()
    }

    fn initialize_files(&self) -> Result<(), PagedTimerWheelError> {
        let file_len = (self.page_count * self.page_size) as u64;
        let path_a = self.directory.join(PAGES_A);
        let path_b = self.directory.join(PAGES_B);
        for path in [&path_a, &path_b, &self.directory.join(GENERATION_JOURNAL)] {
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }
        let file_a = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path_a)?;
        let file_b = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path_b)?;
        file_a.set_len(file_len)?;
        file_b.set_len(file_len)?;
        file_a.sync_data()?;
        file_b.sync_data()?;
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.directory.join(GENERATION_JOURNAL))?
            .sync_data()?;
        let mut marker = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.directory.join(FORMAT_MARKER))?;
        marker.write_all(b"paged-timer-wheel-v2\n")?;
        marker.sync_data()?;
        Ok(())
    }

    fn read_page(
        &self,
        file: &mut std::fs::File,
        expected_page_id: usize,
        expected_generation: u64,
    ) -> Result<DecodedPage, PagedTimerWheelError> {
        file.seek(SeekFrom::Start((expected_page_id * self.page_size) as u64))?;
        let mut bytes = vec![0u8; self.page_size];
        file.read_exact(&mut bytes)?;
        if bytes.iter().all(|byte| *byte == 0) {
            if expected_generation != 0 {
                return Err(PagedTimerWheelError::InvalidPage(expected_page_id));
            }
            return Ok(DecodedPage {
                generation: 0,
                non_empty: false,
                slots: vec![empty_slot(); self.page_slot_range(expected_page_id).len()],
            });
        }
        if read_u32(&bytes, 0) != PAGE_MAGIC || read_u16(&bytes, 4) != PAGE_VERSION {
            return Err(PagedTimerWheelError::InvalidPage(expected_page_id));
        }
        if read_u16(&bytes, 6) as usize != PAGE_HEADER_SIZE || read_u32(&bytes, 8) as usize != expected_page_id {
            return Err(PagedTimerWheelError::InvalidPage(expected_page_id));
        }
        let generation = read_u64(&bytes, 16);
        if generation != expected_generation {
            return Err(PagedTimerWheelError::UnexpectedPageGeneration {
                page_id: expected_page_id,
                actual: generation,
                expected: expected_generation,
            });
        }
        let payload_len = read_u32(&bytes, 12) as usize;
        if payload_len > self.page_size - PAGE_HEADER_SIZE || !payload_len.is_multiple_of(Slot::SIZE as usize) {
            return Err(PagedTimerWheelError::InvalidPage(expected_page_id));
        }
        let stored_checksum = read_u32(&bytes, 28);
        bytes[28..32].fill(0);
        if crc32c(&bytes) != stored_checksum {
            return Err(PagedTimerWheelError::PageChecksumMismatch(expected_page_id));
        }
        let mut slots = Vec::with_capacity(payload_len / Slot::SIZE as usize);
        for chunk in bytes[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + payload_len].chunks_exact(Slot::SIZE as usize) {
            slots.push(decode_slot(chunk));
        }
        Ok(DecodedPage {
            generation,
            non_empty: read_u32(&bytes, 24) != 0,
            slots,
        })
    }

    fn append_generation(
        &self,
        generation: u64,
        page_ids: impl ExactSizeIterator<Item = usize>,
    ) -> Result<(), PagedTimerWheelError> {
        let page_count = page_ids.len();
        let record_size = GENERATION_HEADER_SIZE + page_count * 4 + GENERATION_TRAILER_SIZE;
        let mut bytes = vec![0u8; record_size];
        bytes[0..4].copy_from_slice(&GENERATION_MAGIC.to_be_bytes());
        bytes[4..6].copy_from_slice(&GENERATION_VERSION.to_be_bytes());
        bytes[6..8].copy_from_slice(&(GENERATION_HEADER_SIZE as u16).to_be_bytes());
        bytes[8..12].copy_from_slice(&(record_size as u32).to_be_bytes());
        bytes[12..20].copy_from_slice(&generation.to_be_bytes());
        bytes[20..24].copy_from_slice(&(page_count as u32).to_be_bytes());
        for (index, page_id) in page_ids.enumerate() {
            let start = GENERATION_HEADER_SIZE + index * 4;
            let page_id = u32::try_from(page_id).map_err(|_| PagedTimerWheelError::InvalidGenerationPage(page_id))?;
            bytes[start..start + 4].copy_from_slice(&page_id.to_be_bytes());
        }
        let checksum = crc32c(&bytes[..record_size - GENERATION_TRAILER_SIZE]);
        bytes[record_size - GENERATION_TRAILER_SIZE..].copy_from_slice(&checksum.to_be_bytes());
        let mut journal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join(GENERATION_JOURNAL))?;
        let started = Instant::now();
        journal.write_all(&bytes)?;
        journal.sync_data()?;
        self.metrics.record_physical_write(bytes.len() as u64);
        self.metrics.record_fsync(started.elapsed().as_nanos() as u64);
        Ok(())
    }

    fn load_expected_generations(&self, committed_generation: u64) -> Result<Vec<u64>, PagedTimerWheelError> {
        let path = self.directory.join(GENERATION_JOURNAL);
        let mut journal = OpenOptions::new().read(true).write(true).open(path)?;
        let mut bytes = Vec::new();
        journal.read_to_end(&mut bytes)?;
        let mut expected = vec![0u64; self.page_count];
        let mut cursor = 0usize;
        let mut committed_end = 0usize;
        let mut last_generation = 0u64;
        let load_all = committed_generation == u64::MAX;
        while cursor < bytes.len() {
            if bytes.len() - cursor < GENERATION_HEADER_SIZE + GENERATION_TRAILER_SIZE {
                break;
            }
            let header = &bytes[cursor..];
            if read_u32(header, 0) != GENERATION_MAGIC
                || read_u16(header, 4) != GENERATION_VERSION
                || read_u16(header, 6) as usize != GENERATION_HEADER_SIZE
            {
                break;
            }
            let record_size = read_u32(header, 8) as usize;
            let page_count = read_u32(header, 20) as usize;
            let expected_size = GENERATION_HEADER_SIZE + page_count * 4 + GENERATION_TRAILER_SIZE;
            if record_size != expected_size || cursor + record_size > bytes.len() {
                break;
            }
            let record = &bytes[cursor..cursor + record_size];
            if crc32c(&record[..record_size - GENERATION_TRAILER_SIZE])
                != read_u32(record, record_size - GENERATION_TRAILER_SIZE)
            {
                break;
            }
            let generation = read_u64(record, 12);
            if generation != last_generation.saturating_add(1) {
                break;
            }
            if !load_all && generation > committed_generation {
                break;
            }
            for index in 0..page_count {
                let page_id = read_u32(record, GENERATION_HEADER_SIZE + index * 4) as usize;
                let page_generation = expected
                    .get_mut(page_id)
                    .ok_or(PagedTimerWheelError::InvalidGenerationPage(page_id))?;
                *page_generation = generation;
            }
            last_generation = generation;
            cursor += record_size;
            committed_end = cursor;
        }
        if !load_all && last_generation != committed_generation {
            return Err(PagedTimerWheelError::MissingGeneration {
                actual: last_generation,
                expected: committed_generation,
            });
        }
        if committed_end != bytes.len() {
            journal.set_len(committed_end as u64)?;
            journal.sync_data()?;
        }
        Ok(expected)
    }

    fn encode_page(&self, page_id: usize, generation: u64, slots: &[Slot]) -> Vec<u8> {
        let payload_len = slots.len() * Slot::SIZE as usize;
        let mut bytes = vec![0u8; self.page_size];
        bytes[0..4].copy_from_slice(&PAGE_MAGIC.to_be_bytes());
        bytes[4..6].copy_from_slice(&PAGE_VERSION.to_be_bytes());
        bytes[6..8].copy_from_slice(&(PAGE_HEADER_SIZE as u16).to_be_bytes());
        bytes[8..12].copy_from_slice(&(page_id as u32).to_be_bytes());
        bytes[12..16].copy_from_slice(&(payload_len as u32).to_be_bytes());
        bytes[16..24].copy_from_slice(&generation.to_be_bytes());
        bytes[24..28].copy_from_slice(&u32::from(slots.iter().any(|slot| slot.num > 0)).to_be_bytes());
        for (index, slot) in slots.iter().enumerate() {
            let start = PAGE_HEADER_SIZE + index * Slot::SIZE as usize;
            encode_slot(*slot, &mut bytes[start..start + Slot::SIZE as usize]);
        }
        let checksum = crc32c(&bytes);
        bytes[28..32].copy_from_slice(&checksum.to_be_bytes());
        bytes
    }

    fn page_slot_range(&self, page_id: usize) -> std::ops::Range<usize> {
        let start = page_id * self.slots_per_page;
        start..(start + self.slots_per_page).min(self.slots_total)
    }
}

struct FrozenPage {
    page_id: usize,
    version: u64,
    slots: Vec<Slot>,
}

struct DecodedPage {
    generation: u64,
    non_empty: bool,
    slots: Vec<Slot>,
}

#[derive(Debug, Error)]
pub enum PagedTimerWheelError {
    #[error("timer wheel I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("timer wheel page size {0} is invalid")]
    InvalidPageSize(usize),
    #[error("timer wheel slot count is {actual}, expected {expected}")]
    InvalidSlotCount { actual: usize, expected: usize },
    #[error("timer wheel slot {0} is out of range")]
    SlotOutOfRange(usize),
    #[error("timer wheel page {0} is invalid")]
    InvalidPage(usize),
    #[error("timer wheel page {0} checksum does not match")]
    PageChecksumMismatch(usize),
    #[error("timer wheel page {page_id} has generation {actual}, expected {expected}")]
    UnexpectedPageGeneration { page_id: usize, actual: u64, expected: u64 },
    #[error("timer wheel generation journal references out-of-range page {0}")]
    InvalidGenerationPage(usize),
    #[error("timer wheel generation journal ends at {actual}, expected {expected}")]
    MissingGeneration { actual: u64, expected: u64 },
    #[error("timer wheel generation {0} has not been written")]
    GenerationNotWritten(u64),
}

fn empty_slot() -> Slot {
    Slot::new_with_num_magic(0, 0, 0, 0, 0)
}

fn encode_slot(slot: Slot, bytes: &mut [u8]) {
    bytes[0..8].copy_from_slice(&slot.time_ms.to_be_bytes());
    bytes[8..16].copy_from_slice(&slot.first_pos.to_be_bytes());
    bytes[16..24].copy_from_slice(&slot.last_pos.to_be_bytes());
    bytes[24..28].copy_from_slice(&slot.num.to_be_bytes());
    bytes[28..32].copy_from_slice(&slot.magic.to_be_bytes());
}

fn decode_slot(bytes: &[u8]) -> Slot {
    Slot::new_with_num_magic(
        read_i64(bytes, 0),
        read_i64(bytes, 8),
        read_i64(bytes, 16),
        read_i32(bytes, 24),
        read_i32(bytes, 28),
    )
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
    fn one_slot_flush_writes_one_page_and_recovers_by_generation() {
        let directory = tempdir().unwrap();
        let metrics = Arc::new(TimerStorageMetrics::default());
        let wheel = PagedTimerWheel::new(directory.path(), 32, 288, Arc::clone(&metrics)).unwrap();
        wheel.load(0).unwrap();
        let baseline = metrics.snapshot().physical_write_bytes;
        wheel
            .put_slot(9, Slot::new_with_num_magic(1_000, 40, 40, 1, 1))
            .unwrap();
        let generation = wheel.flush_dirty().unwrap();
        wheel.commit_generation(generation).unwrap();
        assert_eq!(metrics.snapshot().physical_write_bytes - baseline, 320);

        let reloaded = PagedTimerWheel::new(directory.path(), 32, 288, metrics).unwrap();
        reloaded.load(generation).unwrap();
        assert_eq!(reloaded.get_slot(9).unwrap().time_ms, 1_000);
    }

    #[test]
    fn uncommitted_generation_is_not_loaded() {
        let directory = tempdir().unwrap();
        let metrics = Arc::new(TimerStorageMetrics::default());
        let wheel = PagedTimerWheel::new(directory.path(), 16, 288, Arc::clone(&metrics)).unwrap();
        wheel.load(0).unwrap();
        wheel
            .put_slot(2, Slot::new_with_num_magic(2_000, 40, 40, 1, 1))
            .unwrap();
        assert_eq!(wheel.flush_dirty().unwrap(), 1);

        let reloaded = PagedTimerWheel::new(directory.path(), 16, 288, metrics).unwrap();
        reloaded.load(0).unwrap();
        assert_eq!(reloaded.get_slot(2).unwrap(), empty_slot());
    }

    #[test]
    fn corrupt_committed_copy_rejects_mixed_generation_state() {
        let directory = tempdir().unwrap();
        let metrics = Arc::new(TimerStorageMetrics::default());
        let wheel = PagedTimerWheel::new(directory.path(), 16, 288, Arc::clone(&metrics)).unwrap();
        wheel.load(0).unwrap();
        wheel
            .put_slot(1, Slot::new_with_num_magic(1_000, 40, 40, 1, 1))
            .unwrap();
        let generation = wheel.flush_dirty().unwrap();
        wheel.commit_generation(generation).unwrap();
        OpenOptions::new()
            .write(true)
            .open(directory.path().join(PAGES_B))
            .unwrap()
            .write_all(&[0; 32])
            .unwrap();

        let reloaded = PagedTimerWheel::new(directory.path(), 16, 288, metrics).unwrap();
        assert!(matches!(
            reloaded.load(generation),
            Err(PagedTimerWheelError::InvalidPage(0))
        ));
    }
}
