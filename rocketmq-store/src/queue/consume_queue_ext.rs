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

use std::path::PathBuf;

use bytes::Buf;
use cheetah_string::CheetahString;
use rocketmq_rust::ArcMut;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::consume_queue::cq_ext_unit::CqExtUnit;
use crate::consume_queue::cq_ext_unit::MAX_EXT_UNIT_SIZE;
use crate::consume_queue::mapped_file_queue::MappedFileQueue;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;

const END_BLANK_DATA_LENGTH: usize = 4;

/// Addr can not exceed this value. For compatible.
const MAX_ADDR: i64 = i32::MIN as i64 - 1;
const MAX_REAL_OFFSET: i64 = MAX_ADDR - i64::MIN;

#[derive(Clone)]
pub struct ConsumeQueueExt {
    mapped_file_queue: ArcMut<MappedFileQueue>,
    topic: CheetahString,
    queue_id: i32,
    store_path: CheetahString,
    mapped_file_size: i32,
}

impl ConsumeQueueExt {
    pub fn new(
        topic: CheetahString,
        queue_id: i32,
        store_path: CheetahString,
        mapped_file_size: i32,
        bit_map_length: i32,
    ) -> Self {
        let queue_dir = PathBuf::from(store_path.as_str())
            .join(topic.as_str())
            .join(queue_id.to_string());
        let mapped_file_queue = ArcMut::new(MappedFileQueue::new(
            queue_dir.to_string_lossy().to_string(),
            mapped_file_size as u64,
            None,
        ));
        let _ = bit_map_length;
        Self {
            mapped_file_queue,
            topic,
            queue_id,
            store_path,
            mapped_file_size,
        }
    }

    pub fn is_ext_addr(address: i64) -> bool {
        address <= MAX_ADDR
    }

    #[inline]
    fn decorate(offset: i64) -> i64 {
        if !Self::is_ext_addr(offset) {
            offset + i64::MIN
        } else {
            offset
        }
    }

    #[inline]
    fn undecorate(address: i64) -> i64 {
        if Self::is_ext_addr(address) {
            address - i64::MIN
        } else {
            address
        }
    }

    #[inline]
    fn fill_to_end(&self, mapped_file: &DefaultMappedFile, wrote_position: i32) {
        let end_marker_len = i16::BITS as usize / 8;
        if wrote_position as usize + end_marker_len <= self.mapped_file_size as usize {
            let _ = mapped_file.put_slice(&(-1i16).to_be_bytes(), wrote_position as usize);
        }
        mapped_file.set_wrote_position(self.mapped_file_size);
    }

    pub fn truncate_by_max_address(&self, max_address: i64) {
        if !Self::is_ext_addr(max_address) {
            return;
        }

        info!("Truncate consume queue ext by max {}.", max_address);

        let mut cq_ext_unit = CqExtUnit::default();
        if !self.get(max_address, &mut cq_ext_unit) {
            error!("[BUG] address {} of consume queue extend not found!", max_address);
            return;
        }

        let real_offset = Self::undecorate(max_address);
        self.mapped_file_queue
            .mut_from_ref()
            .truncate_dirty_files(real_offset + cq_ext_unit.size() as i64);
    }

    pub fn truncate_by_min_address(&self, min_address: i64) {
        if !Self::is_ext_addr(min_address) {
            return;
        }

        info!("Truncate consume queue ext by min {}.", min_address);

        let real_offset = Self::undecorate(min_address);
        let mut will_remove_files = Vec::new();
        let mapped_files = self.mapped_file_queue.get_mapped_files().load().clone();

        for mapped_file in mapped_files.iter() {
            let file_tail_offset = mapped_file.get_file_from_offset() as i64 + self.mapped_file_size as i64;
            if file_tail_offset < real_offset {
                info!(
                    "Destroy consume queue ext by min: file={}, fileTailOffset={}, minOffset={}",
                    mapped_file.get_file_name(),
                    file_tail_offset,
                    real_offset
                );
                if mapped_file.destroy(1000) {
                    will_remove_files.push(mapped_file.clone());
                }
            }
        }

        self.mapped_file_queue.delete_expired_file(will_remove_files);
    }

    pub fn load(&mut self) -> bool {
        let result = self.mapped_file_queue.load();
        info!(
            "load consume queue extend {}-{}  {}",
            self.topic,
            self.queue_id,
            if result { "OK" } else { "Failed" }
        );

        result
    }

    pub fn recover(&mut self) {
        let mapped_files = self.mapped_file_queue.get_mapped_files().load().clone();
        if mapped_files.is_empty() {
            return;
        }

        let mut process_offset = 0i64;
        for (index, mapped_file) in mapped_files.iter().enumerate() {
            let read_position = mapped_file.get_read_position() as usize;
            let mut mapped_file_offset = 0usize;

            loop {
                let size_prefix_len = i16::BITS as usize / 8;
                if mapped_file_offset + size_prefix_len > read_position {
                    break;
                }

                let remaining = read_position - mapped_file_offset;
                let Some(mut buffer) = mapped_file.get_bytes_readable_checked(mapped_file_offset, remaining) else {
                    break;
                };

                let before = buffer.remaining();
                let mut ext_unit = CqExtUnit::default();
                ext_unit.read_by_skip(&mut buffer);
                let consumed = before - buffer.remaining();

                if ext_unit.size() > 0 && consumed == ext_unit.size() as usize {
                    mapped_file_offset += ext_unit.size() as usize;
                    continue;
                }

                break;
            }

            process_offset = mapped_file.get_file_from_offset() as i64 + mapped_file_offset as i64;

            if mapped_file_offset != read_position && index < mapped_files.len() - 1 {
                info!(
                    "Recover next consume queue extend file, {}",
                    mapped_file.get_file_name()
                );
            }
        }

        self.mapped_file_queue.set_flushed_where(process_offset);
        self.mapped_file_queue.set_committed_where(process_offset);
        self.mapped_file_queue
            .mut_from_ref()
            .truncate_dirty_files(process_offset);
    }

    pub fn put(&self, cq_ext_unit: CqExtUnit) -> i64 {
        const RETRY_TIMES: i32 = 3;

        let size = cq_ext_unit.calc_unit_size();
        if size > MAX_EXT_UNIT_SIZE as i32 {
            error!(
                "Size of cq ext unit is greater than {}, {}",
                MAX_EXT_UNIT_SIZE, cq_ext_unit
            );
            return 1;
        }

        if self.mapped_file_queue.get_max_offset() + size as i64 > MAX_REAL_OFFSET {
            warn!(
                "Capacity of ext is maximum!{}, {}",
                self.mapped_file_queue.get_max_offset(),
                size
            );
            return 1;
        }

        for _ in 0..RETRY_TIMES {
            let mapped_file = {
                let queue = self.mapped_file_queue.mut_from_ref();
                match queue.get_last_mapped_file() {
                    Some(mapped_file) if !mapped_file.is_full() => mapped_file,
                    _ => match queue.get_last_mapped_file_mut_start_offset(0, true) {
                        Some(mapped_file) => mapped_file,
                        None => {
                            error!("Create mapped file when save consume queue extend, {}", cq_ext_unit);
                            continue;
                        }
                    },
                }
            };

            let wrote_position = mapped_file.get_wrote_position();
            let blank_size = self.mapped_file_size - wrote_position - END_BLANK_DATA_LENGTH as i32;

            if size > blank_size {
                self.fill_to_end(mapped_file.as_ref(), wrote_position);
                info!(
                    "No enough space(need:{}, has:{}) of file {}, so fill to end",
                    size,
                    blank_size,
                    mapped_file.get_file_name()
                );
                continue;
            }

            let mut ext_unit = cq_ext_unit.clone();
            let data = ext_unit.write();
            if mapped_file.append_message_offset_length(&data, 0, size as usize) {
                return Self::decorate(wrote_position as i64 + mapped_file.get_file_from_offset() as i64);
            }
        }

        1
    }

    pub fn destroy(&mut self) {
        self.mapped_file_queue.destroy();
    }

    pub fn get(&self, address: i64, cq_ext_unit: &mut CqExtUnit) -> bool {
        if !Self::is_ext_addr(address) {
            return false;
        }

        let real_offset = Self::undecorate(address);
        let Some(mapped_file) = self
            .mapped_file_queue
            .find_mapped_file_by_offset(real_offset, real_offset == 0)
        else {
            return false;
        };

        let pos = (real_offset % self.mapped_file_size as i64) as usize;
        let readable = mapped_file.get_read_position() as usize;
        if pos >= readable {
            warn!("[BUG] Consume queue extend unit({}) is not found!", real_offset);
            return false;
        }

        let Some(mut buffer) = mapped_file.get_bytes_readable_checked(pos, readable - pos) else {
            warn!("[BUG] Consume queue extend unit({}) is not found!", real_offset);
            return false;
        };

        cq_ext_unit.read(&mut buffer)
    }

    pub fn flush(&self, flush_least_pages: i32) -> bool {
        self.mapped_file_queue.flush(flush_least_pages)
    }

    pub fn get_total_size(&self) -> i64 {
        self.mapped_file_queue.get_total_file_size()
    }

    pub fn check_self(&self) {
        let _ = self.store_path.as_str();
        self.mapped_file_queue.check_self()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn new_test_ext(temp_dir: &tempfile::TempDir, mapped_file_size: i32) -> ConsumeQueueExt {
        ConsumeQueueExt::new(
            CheetahString::from_static_str("cqext-topic"),
            0,
            CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
            mapped_file_size,
            64,
        )
    }

    #[test]
    fn cqext_put_and_get_round_trip() {
        let temp_dir = tempdir().unwrap();
        let ext = new_test_ext(&temp_dir, 64);
        let original = CqExtUnit::new(12345, 67890, Some(vec![1, 2, 3, 4]));

        let address = ext.put(original.clone());
        assert!(ConsumeQueueExt::is_ext_addr(address));

        let mut restored = CqExtUnit::default();
        assert!(ext.get(address, &mut restored));
        assert_eq!(restored, original);
    }

    #[test]
    fn cqext_recover_truncates_dirty_tail() {
        let temp_dir = tempdir().unwrap();
        let ext = new_test_ext(&temp_dir, 64);
        let _first_addr = ext.put(CqExtUnit::new(11, 101, Some(vec![1, 2])));
        let second = CqExtUnit::new(22, 202, Some(vec![3, 4, 5]));
        let second_addr = ext.put(second.clone());

        let queue = ext.mapped_file_queue.mut_from_ref();
        let mapped_file = queue.get_last_mapped_file().expect("mapped file");
        let dirty_pos = mapped_file.get_wrote_position() as usize;
        assert!(mapped_file.put_slice(&[0x7F, 0xFF, 0xAA], dirty_pos));
        mapped_file.set_wrote_position(dirty_pos as i32 + 3);

        let mut reloaded = new_test_ext(&temp_dir, 64);
        assert!(reloaded.load());
        reloaded.recover();

        let mut restored = CqExtUnit::default();
        assert!(reloaded.get(second_addr, &mut restored));
        assert_eq!(restored, second);
        assert_eq!(
            reloaded.mapped_file_queue.get_max_offset(),
            ConsumeQueueExt::undecorate(second_addr) + restored.size() as i64
        );
    }

    #[test]
    fn cqext_truncate_by_min_address_removes_older_files() {
        let temp_dir = tempdir().unwrap();
        let ext = new_test_ext(&temp_dir, 64);

        let addresses: Vec<i64> = (0..5)
            .map(|index| ext.put(CqExtUnit::new(100 + index, 1000 + index, None)))
            .collect();

        assert_eq!(ext.mapped_file_queue.get_mapped_files_size(), 2);

        ext.truncate_by_min_address(addresses[4]);

        assert_eq!(ext.mapped_file_queue.get_mapped_files_size(), 1);
        let first_file = ext
            .mapped_file_queue
            .get_first_mapped_file()
            .expect("remaining mapped file");
        assert_eq!(
            first_file.get_file_from_offset() as i64,
            ConsumeQueueExt::undecorate(addresses[3])
        );
    }

    #[test]
    fn cqext_truncate_by_max_address_discards_newer_units() {
        let temp_dir = tempdir().unwrap();
        let ext = new_test_ext(&temp_dir, 64);

        let first_addr = ext.put(CqExtUnit::new(11, 101, None));
        let second_addr = ext.put(CqExtUnit::new(22, 202, Some(vec![1, 2, 3])));
        let third_addr = ext.put(CqExtUnit::new(33, 303, Some(vec![4, 5, 6, 7])));

        ext.truncate_by_max_address(second_addr);

        let mut first = CqExtUnit::default();
        let mut second = CqExtUnit::default();
        let mut third = CqExtUnit::default();
        assert!(ext.get(first_addr, &mut first));
        assert!(ext.get(second_addr, &mut second));
        assert!(!ext.get(third_addr, &mut third));
    }
}
