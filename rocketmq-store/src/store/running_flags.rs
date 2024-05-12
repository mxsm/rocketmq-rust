/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::sync::atomic::{AtomicU32, Ordering};

pub struct RunningFlags {
    flag_bits: AtomicU32,
}

const NOT_READABLE_BIT: u32 = 1;
const NOT_WRITEABLE_BIT: u32 = 1 << 1;
const WRITE_LOGICS_QUEUE_ERROR_BIT: u32 = 1 << 2;
const WRITE_INDEX_FILE_ERROR_BIT: u32 = 1 << 3;
const DISK_FULL_BIT: u32 = 1 << 4;
const FENCED_BIT: u32 = 1 << 5;
const LOGIC_DISK_FULL_BIT: u32 = 1 << 6;

impl RunningFlags {
    pub fn new() -> Self {
        Self {
            flag_bits: AtomicU32::new(0),
        }
    }

    pub fn get_flag_bits(&self) -> u32 {
        self.flag_bits.load(Ordering::Acquire)
    }

    pub fn get_and_make_readable(&self) -> bool {
        let result = self.is_readable();
        if !result {
            self.flag_bits
                .fetch_and(!(NOT_READABLE_BIT), Ordering::AcqRel);
        }
        result
    }

    pub fn is_readable(&self) -> bool {
        self.flag_bits.load(Ordering::Acquire) & NOT_READABLE_BIT == 0
    }

    pub fn is_fenced(&self) -> bool {
        self.flag_bits.load(Ordering::Acquire) & FENCED_BIT != 0
    }

    pub fn get_and_make_not_readable(&self) -> bool {
        let result = self.is_readable();
        if result {
            self.flag_bits.fetch_or(NOT_READABLE_BIT, Ordering::AcqRel);
        }
        result
    }

    pub fn clear_logics_queue_error(&self) {
        self.flag_bits
            .fetch_and(!(WRITE_LOGICS_QUEUE_ERROR_BIT), Ordering::AcqRel);
    }

    pub fn get_and_make_writeable(&self) -> bool {
        let result = self.is_writeable();
        if !result {
            self.flag_bits
                .fetch_and(!(NOT_WRITEABLE_BIT), Ordering::AcqRel);
        }
        result
    }

    pub fn is_writeable(&self) -> bool {
        let flags = self.flag_bits.load(Ordering::Acquire);
        flags & 0b0011110 == 0
    }

    pub fn is_cq_writeable(&self) -> bool {
        let flags = self.flag_bits.load(Ordering::Acquire);
        flags & 0b0011100 == 0
    }

    pub fn get_and_make_not_writeable(&self) -> bool {
        let result = self.is_writeable();
        if result {
            self.flag_bits.fetch_or(NOT_WRITEABLE_BIT, Ordering::AcqRel);
        }
        result
    }

    pub fn make_logics_queue_error(&self) {
        self.flag_bits
            .fetch_or(WRITE_LOGICS_QUEUE_ERROR_BIT, Ordering::AcqRel);
    }

    pub fn make_fenced(&self, fenced: bool) {
        if fenced {
            self.flag_bits.fetch_or(FENCED_BIT, Ordering::AcqRel);
        } else {
            self.flag_bits.fetch_and(!(FENCED_BIT), Ordering::AcqRel);
        }
    }

    pub fn is_logics_queue_error(&self) -> bool {
        let flags = self.flag_bits.load(Ordering::Acquire);
        flags & WRITE_LOGICS_QUEUE_ERROR_BIT != 0
    }

    pub fn make_index_file_error(&self) {
        self.flag_bits
            .fetch_or(WRITE_INDEX_FILE_ERROR_BIT, Ordering::AcqRel);
    }

    pub fn is_index_file_error(&self) -> bool {
        let flags = self.flag_bits.load(Ordering::Acquire);
        flags & WRITE_INDEX_FILE_ERROR_BIT != 0
    }

    pub fn get_and_make_disk_full(&self) -> bool {
        let result = (self.flag_bits.fetch_and(!(DISK_FULL_BIT), Ordering::AcqRel)) == 0;
        self.flag_bits.fetch_or(DISK_FULL_BIT, Ordering::AcqRel);
        result
    }

    pub fn get_and_make_disk_ok(&self) -> bool {
        (self.flag_bits.fetch_and(!(DISK_FULL_BIT), Ordering::AcqRel)) == 0
    }

    pub fn get_and_make_logic_disk_full(&self) -> bool {
        let result = (self
            .flag_bits
            .fetch_and(!(LOGIC_DISK_FULL_BIT), Ordering::AcqRel))
            == 0;
        self.flag_bits
            .fetch_or(LOGIC_DISK_FULL_BIT, Ordering::AcqRel);
        result
    }

    pub fn get_and_make_logic_disk_ok(&self) -> bool {
        (self
            .flag_bits
            .fetch_and(!(LOGIC_DISK_FULL_BIT), Ordering::AcqRel))
            == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_flag_bits() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.get_flag_bits(), 0);
    }

    #[test]
    fn test_get_and_make_readable() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.get_and_make_readable(), true);
        assert_eq!(running_flags.is_readable(), true);
        assert_eq!(running_flags.get_and_make_readable(), true);
        assert_eq!(running_flags.is_readable(), true);
    }

    #[test]
    fn test_is_readable() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.is_readable(), true);
        running_flags
            .flag_bits
            .store(NOT_READABLE_BIT, Ordering::Relaxed);
        assert_eq!(running_flags.is_readable(), false);
    }

    #[test]
    fn test_is_fenced() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.is_fenced(), false);
        running_flags.flag_bits.store(FENCED_BIT, Ordering::Relaxed);
        assert_eq!(running_flags.is_fenced(), true);
    }

    #[test]
    fn test_get_and_make_not_readable() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.get_and_make_not_readable(), true);
        assert_eq!(running_flags.is_readable(), false);
        assert_eq!(running_flags.get_and_make_not_readable(), false);
        assert_eq!(running_flags.is_readable(), false);
    }

    #[test]
    fn test_clear_logics_queue_error() {
        let running_flags = RunningFlags::new();
        running_flags
            .flag_bits
            .store(WRITE_LOGICS_QUEUE_ERROR_BIT, Ordering::Relaxed);
        running_flags.clear_logics_queue_error();
        assert_eq!(running_flags.is_logics_queue_error(), false);
    }

    #[test]
    fn test_get_and_make_writeable() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.get_and_make_writeable(), true);
        assert_eq!(running_flags.is_writeable(), true);
        assert_eq!(running_flags.is_cq_writeable(), true);
    }

    #[test]
    fn test_is_writeable() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.is_writeable(), true);
        running_flags
            .flag_bits
            .store(NOT_WRITEABLE_BIT, Ordering::Relaxed);
        assert_eq!(running_flags.is_writeable(), false);
    }

    #[test]
    fn test_is_cq_writeable() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.is_cq_writeable(), true);
        running_flags.flag_bits.store(
            NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT,
            Ordering::Relaxed,
        );
        assert_eq!(running_flags.is_cq_writeable(), false);
    }

    #[test]
    fn test_get_and_make_not_writeable() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.get_and_make_not_writeable(), true);
        assert_eq!(running_flags.is_writeable(), false);
        assert_eq!(running_flags.get_and_make_not_writeable(), false);
        assert_eq!(running_flags.is_writeable(), false);
    }

    #[test]
    fn test_make_logics_queue_error() {
        let running_flags = RunningFlags::new();
        running_flags.make_logics_queue_error();
        assert_eq!(running_flags.is_logics_queue_error(), true);
    }

    #[test]
    fn test_make_fenced() {
        let running_flags = RunningFlags::new();
        running_flags.make_fenced(true);
        assert_eq!(running_flags.is_fenced(), true);
        running_flags.make_fenced(false);
        assert_eq!(running_flags.is_fenced(), false);
    }

    #[test]
    fn test_is_logics_queue_error() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.is_logics_queue_error(), false);
        running_flags
            .flag_bits
            .store(WRITE_LOGICS_QUEUE_ERROR_BIT, Ordering::Relaxed);
        assert_eq!(running_flags.is_logics_queue_error(), true);
    }

    #[test]
    fn test_make_index_file_error() {
        let running_flags = RunningFlags::new();
        running_flags.make_index_file_error();
        assert_eq!(running_flags.is_index_file_error(), true);
    }

    #[test]
    fn test_is_index_file_error() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.is_index_file_error(), false);
        running_flags
            .flag_bits
            .store(WRITE_INDEX_FILE_ERROR_BIT, Ordering::Relaxed);
        assert_eq!(running_flags.is_index_file_error(), true);
    }

    #[test]
    fn test_get_and_make_disk_full() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.get_and_make_disk_full(), true);
    }

    #[test]
    fn test_get_and_make_disk_ok() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.get_and_make_disk_ok(), true);
    }

    #[test]
    fn test_get_and_make_logic_disk_full() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.get_and_make_logic_disk_full(), true);
    }

    #[test]
    fn test_get_and_make_logic_disk_ok() {
        let running_flags = RunningFlags::new();
        assert_eq!(running_flags.get_and_make_logic_disk_ok(), true);
    }
}
