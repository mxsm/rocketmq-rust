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

use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;

// Bit flags to represent different states
const NOT_READABLE_BIT: i32 = 1;
const NOT_WRITEABLE_BIT: i32 = 1 << 1;
const WRITE_LOGICS_QUEUE_ERROR_BIT: i32 = 1 << 2;
const WRITE_INDEX_FILE_ERROR_BIT: i32 = 1 << 3;
const DISK_FULL_BIT: i32 = 1 << 4;
const FENCED_BIT: i32 = 1 << 5;
const LOGIC_DISK_FULL_BIT: i32 = 1 << 6;

/// `RunningFlags` is a structure to manage various states using bit flags.
/// The state is represented by an `AtomicI32` to ensure thread safety.
pub struct RunningFlags {
    flag_bits: AtomicI32,
}

impl Default for RunningFlags {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl RunningFlags {
    #[inline]
    pub fn new() -> Self {
        RunningFlags {
            flag_bits: AtomicI32::new(0),
        }
    }

    #[inline]
    pub fn get_flag_bits(&self) -> i32 {
        self.flag_bits.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn get_and_make_readable(&self) -> bool {
        let result = self.is_readable();
        if !result {
            self.flag_bits.fetch_and(!NOT_READABLE_BIT, Ordering::SeqCst);
        }
        result
    }

    #[inline]
    pub fn is_readable(&self) -> bool {
        (self.flag_bits.load(Ordering::SeqCst) & NOT_READABLE_BIT) == 0
    }

    #[inline]
    pub fn is_fenced(&self) -> bool {
        (self.flag_bits.load(Ordering::SeqCst) & FENCED_BIT) != 0
    }

    #[inline]
    pub fn get_and_make_not_readable(&self) -> bool {
        let result = self.is_readable();
        if result {
            self.flag_bits.fetch_or(NOT_READABLE_BIT, Ordering::SeqCst);
        }
        result
    }

    #[inline]
    pub fn clear_logics_queue_error(&self) {
        self.flag_bits
            .fetch_and(!WRITE_LOGICS_QUEUE_ERROR_BIT, Ordering::SeqCst);
    }

    #[inline]
    pub fn get_and_make_writeable(&self) -> bool {
        let result = self.is_writeable();
        if !result {
            self.flag_bits.fetch_and(!NOT_WRITEABLE_BIT, Ordering::SeqCst);
        }
        result
    }

    #[inline]
    pub fn is_writeable(&self) -> bool {
        (self.flag_bits.load(Ordering::SeqCst)
            & (NOT_WRITEABLE_BIT
                | WRITE_LOGICS_QUEUE_ERROR_BIT
                | DISK_FULL_BIT
                | WRITE_INDEX_FILE_ERROR_BIT
                | FENCED_BIT
                | LOGIC_DISK_FULL_BIT))
            == 0
    }

    #[inline]
    pub fn is_cq_writeable(&self) -> bool {
        (self.flag_bits.load(Ordering::SeqCst)
            & (NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | WRITE_INDEX_FILE_ERROR_BIT | LOGIC_DISK_FULL_BIT))
            == 0
    }

    #[inline]
    pub fn get_and_make_not_writeable(&self) -> bool {
        let result = self.is_writeable();
        if result {
            self.flag_bits.fetch_or(NOT_WRITEABLE_BIT, Ordering::SeqCst);
        }
        result
    }

    #[inline]
    pub fn make_logics_queue_error(&self) {
        self.flag_bits.fetch_or(WRITE_LOGICS_QUEUE_ERROR_BIT, Ordering::SeqCst);
    }

    #[inline]
    pub fn make_fenced(&self, fenced: bool) {
        if fenced {
            self.flag_bits.fetch_or(FENCED_BIT, Ordering::SeqCst);
        } else {
            self.flag_bits.fetch_and(!FENCED_BIT, Ordering::SeqCst);
        }
    }

    #[inline]
    pub fn is_logics_queue_error(&self) -> bool {
        (self.flag_bits.load(Ordering::SeqCst) & WRITE_LOGICS_QUEUE_ERROR_BIT) == WRITE_LOGICS_QUEUE_ERROR_BIT
    }

    #[inline]
    pub fn make_index_file_error(&self) {
        self.flag_bits.fetch_or(WRITE_INDEX_FILE_ERROR_BIT, Ordering::SeqCst);
    }

    #[inline]
    pub fn is_index_file_error(&self) -> bool {
        (self.flag_bits.load(Ordering::SeqCst) & WRITE_INDEX_FILE_ERROR_BIT) == WRITE_INDEX_FILE_ERROR_BIT
    }

    #[inline]
    pub fn get_and_make_disk_full(&self) -> bool {
        let result = (self.flag_bits.load(Ordering::SeqCst) & DISK_FULL_BIT) != DISK_FULL_BIT;
        self.flag_bits.fetch_or(DISK_FULL_BIT, Ordering::SeqCst);
        result
    }

    #[inline]
    pub fn get_and_make_disk_ok(&self) -> bool {
        let result = (self.flag_bits.load(Ordering::SeqCst) & DISK_FULL_BIT) != DISK_FULL_BIT;
        self.flag_bits.fetch_and(!DISK_FULL_BIT, Ordering::SeqCst);
        result
    }

    #[inline]
    pub fn get_and_make_logic_disk_full(&self) -> bool {
        let result = (self.flag_bits.load(Ordering::SeqCst) & LOGIC_DISK_FULL_BIT) != LOGIC_DISK_FULL_BIT;
        self.flag_bits.fetch_or(LOGIC_DISK_FULL_BIT, Ordering::SeqCst);
        result
    }

    #[inline]
    pub fn get_and_make_logic_disk_ok(&self) -> bool {
        let result = (self.flag_bits.load(Ordering::SeqCst) & LOGIC_DISK_FULL_BIT) != LOGIC_DISK_FULL_BIT;
        self.flag_bits.fetch_and(!LOGIC_DISK_FULL_BIT, Ordering::SeqCst);
        result
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
        assert!(running_flags.get_and_make_readable());
        assert!(running_flags.is_readable());
        assert!(running_flags.get_and_make_readable());
        assert!(running_flags.is_readable());
    }

    #[test]
    fn test_is_readable() {
        let running_flags = RunningFlags::new();
        assert!(running_flags.is_readable());
        running_flags.flag_bits.store(NOT_READABLE_BIT, Ordering::Relaxed);
        assert!(!running_flags.is_readable());
    }

    #[test]
    fn test_is_fenced() {
        let running_flags = RunningFlags::new();
        assert!(!running_flags.is_fenced());
        running_flags.flag_bits.store(FENCED_BIT, Ordering::Relaxed);
        assert!(running_flags.is_fenced());
    }

    #[test]
    fn test_get_and_make_not_readable() {
        let running_flags = RunningFlags::new();
        assert!(running_flags.get_and_make_not_readable());
        assert!(!running_flags.is_readable());
        assert!(!running_flags.get_and_make_not_readable());
        assert!(!running_flags.is_readable());
    }

    #[test]
    fn test_clear_logics_queue_error() {
        let running_flags = RunningFlags::new();
        running_flags
            .flag_bits
            .store(WRITE_LOGICS_QUEUE_ERROR_BIT, Ordering::Relaxed);
        running_flags.clear_logics_queue_error();
        assert!(!running_flags.is_logics_queue_error());
    }

    #[test]
    fn test_get_and_make_writeable() {
        let running_flags = RunningFlags::new();
        assert!(running_flags.get_and_make_writeable());
        assert!(running_flags.is_writeable());
        assert!(running_flags.is_cq_writeable());
    }

    #[test]
    fn test_is_writeable() {
        let running_flags = RunningFlags::new();
        assert!(running_flags.is_writeable());
        running_flags.flag_bits.store(NOT_WRITEABLE_BIT, Ordering::Relaxed);
        assert!(!running_flags.is_writeable());
    }

    #[test]
    fn test_is_cq_writeable() {
        let running_flags = RunningFlags::new();
        assert!(running_flags.is_cq_writeable());
        running_flags
            .flag_bits
            .store(NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT, Ordering::Relaxed);
        assert!(!running_flags.is_cq_writeable());
    }

    #[test]
    fn test_get_and_make_not_writeable() {
        let running_flags = RunningFlags::new();
        assert!(running_flags.get_and_make_not_writeable());
        assert!(!running_flags.is_writeable());
        assert!(!running_flags.get_and_make_not_writeable());
        assert!(!running_flags.is_writeable());
    }

    #[test]
    fn test_make_logics_queue_error() {
        let running_flags = RunningFlags::new();
        running_flags.make_logics_queue_error();
        assert!(running_flags.is_logics_queue_error());
    }

    #[test]
    fn test_make_fenced() {
        let running_flags = RunningFlags::new();
        running_flags.make_fenced(true);
        assert!(running_flags.is_fenced());
        running_flags.make_fenced(false);
        assert!(!running_flags.is_fenced());
    }

    #[test]
    fn test_is_logics_queue_error() {
        let running_flags = RunningFlags::new();
        assert!(!running_flags.is_logics_queue_error());
        running_flags
            .flag_bits
            .store(WRITE_LOGICS_QUEUE_ERROR_BIT, Ordering::Relaxed);
        assert!(running_flags.is_logics_queue_error());
    }

    #[test]
    fn test_make_index_file_error() {
        let running_flags = RunningFlags::new();
        running_flags.make_index_file_error();
        assert!(running_flags.is_index_file_error());
    }

    #[test]
    fn test_is_index_file_error() {
        let running_flags = RunningFlags::new();
        assert!(!running_flags.is_index_file_error());
        running_flags
            .flag_bits
            .store(WRITE_INDEX_FILE_ERROR_BIT, Ordering::Relaxed);
        assert!(running_flags.is_index_file_error());
    }

    #[test]
    fn test_get_and_make_disk_full() {
        let running_flags = RunningFlags::new();
        assert!(running_flags.get_and_make_disk_full());
    }

    #[test]
    fn test_get_and_make_disk_ok() {
        let running_flags = RunningFlags::new();
        assert!(running_flags.get_and_make_disk_ok());
    }

    #[test]
    fn test_get_and_make_logic_disk_full() {
        let running_flags = RunningFlags::new();
        assert!(running_flags.get_and_make_logic_disk_full());
    }

    #[test]
    fn test_get_and_make_logic_disk_ok() {
        let running_flags = RunningFlags::new();
        assert!(running_flags.get_and_make_logic_disk_ok());
    }
}
