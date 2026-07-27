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

use std::cell::UnsafeCell;
use std::fs::File;
use std::io;
use std::sync::Arc;

use rocketmq_store_local::mapped_file::MappedMemory;

/// Heap-backed mapping used to run the production mapped-copy boundary under Miri.
#[derive(Clone)]
struct MiriMappedMemory {
    bytes: Arc<MiriMappedBytes>,
}

struct MiriMappedBytes(UnsafeCell<Box<[u8]>>);

// SAFETY: the allocation is stable, and the `MappedMemory` contract requires the mapped-file
// owner to serialize mutation against all reads.
unsafe impl Sync for MiriMappedBytes {}

impl MiriMappedMemory {
    fn new(len: usize) -> Self {
        Self {
            bytes: Arc::new(MiriMappedBytes(UnsafeCell::new(vec![0; len].into_boxed_slice()))),
        }
    }
}

// SAFETY: the heap allocation remains live for every clone, mutable access is sequenced by
// `DefaultMappedFile`, and regions are returned as independent immutable copies.
unsafe impl MappedMemory for MiriMappedMemory {
    type Region = Vec<u8>;

    fn map_mut(file: &File) -> io::Result<Self> {
        let len = usize::try_from(file.metadata()?.len())
            .map_err(|_| io::Error::other("mapped-file length does not fit usize"))?;
        Ok(Self::new(len))
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: callers obey the `MappedMemory` sequencing contract documented above.
        unsafe { &*self.bytes.0.get() }
    }

    fn as_mut_ptr(&self) -> *mut u8 {
        // SAFETY: callers obey the `MappedMemory` sequencing contract documented above.
        unsafe { (&mut *self.bytes.0.get()).as_mut_ptr() }
    }

    fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    fn flush_range(&self, _offset: usize, _len: usize) -> io::Result<()> {
        Ok(())
    }

    fn region(&self, offset: usize, len: usize) -> Self::Region {
        self.as_slice()[offset..offset + len].to_vec()
    }
}

#[test]
fn mapped_memory_copy_boundary_is_valid_under_miri() {
    let mapping = MiriMappedMemory::new(16);

    // SAFETY: this test has exclusive logical access to the mapping and the static source is a
    // disjoint allocation.
    unsafe {
        mapping
            .copy_from_slice(4, b"commit")
            .expect("checked mapped-memory copy");
    }

    assert_eq!(&mapping.as_slice()[..4], &[0; 4]);
    assert_eq!(&mapping.as_slice()[4..10], b"commit");
    assert_eq!(&mapping.as_slice()[10..], &[0; 6]);
}

#[test]
fn mapped_memory_copy_rejects_invalid_ranges_before_pointer_arithmetic() {
    let mapping = MiriMappedMemory::new(8);

    // SAFETY: the test has exclusive logical access and both sources are disjoint. The method must
    // reject each range before dereferencing its destination.
    unsafe {
        assert!(mapping.copy_from_slice(7, b"no").is_err());
        assert!(mapping.copy_from_slice(usize::MAX, b"x").is_err());
    }
    assert_eq!(mapping.as_slice(), &[0; 8]);
}
