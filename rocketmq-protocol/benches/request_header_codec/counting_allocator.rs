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

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Copy)]
pub struct AllocationSnapshot {
    pub allocations: u64,
    pub allocated_bytes: u64,
}

pub struct CountingAllocator<A> {
    inner: A,
    enabled: AtomicBool,
    allocations: AtomicU64,
    allocated_bytes: AtomicU64,
}

impl<A> CountingAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self {
            inner,
            enabled: AtomicBool::new(false),
            allocations: AtomicU64::new(0),
            allocated_bytes: AtomicU64::new(0),
        }
    }

    pub fn measure<T>(&self, operation: impl FnOnce() -> T) -> (T, AllocationSnapshot) {
        assert!(
            !self.enabled.swap(false, Ordering::SeqCst),
            "nested allocation measurement"
        );
        self.allocations.store(0, Ordering::SeqCst);
        self.allocated_bytes.store(0, Ordering::SeqCst);
        self.enabled.store(true, Ordering::SeqCst);
        let result = operation();
        self.enabled.store(false, Ordering::SeqCst);
        let snapshot = AllocationSnapshot {
            allocations: self.allocations.load(Ordering::SeqCst),
            allocated_bytes: self.allocated_bytes.load(Ordering::SeqCst),
        };
        (result, snapshot)
    }

    #[inline]
    fn record(&self, bytes: usize) {
        if self.enabled.load(Ordering::Relaxed) {
            self.allocations.fetch_add(1, Ordering::Relaxed);
            self.allocated_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }
}

// SAFETY: every allocation operation is delegated unchanged to the wrapped
// allocator. The counters do not affect pointer ownership or layout validity.
unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.record(layout.size());
        // SAFETY: the caller supplies the GlobalAlloc layout contract and it is
        // forwarded unchanged to the wrapped allocator.
        unsafe { self.inner.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        self.record(layout.size());
        // SAFETY: the caller supplies the GlobalAlloc layout contract and it is
        // forwarded unchanged to the wrapped allocator.
        unsafe { self.inner.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are forwarded to the allocator that
        // produced the allocation.
        unsafe { self.inner.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        self.record(new_size);
        // SAFETY: the pointer, old layout, and requested size are forwarded
        // unchanged to the wrapped allocator.
        unsafe { self.inner.realloc(ptr, layout, new_size) }
    }
}
