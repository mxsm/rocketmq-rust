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

use std::fmt;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::BytesMut;

struct PoolInner {
    max_bytes: usize,
    used_bytes: AtomicUsize,
    rejected: AtomicUsize,
}

#[derive(Clone)]
pub struct ByteBufferPool {
    inner: Arc<PoolInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferCapacityExhausted;

impl fmt::Display for BufferCapacityExhausted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("transport buffer byte capacity exhausted")
    }
}

impl std::error::Error for BufferCapacityExhausted {}

impl ByteBufferPool {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                max_bytes: max_bytes.max(1),
                used_bytes: AtomicUsize::new(0),
                rejected: AtomicUsize::new(0),
            }),
        }
    }

    pub fn try_acquire(&self, capacity: usize) -> Result<PooledBuffer, BufferCapacityExhausted> {
        if self
            .inner
            .used_bytes
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(capacity).filter(|next| *next <= self.inner.max_bytes)
            })
            .is_err()
        {
            self.inner.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(BufferCapacityExhausted);
        }
        Ok(PooledBuffer {
            pool: self.inner.clone(),
            reserved: capacity,
            buffer: BytesMut::with_capacity(capacity),
        })
    }

    pub fn used_bytes(&self) -> usize {
        self.inner.used_bytes.load(Ordering::Acquire)
    }

    pub fn rejected(&self) -> usize {
        self.inner.rejected.load(Ordering::Relaxed)
    }
}

pub struct PooledBuffer {
    pool: Arc<PoolInner>,
    reserved: usize,
    buffer: BytesMut,
}

impl Deref for PooledBuffer {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        self.pool.used_bytes.fetch_sub(self.reserved, Ordering::AcqRel);
    }
}
