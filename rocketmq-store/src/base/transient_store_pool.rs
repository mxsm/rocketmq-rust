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

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_error::RocketMQResult;
use tracing::warn;

use crate::utils::ffi::mlock;
use crate::utils::ffi::munlock;

#[derive(Clone)]
pub struct TransientStorePool {
    pool_size: usize,
    file_size: usize,
    available_buffers: Arc<parking_lot::Mutex<VecDeque<Vec<u8>>>>,
    is_real_commit: Arc<parking_lot::Mutex<bool>>,
}

impl TransientStorePool {
    pub fn new(pool_size: usize, file_size: usize) -> Self {
        let available_buffers = Arc::new(Mutex::new(VecDeque::with_capacity(pool_size)));
        let is_real_commit = Arc::new(Mutex::new(true));
        TransientStorePool {
            pool_size,
            file_size,
            available_buffers,
            is_real_commit,
        }
    }

    pub fn init(&self) -> RocketMQResult<()> {
        let mut available_buffers = self.available_buffers.lock();
        for _ in 0..self.pool_size {
            let buffer = vec![0u8; self.file_size];
            mlock(buffer.as_ptr(), self.file_size)?;
            available_buffers.push_back(buffer);
        }
        Ok(())
    }

    pub fn destroy(&self) -> RocketMQResult<()> {
        let mut available_buffers = self.available_buffers.lock();
        for available_buffer in available_buffers.drain(0..) {
            munlock(available_buffer.as_ptr(), self.file_size)?;
        }
        Ok(())
    }

    pub fn return_buffer(&self, buffer: Vec<u8>) {
        let mut available_buffers = self.available_buffers.lock();
        available_buffers.push_front(buffer);
    }

    pub fn borrow_buffer(&self) -> Option<Vec<u8>> {
        let mut available_buffers = self.available_buffers.lock();
        let buffer = available_buffers.pop_front();
        if available_buffers.len() < self.pool_size / 10 * 4 {
            warn!("TransientStorePool only remain {} sheets.", available_buffers.len());
        }
        buffer
    }

    pub fn available_buffer_nums(&self) -> usize {
        let available_buffers = self.available_buffers.lock();
        available_buffers.len()
    }

    pub fn is_real_commit(&self) -> bool {
        let is_real_commit = self.is_real_commit.lock();
        *is_real_commit
    }

    pub fn set_real_commit(&self, real_commit: bool) {
        let mut is_real_commit = self.is_real_commit.lock();
        *is_real_commit = real_commit;
    }
}
