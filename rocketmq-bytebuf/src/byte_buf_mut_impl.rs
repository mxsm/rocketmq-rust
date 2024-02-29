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

use bytes::{buf::UninitSlice, BufMut};

/// A byte buffer which is used to read bytes like java byte buffer.
pub struct ByteBufferMut {
    bytes: bytes::BytesMut,
    capacity: usize,
    write_index: usize,
    read_index: usize,
    remark_read_index: usize,
    remark_write_index: usize,
}

impl ByteBufferMut {
    pub fn new() -> ByteBufferMut {
        ByteBufferMut {
            bytes: bytes::BytesMut::new(),
            capacity: 0,
            write_index: 0,
            read_index: 0,
            remark_read_index: 0,
            remark_write_index: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> ByteBufferMut {
        ByteBufferMut {
            bytes: bytes::BytesMut::with_capacity(capacity),
            capacity,
            write_index: 0,
            read_index: 0,
            remark_read_index: 0,
            remark_write_index: 0,
        }
    }

    pub fn bytes(&self) -> &bytes::BytesMut {
        &self.bytes
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn write_index(&self) -> usize {
        self.write_index
    }
    pub fn read_index(&self) -> usize {
        self.read_index
    }
    pub fn remark_read_index(&self) -> usize {
        self.remark_read_index
    }
    pub fn remark_write_index(&self) -> usize {
        self.remark_write_index
    }

    pub fn set_bytes(&mut self, bytes: bytes::BytesMut) {
        self.bytes = bytes;
    }
    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
    }
    pub fn set_write_index(&mut self, write_index: usize) {
        self.write_index = write_index;
    }
    pub fn set_read_index(&mut self, read_index: usize) {
        self.read_index = read_index;
    }
    pub fn set_remark_read_index(&mut self, remark_read_index: usize) {
        self.remark_read_index = remark_read_index;
    }
    pub fn set_remark_write_index(&mut self, remark_write_index: usize) {
        self.remark_write_index = remark_write_index;
    }
}

impl Default for ByteBufferMut {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(unused_variables)]
unsafe impl BufMut for ByteBufferMut {
    fn remaining_mut(&self) -> usize {
        todo!()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        todo!()
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        todo!()
    }
}
