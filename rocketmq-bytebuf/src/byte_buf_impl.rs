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

use std::io::IoSlice;

use bytes::{
    buf::{Chain, Reader, Take},
    Buf, Bytes,
};

/// A byte buffer which is used to read bytes like java byte buffer.
pub struct ByteBuffer {
    bytes: bytes::Bytes,
    capacity: usize,
    read_index: usize,
    write_index: usize,
    remark_read_index: usize,
    remark_write_index: usize,
}

impl ByteBuffer {
    pub fn new() -> ByteBuffer {
        ByteBuffer {
            bytes: bytes::Bytes::new(),
            capacity: 0,
            read_index: 0,
            write_index: 0,
            remark_read_index: 0,
            remark_write_index: 0,
        }
    }

    pub fn from_static(bytes: &'static [u8]) -> ByteBuffer {
        ByteBuffer {
            bytes: bytes::Bytes::from_static(bytes),
            capacity: 0,
            read_index: 0,
            write_index: 0,
            remark_read_index: 0,
            remark_write_index: 0,
        }
    }

    pub fn bytes(&self) -> &bytes::Bytes {
        &self.bytes
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn read_index(&self) -> usize {
        self.read_index
    }
    pub fn write_index(&self) -> usize {
        self.write_index
    }
    pub fn remark_read_index(&self) -> usize {
        self.remark_read_index
    }
    pub fn remark_write_index(&self) -> usize {
        self.remark_write_index
    }

    pub fn set_bytes(&mut self, bytes: bytes::Bytes) {
        self.bytes = bytes;
    }
    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
    }
    pub fn set_read_index(&mut self, read_index: usize) {
        self.read_index = read_index;
    }
    pub fn set_write_index(&mut self, write_index: usize) {
        self.write_index = write_index;
    }
    pub fn set_remark_read_index(&mut self, remark_read_index: usize) {
        self.remark_read_index = remark_read_index;
    }
    pub fn set_remark_write_index(&mut self, remark_write_index: usize) {
        self.remark_write_index = remark_write_index;
    }
}

impl Default for ByteBuffer {
    fn default() -> Self {
        ByteBuffer::new()
    }
}

#[allow(unused_variables)]
impl Buf for ByteBuffer {
    fn remaining(&self) -> usize {
        todo!()
    }

    fn chunk(&self) -> &[u8] {
        todo!()
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        todo!()
    }

    fn advance(&mut self, cnt: usize) {
        todo!()
    }

    fn has_remaining(&self) -> bool {
        todo!()
    }

    fn copy_to_slice(&mut self, dst: &mut [u8]) {
        todo!()
    }

    fn get_u8(&mut self) -> u8 {
        todo!()
    }

    fn get_i8(&mut self) -> i8 {
        todo!()
    }

    fn get_u16(&mut self) -> u16 {
        todo!()
    }

    fn get_u16_le(&mut self) -> u16 {
        todo!()
    }

    fn get_u16_ne(&mut self) -> u16 {
        todo!()
    }

    fn get_i16(&mut self) -> i16 {
        todo!()
    }

    fn get_i16_le(&mut self) -> i16 {
        todo!()
    }

    fn get_i16_ne(&mut self) -> i16 {
        todo!()
    }

    fn get_u32(&mut self) -> u32 {
        todo!()
    }

    fn get_u32_le(&mut self) -> u32 {
        todo!()
    }

    fn get_u32_ne(&mut self) -> u32 {
        todo!()
    }

    fn get_i32(&mut self) -> i32 {
        todo!()
    }

    fn get_i32_le(&mut self) -> i32 {
        todo!()
    }

    fn get_i32_ne(&mut self) -> i32 {
        todo!()
    }

    fn get_u64(&mut self) -> u64 {
        todo!()
    }

    fn get_u64_le(&mut self) -> u64 {
        todo!()
    }

    fn get_u64_ne(&mut self) -> u64 {
        todo!()
    }

    fn get_i64(&mut self) -> i64 {
        todo!()
    }

    fn get_i64_le(&mut self) -> i64 {
        todo!()
    }

    fn get_i64_ne(&mut self) -> i64 {
        todo!()
    }

    fn get_u128(&mut self) -> u128 {
        todo!()
    }

    fn get_u128_le(&mut self) -> u128 {
        todo!()
    }

    fn get_u128_ne(&mut self) -> u128 {
        todo!()
    }

    fn get_i128(&mut self) -> i128 {
        todo!()
    }

    fn get_i128_le(&mut self) -> i128 {
        todo!()
    }

    fn get_i128_ne(&mut self) -> i128 {
        todo!()
    }

    fn get_uint(&mut self, nbytes: usize) -> u64 {
        todo!()
    }

    fn get_uint_le(&mut self, nbytes: usize) -> u64 {
        todo!()
    }

    fn get_uint_ne(&mut self, nbytes: usize) -> u64 {
        todo!()
    }

    fn get_int(&mut self, nbytes: usize) -> i64 {
        todo!()
    }

    fn get_int_le(&mut self, nbytes: usize) -> i64 {
        todo!()
    }

    fn get_int_ne(&mut self, nbytes: usize) -> i64 {
        todo!()
    }

    fn get_f32(&mut self) -> f32 {
        todo!()
    }

    fn get_f32_le(&mut self) -> f32 {
        todo!()
    }

    fn get_f32_ne(&mut self) -> f32 {
        todo!()
    }

    fn get_f64(&mut self) -> f64 {
        todo!()
    }

    fn get_f64_le(&mut self) -> f64 {
        todo!()
    }

    fn get_f64_ne(&mut self) -> f64 {
        todo!()
    }

    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        todo!()
    }

    fn take(self, limit: usize) -> Take<Self>
    where
        Self: Sized,
    {
        todo!()
    }

    fn chain<U: Buf>(self, next: U) -> Chain<Self, U>
    where
        Self: Sized,
    {
        todo!()
    }

    fn reader(self) -> Reader<Self>
    where
        Self: Sized,
    {
        todo!()
    }
}
