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

use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;

use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;

/// Represents the result of selecting a mapped buffer.
pub struct SelectMappedBufferResult {
    /// The start offset.
    pub start_offset: u64,
    /// The size.
    pub size: i32,
    /// The mapped file.
    pub mapped_file: Option<Arc<DefaultMappedFile>>,
    /// Whether the buffer is in cache.
    pub is_in_cache: bool,
}

impl Default for SelectMappedBufferResult {
    fn default() -> Self {
        Self {
            start_offset: 0,
            size: 0,
            mapped_file: None,
            is_in_cache: true,
        }
    }
}

impl SelectMappedBufferResult {
    /// Returns the buffer.
    pub fn get_buffer(&self) -> &[u8] {
        self.mapped_file.as_ref().unwrap().get_mapped_file()
            [self.start_offset as usize..(self.start_offset + self.size as u64) as usize]
            .as_ref()
    }

    pub fn get_buffer_slice_mut(&self) -> &mut [u8] {
        self.mapped_file.as_ref().unwrap().get_mapped_file_mut()
            [self.start_offset as usize..(self.start_offset + self.size as u64) as usize]
            .as_mut()
    }

    pub fn get_bytes(&self) -> Option<Bytes> {
        if self.size <= 0 || self.mapped_file.is_none() {
            return None;
        }
        Some(BytesMut::from(self.get_buffer()).freeze())
    }

    pub fn is_in_mem(&self) -> bool {
        match self.mapped_file.as_ref() {
            None => true,
            Some(inner) => {
                let pos = self.start_offset - inner.get_file_from_offset();
                inner.is_loaded(pos as i64, self.size as usize)
            }
        }
    }

    pub fn release(&mut self) {
        if let Some(mapped_file) = self.mapped_file.take() {
            mapped_file.release()
        }
    }
}
