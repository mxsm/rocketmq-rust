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

use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicI32, Ordering},
};

use bytes::{Bytes, BytesMut};
use memmap2::MmapMut;
use rocketmq_common::common::message::message_single::MessageExtBrokerInner;
use tracing::error;

use crate::base::{
    append_message_callback::AppendMessageCallback, message_result::AppendMessageResult,
    put_message_context::PutMessageContext,
};

pub struct LocalMappedFile {
    //file information
    file_name: String,
    file_size: u64,
    file: File,
    //file data information
    wrote_position: AtomicI32,
    committed_position: AtomicI32,
    flushed_position: AtomicI32,

    file_from_offset: u64,

    mmapped_file: MmapMut,
}

impl LocalMappedFile {
    pub fn new(file_name: String, file_size: u64) -> Self {
        let path_buf = PathBuf::from(file_name.clone());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            //.create(true)
            .open(&path_buf)
            .unwrap();
        file.set_len(file_size).unwrap();

        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        Self {
            file_name,
            file_size,
            file,
            wrote_position: AtomicI32::new(0),
            committed_position: AtomicI32::new(0),
            flushed_position: AtomicI32::new(0),
            file_from_offset: path_buf
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string()
                .parse::<u64>()
                .unwrap(),
            mmapped_file: mmap,
        }
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }
    pub fn wrote_position(&self) -> i32 {
        self.wrote_position.load(Ordering::Relaxed)
    }
    pub fn committed_position(&self) -> i32 {
        self.committed_position.load(Ordering::Relaxed)
    }
    pub fn flushed_position(&self) -> i32 {
        self.flushed_position.load(Ordering::Relaxed)
    }

    pub fn file_from_offset(&self) -> u64 {
        self.file_from_offset
    }
}

impl LocalMappedFile {
    pub fn append_data(&mut self, data: BytesMut, sync: bool) -> bool {
        let current_pos = self.wrote_position.load(Ordering::SeqCst) as usize;
        if current_pos + data.len() > self.file_size as usize {
            return false;
        }
        let mut write_success = if (&mut self.mmapped_file[current_pos..])
            .write_all(data.as_ref())
            .is_ok()
        {
            self.wrote_position
                .store((current_pos + data.len()) as i32, Ordering::SeqCst);
            true
        } else {
            false
        };

        write_success &= if sync {
            self.mmapped_file.flush().is_ok()
        } else {
            self.mmapped_file.flush_async().is_ok()
        };

        write_success
    }

    pub fn append_message(
        &mut self,
        message: MessageExtBrokerInner,
        message_callback: impl AppendMessageCallback,
        put_message_context: &mut PutMessageContext,
    ) -> AppendMessageResult {
        let mut message = message;
        let current_pos = self.wrote_position.load(Ordering::Relaxed) as u64;
        if current_pos < self.file_size {
            unimplemented!()
        }
        error!(
            "MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}",
            current_pos, self.file_size
        );
        let mut message_callback = message_callback;
        let append_message_result = message_callback.do_append(
            self.file_from_offset() as i64,
            current_pos as i64,
            (self.file_size - current_pos) as i32,
            &mut message,
            put_message_context,
        );
        self.append_data(message.encoded_buff.clone(), false);
        append_message_result
    }

    pub fn get_bytes(&mut self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        if pos + size > self.file_size as usize {
            return None;
        }
        Some(Bytes::copy_from_slice(&self.mmapped_file[pos..pos + size]))
    }
}

#[cfg(test)]
mod tests {

    // use super::*;

    #[test]
    pub fn test_local_mapped_file() {
        // let mut file = LocalMappedFile::new(
        //     "C:\\Users\\ljbmx\\Desktop\\EventMesh\\0000".to_string(),
        //     1024,
        // );
        // let data = Bytes::from("ttt");
        // assert!(file.append_data(data, true));
    }
}
