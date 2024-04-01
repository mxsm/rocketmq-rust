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

use bytes::Bytes;
use memmap2::MmapMut;

use crate::base::message_result::AppendMessageResult;

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
            .create(true)
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
    pub fn append_data(&mut self, data: Bytes, sync: bool) -> bool {
        let current_pos = self.wrote_position.load(Ordering::SeqCst) as usize;
        if current_pos + data.len() > self.file_size as usize {
            return false;
        }
        let mut write_success =
            if let Ok(_) = (&mut self.mmapped_file[current_pos..]).write_all(data.as_ref()) {
                self.wrote_position
                    .store((current_pos + data.len()) as i32, Ordering::SeqCst);
                true
            } else {
                false
            };

        write_success &= if sync {
            match self.mmapped_file.flush() {
                Ok(_) => true,
                Err(_) => false,
            }
        } else {
            match self.mmapped_file.flush_async() {
                Ok(_) => true,
                Err(_) => false,
            }
        };

        write_success
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    pub fn test_local_mapped_file() {
        let mut file = LocalMappedFile::new(
            "C:\\Users\\ljbmx\\Desktop\\EventMesh\\0000".to_string(),
            1024,
        );
        let data = Bytes::from("ttt");
        assert!(file.append_data(data, true));
    }
}
