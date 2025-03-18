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
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::error;

use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;

pub struct AllocateMappedFileService {
    tx: Sender<AllocateRequest>,
    rx: Arc<Mutex<Receiver<AllocateRequest>>>,
    request_table: Arc<parking_lot::Mutex<HashMap<String, AllocateRequest>>>,
}

impl AllocateMappedFileService {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            tx,
            rx: Arc::new(parking_lot::Mutex::new(rx)),
            request_table: Arc::new(Default::default()),
        }
    }
}

impl Default for AllocateMappedFileService {
    fn default() -> Self {
        Self::new()
    }
}

impl AllocateMappedFileService {
    pub fn put_request_and_return_mapped_file(
        &self,
        next_file_path: String,
        next_next_file_path: String,
        file_size: i32,
    ) -> DefaultMappedFile {
        unimplemented!()
    }

    pub fn start(&self) {
        error!("AllocateMappedFileService start failed, not implement yet");
    }

    pub fn shutdown(&self) {
        error!("AllocateMappedFileService shutdown failed, not implement yet");
    }
}

struct AllocateRequest {
    file_path: String,
    file_size: i32,
    mapped_file: Option<DefaultMappedFile>,
}

impl Display for AllocateRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AllocateRequest[file_path={},file_size={}]",
            self.file_path, self.file_size
        )
    }
}

impl PartialEq<Self> for AllocateRequest {
    fn eq(&self, other: &Self) -> bool {
        self.file_path == other.file_path && self.file_size == other.file_size
    }
}

impl Eq for AllocateRequest {}
