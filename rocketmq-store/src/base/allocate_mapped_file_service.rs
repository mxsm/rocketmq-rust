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
    collections::HashMap,
    fmt::{Display, Formatter},
    sync::{
        mpsc,
        mpsc::{Receiver, Sender},
        Arc,
    },
};

use parking_lot::Mutex;
use rocketmq_common::common::thread::thread_service::ThreadService;
use tracing::{error, warn};

use crate::log_file::mapped_file::default_impl_refactor::LocalMappedFile;

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
    ) -> LocalMappedFile {
        unimplemented!()
    }
}

impl ThreadService for AllocateMappedFileService {
    fn start(&self) {
        let service_name = self.get_service_name();
        let builder = std::thread::Builder::new().name(service_name);
        let rx = self.rx.clone();
        let request_table = self.request_table.clone();
        let _ = builder
            .spawn(move || loop {
                match rx.lock().recv() {
                    Ok(ref req) => {
                        let guard = request_table.lock();
                        let expected_request = guard.get(&req.file_path);
                        if expected_request.is_none() {
                            warn!(
                                "this mmap request expired, maybe cause timeout {} {}",
                                req.file_path, req.file_size,
                            );
                            continue;
                        }
                        if expected_request.unwrap() != req {
                            warn!(
                                "this mmap request expired, maybe cause timeout {} {}",
                                req.file_path, req.file_size,
                            );
                            continue;
                        }
                        if req.mapped_file.is_none() {}
                    }
                    Err(err) => {
                        error!("{}", err)
                    }
                }
            })
            .unwrap()
            .join();
    }

    fn shutdown(&self) {}

    fn get_service_name(&self) -> String {
        "AllocateMappedFileService".to_string()
    }
}

struct AllocateRequest {
    file_path: String,
    file_size: i32,
    mapped_file: Option<LocalMappedFile>,
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
