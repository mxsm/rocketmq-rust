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
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use log::warn;
use rocketmq_common::UtilAll::offset_to_file_name;
use tracing::info;

use crate::{
    log_file::mapped_file::{default_impl::DefaultMappedFile, MappedFile},
    services::allocate_mapped_file_service::AllocateMappedFileService,
};

#[derive(Default, Clone)]
pub struct MappedFileQueue {
    pub(crate) store_path: String,

    pub(crate) mapped_file_size: u64,
    //pub(crate) mapped_files: Arc<Mutex<Vec<LocalMappedFile>>>,
    //pub(crate) mapped_files: Vec<Arc<Mutex<LocalMappedFile>>>,
    //pub(crate) mapped_files: Vec<Arc<LocalMappedFile>>,
    pub(crate) mapped_files: Vec<Arc<DefaultMappedFile>>,
    //  pub(crate) mapped_files: Vec<LocalMappedFile>,
    pub(crate) allocate_mapped_file_service: Option<AllocateMappedFileService>,

    pub(crate) flushed_where: Arc<AtomicU64>,

    pub(crate) committed_where: Arc<AtomicU64>,

    pub(crate) store_timestamp: Arc<AtomicU64>,
}

/*impl Swappable for MappedFileQueue {
    fn swap_map(
        &self,
        _reserve_num: i32,
        _force_swap_interval_ms: i64,
        _normal_swap_interval_ms: i64,
    ) {
        todo!()
    }

    fn clean_swapped_map(&self, _force_clean_swap_interval_ms: i64) {
        todo!()
    }
}*/

impl MappedFileQueue {
    pub fn new(
        store_path: String,
        mapped_file_size: u64,
        allocate_mapped_file_service: Option<AllocateMappedFileService>,
    ) -> MappedFileQueue {
        MappedFileQueue {
            store_path,
            mapped_file_size,
            mapped_files: Vec::new(),
            allocate_mapped_file_service,
            flushed_where: Arc::new(AtomicU64::new(0)),
            committed_where: Arc::new(AtomicU64::new(0)),
            store_timestamp: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn load(&mut self) -> bool {
        //list dir files
        let dir = Path::new(&self.store_path);
        if let Ok(ls) = fs::read_dir(dir) {
            let files: Vec<_> = ls
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .collect();
            return self.do_load(files);
        }
        true
    }

    pub fn check_self(&self) {}

    pub fn do_load(&mut self, files: Vec<std::path::PathBuf>) -> bool {
        // Ascending order sorting
        let mut files = files;
        files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        let mut index = 0;
        for file in &files {
            index += 1;
            if file.is_dir() {
                continue;
            }

            if file.metadata().map(|metadata| metadata.len()).unwrap_or(0) == 0
                && files.len() == index
            {
                if let Err(err) = fs::remove_file(file) {
                    warn!("{} size is 0, auto delete. is_ok: {}", file.display(), err);
                }
                continue;
            }

            if file.metadata().map(|metadata| metadata.len()).unwrap_or(0) != self.mapped_file_size
            {
                warn!(
                    "{} {} length not matched message store config value, please check it manually",
                    file.display(),
                    file.metadata().map(|metadata| metadata.len()).unwrap_or(0)
                );
                return false;
            }

            let mapped_file =
                DefaultMappedFile::new(file.to_string_lossy().to_string(), self.mapped_file_size);
            // Set wrote, flushed, committed positions for mapped_file

            self.mapped_files.push(Arc::new(mapped_file));
            // self.mapped_files
            //     .push(mapped_file);
            info!("load {} OK", file.display());
        }

        true
    }

    // pub fn get_last_mapped_file_mut(&mut self) -> Option<&mut LocalMappedFile>{
    //     if self.mapped_files.is_empty() {
    //         return None;
    //     }
    //     self.mapped_files.last_mut()
    // }

    // pub fn get_last_mapped_file(&mut self) -> Option<&LocalMappedFile>{
    //     if self.mapped_files.is_empty() {
    //         return None;
    //     }
    //     self.mapped_files.last()
    // }

    pub fn get_last_mapped_file(&self) -> Option<Arc<DefaultMappedFile>> {
        if self.mapped_files.is_empty() {
            return None;
        }
        self.mapped_files.last().cloned()
    }

    pub fn get_last_mapped_file_mut_start_offset(
        &mut self,
        start_offset: u64,
        need_create: bool,
    ) -> Option<Arc<DefaultMappedFile>> {
        let mut create_offset = -1i64;
        let file_size = self.mapped_file_size as i64;
        let mapped_file_last = self.get_last_mapped_file();
        match mapped_file_last {
            None => {
                create_offset = start_offset as i64 - (start_offset as i64 % file_size);
            }
            Some(ref value) => {
                if value.is_full() {
                    create_offset = value.get_file_from_offset() as i64 + file_size
                }
            }
        }
        if create_offset != -1 && need_create {
            return self.try_create_mapped_file(create_offset as u64);
        }
        mapped_file_last
    }

    pub fn try_create_mapped_file(&mut self, create_offset: u64) -> Option<Arc<DefaultMappedFile>> {
        let next_file_path =
            PathBuf::from(self.store_path.clone()).join(offset_to_file_name(create_offset));
        let next_next_file_path = PathBuf::from(self.store_path.clone())
            .join(offset_to_file_name(create_offset + self.mapped_file_size));
        self.do_create_mapped_file(next_file_path, next_next_file_path)
    }

    fn do_create_mapped_file(
        &mut self,
        next_file_path: PathBuf,
        _next_next_file_path: PathBuf,
    ) -> Option<Arc<DefaultMappedFile>> {
        let mut mapped_file = match self.allocate_mapped_file_service {
            None => DefaultMappedFile::new(
                next_file_path.to_string_lossy().to_string(),
                self.mapped_file_size,
            ),
            Some(ref _value) => {
                unimplemented!()
            }
        };

        if self.mapped_files.is_empty() {
            mapped_file.set_first_create_in_queue(true);
        }
        let inner = Arc::new(mapped_file);
        self.mapped_files.push(inner.clone());
        Some(inner)
    }

    pub fn get_mapped_files(&self) -> Vec<Arc<DefaultMappedFile>> {
        self.mapped_files.to_vec()
    }

    pub fn get_mapped_files_size(&self) -> usize {
        self.mapped_files.len()
    }

    pub fn set_flushed_where(&mut self, flushed_where: i64) {
        self.flushed_where
            .store(flushed_where as u64, Ordering::SeqCst);
    }

    pub fn set_committed_where(&mut self, committed_where: i64) {
        self.committed_where
            .store(committed_where as u64, Ordering::SeqCst);
    }

    pub fn truncate_dirty_files(&mut self, offset: i64) {}

    pub fn get_max_offset(&self) -> i64 {
        /*let handle = Handle::current();
        let mapped_file = self.get_last_mapped_file();
        std::thread::spawn(move || {
            handle.block_on(async move {
                match mapped_file {
                    None => 0,
                    Some(value) => {
                        let file = value.lock().await;
                        file.get_file_from_offset() as i64 + file.get_read_position() as i64
                    }
                }
            })
        })
        .join()
        .unwrap()*/
        match self.get_last_mapped_file() {
            None => 0,
            Some(file) => file.get_file_from_offset() as i64 + file.get_read_position() as i64,
        }
    }

    pub fn delete_last_mapped_file(&self) {
        unimplemented!()
    }

    pub(crate) fn delete_expired_file(&self, files: Vec<Option<Arc<DefaultMappedFile>>>) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_empty_dir() {
        let mut queue = MappedFileQueue {
            store_path: String::from("/path/to/empty/dir"),
            ..MappedFileQueue::default()
        };
        assert!(queue.load());
        assert!(queue.mapped_files.is_empty());
    }

    #[test]
    fn test_load_with_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file1_path = temp_dir.path().join("1111");
        let file2_path = temp_dir.path().join("2222");
        fs::File::create(&file1_path).unwrap();
        fs::File::create(&file2_path).unwrap();

        let mut queue = MappedFileQueue {
            store_path: temp_dir.path().to_string_lossy().into_owned(),
            ..MappedFileQueue::default()
        };
        assert!(queue.load());
        assert_eq!(queue.mapped_files.len(), 1);
    }

    #[test]
    fn test_load_with_empty_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("1111");
        fs::File::create(&file_path).unwrap();

        let mut queue = MappedFileQueue {
            store_path: temp_dir.path().to_string_lossy().into_owned(),
            ..MappedFileQueue::default()
        };
        assert!(queue.load());
        assert!(queue.mapped_files.is_empty());
    }

    #[test]
    fn test_load_with_invalid_file_size() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("invalid_file.txt");
        fs::write(&file_path, "Some data").unwrap();

        let mut queue = MappedFileQueue {
            store_path: temp_dir.path().to_string_lossy().into_owned(),
            ..MappedFileQueue::default()
        };
        assert!(!queue.load());
        assert!(queue.mapped_files.is_empty());
    }

    #[test]
    fn test_load_with_correct_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("1111");
        fs::write(&file_path, vec![0u8; 1024]).unwrap();

        let mut queue = MappedFileQueue {
            store_path: temp_dir.path().to_string_lossy().into_owned(),
            mapped_file_size: 1024,
            ..MappedFileQueue::default()
        };
        assert!(queue.load());
        assert_eq!(queue.mapped_files.len(), 1);
    }
}
