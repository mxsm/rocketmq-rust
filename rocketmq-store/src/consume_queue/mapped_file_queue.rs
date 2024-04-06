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

use std::{fs, path::Path};

use log::warn;
use tracing::info;

use crate::{
    base::swappable::Swappable,
    log_file::mapped_file::{
        default_impl::DefaultMappedFile, default_impl_refactor::LocalMappedFile, MappedFile,
    },
    services::allocate_mapped_file_service::AllocateMappedFileService,
};

#[derive(Default)]
pub struct MappedFileQueue {
    pub(crate) store_path: String,

    pub(crate) mapped_file_size: u64,

    pub(crate) mapped_files: Vec<LocalMappedFile>,

    pub(crate) allocate_mapped_file_service: AllocateMappedFileService,

    pub(crate) flushed_where: u64,

    pub(crate) committed_where: u64,

    pub(crate) store_timestamp: u64,
}

impl Swappable for MappedFileQueue {
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
}

impl MappedFileQueue {
    pub fn new(
        store_path: String,
        mapped_file_size: u64,
        allocate_mapped_file_service: AllocateMappedFileService,
    ) -> MappedFileQueue {
        MappedFileQueue {
            store_path,
            mapped_file_size,
            mapped_files: Vec::new(),
            allocate_mapped_file_service,
            flushed_where: 0,
            committed_where: 0,
            store_timestamp: 0,
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
                LocalMappedFile::new(file.to_string_lossy().to_string(), self.mapped_file_size);
            // Set wrote, flushed, committed positions for mapped_file

            self.mapped_files.push(mapped_file);
            info!("load {} OK", file.display());
        }

        true
    }

    pub fn get_last_mapped_file_mut(&mut self) -> Option<&mut LocalMappedFile> {
        if self.mapped_files.is_empty() {
            return None;
        }
        self.mapped_files.last_mut()
    }

    pub fn get_last_mapped_file_mut_start_offset(&mut self) -> Option<&mut LocalMappedFile> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

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
