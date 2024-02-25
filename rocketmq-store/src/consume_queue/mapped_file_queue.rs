use std::{fs, path::Path};

use log::warn;
use tracing::info;

use crate::{
    base::swappable::Swappable,
    log_file::mapped_file::{default_impl::DefaultMappedFile, MappedFile},
};

pub struct MappedFileQueue {
    pub(crate) store_path: String,

    pub(crate) mapped_file_size: u64,

    pub(crate) mapped_files: Vec<Box<dyn MappedFile>>,

    //AllocateMappedFileService  -- todo
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
                DefaultMappedFile::new(file.to_string_lossy().to_string(), self.mapped_file_size);
            // Set wrote, flushed, committed positions for mapped_file

            self.mapped_files.push(Box::new(mapped_file));
            info!("load {} OK", file.display());
        }

        true
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
            mapped_files: Vec::new(),
            flushed_where: 0,
            committed_where: 0,
            mapped_file_size: 1024,
            store_timestamp: 0,
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
            mapped_files: Vec::new(),
            flushed_where: 0,
            committed_where: 0,
            mapped_file_size: 0,
            store_timestamp: 0,
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
            mapped_files: Vec::new(),
            flushed_where: 0,
            committed_where: 0,
            mapped_file_size: 1024,
            store_timestamp: 0,
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
            mapped_files: Vec::new(),
            flushed_where: 0,
            committed_where: 0,
            mapped_file_size: 1024,
            store_timestamp: 0,
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
            mapped_files: Vec::new(),
            flushed_where: 0,
            committed_where: 0,
            mapped_file_size: 1024,
            store_timestamp: 0,
        };
        assert!(queue.load());
        assert_eq!(queue.mapped_files.len(), 1);
    }
}
