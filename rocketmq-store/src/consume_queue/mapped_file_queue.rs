use std::{fs, vec};

use tracing::{error, info};

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
        match std::path::Path::new(&self.store_path).read_dir() {
            Ok(dir) => {
                let mut files = vec![];
                for file in dir {
                    files.push(file.unwrap());
                }
                if files.is_empty() {
                    return true;
                }
                self.do_load(files).unwrap()
            }
            Err(_) => false,
        }
    }

    fn do_load(&mut self, files: Vec<fs::DirEntry>) -> anyhow::Result<bool> {
        // Ascending order sorting
        let sorted_files: Vec<_> = files.into_iter().collect();
        //sorted_files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        for (i, file) in sorted_files.iter().enumerate() {
            if file.path().is_dir() {
                continue;
            }

            if file.metadata()?.len() == 0 && i == sorted_files.len() - 1 {
                fs::remove_file(file.path())?;
                error!("{} size is 0, auto delete.", file.path().display());
                continue;
            }

            if file.metadata()?.len() != self.mapped_file_size {
                error!(
                    "{} length not matched message store config value, please check it manually",
                    file.path().display()
                );
                return Ok(false);
            }

            let mapped_file = DefaultMappedFile::new(
                file.path().into_os_string().to_string_lossy().to_string(),
                self.mapped_file_size,
            );
            // Set wrote, flushed, committed positions for mapped_file

            self.mapped_files.push(Box::new(mapped_file));
            info!("load {} OK", file.path().display());
        }

        Ok(true)
    }
}
