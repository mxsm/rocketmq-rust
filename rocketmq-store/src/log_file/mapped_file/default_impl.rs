use crate::log_file::mapped_file::MappedFile;

pub(crate) struct DefaultMappedFile {
    pub(crate) file_name: String,
    pub(crate) file_size: u64,
}

impl DefaultMappedFile {
    pub fn new(file_name: String, file_size: u64) -> Self {
        Self {
            file_name,
            file_size,
        }
    }
}

impl MappedFile for DefaultMappedFile {}
