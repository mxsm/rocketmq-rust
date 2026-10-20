// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{self};
use std::path::Path;

use crate::LocalMetadataFileSystem;
use crate::MetadataFileSystem;
use crate::RuntimeError;
use crate::RuntimeOperation;
use crate::RuntimeResult;
use parking_lot::Mutex;
use tracing::warn;

static LOCK: Mutex<()> = Mutex::new(());

/// Reads a file into a UTF-8 string.
pub fn file_to_string(file_name: impl AsRef<Path>) -> RuntimeResult<String> {
    let path = file_name.as_ref();
    match std::fs::read_to_string(path) {
        Ok(sr) => Ok(sr),
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
            warn!("file not exist: {}", path.display());
            Ok(String::new())
        }
        Err(e) => Err(RuntimeError::io(RuntimeOperation::ReadFile, e)),
    }
}
/// Replaces a file with UTF-8 content, keeping the previous bytes in a sibling
/// `.bak` file.
///
/// Both files are written through the atomic metadata replacement protocol:
/// a synchronized temporary file, an atomic rename, and a parent-directory
/// synchronization where the platform supports it. Calls through this helper
/// are serialized within the process. The call blocks, so asynchronous code
/// runs it through a blocking lane such as
/// [`ChildServiceContext::metadata_io`](crate::ChildServiceContext::metadata_io),
/// or persists through a [`MetadataIoActor`](crate::MetadataIoActor).
///
/// # Errors
///
/// Returns an I/O-classified failure when the previous content cannot be read,
/// or a metadata persistence failure when either replacement fails.
pub fn string_to_file(str_content: &str, file_name: impl AsRef<Path>) -> RuntimeResult<()> {
    let _lock = LOCK.lock();

    let file_path = file_name.as_ref();
    let mut bak_file = file_path.as_os_str().to_os_string();
    bak_file.push(".bak");

    // Create a backup if the file exists
    if file_path.exists() {
        let previous =
            std::fs::read(file_path).map_err(|error| RuntimeError::io(RuntimeOperation::ReadFileBackup, error))?;
        LocalMetadataFileSystem
            .persist_atomic(Path::new(&bak_file), &previous)
            .map_err(metadata_io_error)?;
    }

    LocalMetadataFileSystem
        .persist_atomic(file_path, str_content.as_bytes())
        .map_err(metadata_io_error)
}

/// Creates the metadata io error value.
pub fn metadata_io_error(error: impl std::error::Error + Send + Sync + 'static) -> RuntimeError {
    RuntimeError::internal(RuntimeOperation::PersistMetadata, error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_to_string() {
        // Create a temporary file for testing
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        // Write some content to the file
        let content = "Hello, World!";
        std::fs::write(file_path, content).unwrap();

        // Call the file_to_string function
        let result = file_to_string(file_path);

        // Check if the result is Ok and contains the expected content
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), content);
    }

    #[test]
    fn test_string_to_file() {
        // Create a temporary file for testing
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        // Call the string_to_file function
        let content = "Hello, World!";
        let result = string_to_file(content, file_path);

        // Check if the result is Ok and the file was created with the expected content
        assert!(result.is_ok());
        assert_eq!(std::fs::read_to_string(file_path).unwrap(), content);
    }

    #[test]
    fn replacing_a_file_keeps_the_previous_content_as_a_backup() {
        let directory = tempfile::tempdir().unwrap();
        let file_path = directory.path().join("config.json");
        let backup_path = directory.path().join("config.json.bak");

        string_to_file("first", &file_path).unwrap();
        assert!(!backup_path.exists(), "a new file has no previous content to keep");

        string_to_file("second", &file_path).unwrap();
        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "second");
        assert_eq!(std::fs::read_to_string(&backup_path).unwrap(), "first");
    }

    #[test]
    fn test_file_to_string_not_found() {
        let result = file_to_string("/nonexistent/path/file.txt");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }
}
