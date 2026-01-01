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

use std::fs::File;
use std::io::Write;
use std::io::{self};
use std::path::Path;

use parking_lot::Mutex;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
#[cfg(feature = "async_fs")]
use tokio::io::AsyncReadExt;
#[cfg(feature = "async_fs")]
use tokio::io::AsyncWriteExt;
use tracing::warn;

static LOCK: Mutex<()> = Mutex::new(());

const FILE_EMPTY: &str = "";

#[cfg(feature = "async_fs")]
static ASYNC_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub fn file_to_string(file_name: impl AsRef<Path>) -> RocketMQResult<String> {
    let path = file_name.as_ref();
    match std::fs::read_to_string(path) {
        Ok(sr) => Ok(sr),
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
            warn!("file not exist: {}", path.display());
            Ok(String::new())
        }
        Err(e) => Err(RocketMQError::IO(e)),
    }
}
pub fn string_to_file(str_content: &str, file_name: impl AsRef<Path>) -> RocketMQResult<()> {
    let _lock = LOCK.lock();

    let file_path = file_name.as_ref();
    let mut bak_file = file_path.as_os_str().to_os_string();
    bak_file.push(".bak");

    // Create a backup if the file exists
    if file_path.exists() {
        std::fs::copy(file_path, &bak_file)?;
    }

    // Write new content to the file
    string_to_file_not_safe(str_content, file_path)?;
    Ok(())
}

fn string_to_file_not_safe(str_content: &str, file_name: impl AsRef<Path>) -> RocketMQResult<()> {
    let path = file_name.as_ref();

    // Create parent directories if they don't exist
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = File::create(path)?;
    let mut writer = io::BufWriter::new(file);
    writer.write_all(str_content.as_bytes())?;
    writer.flush()?;
    Ok(())
}

#[cfg(feature = "async_fs")]
pub async fn file_to_string_async(file_name: impl AsRef<Path>) -> RocketMQResult<String> {
    let path = file_name.as_ref();
    if !tokio::fs::try_exists(path).await? {
        warn!("file not exist: {}", path.display());
        return Err(RocketMQError::IO(io::Error::new(
            io::ErrorKind::NotFound,
            format!("File not found: {}", path.display()),
        )));
    }
    tokio::fs::read_to_string(path).await.map_err(RocketMQError::IO)
}

#[cfg(feature = "async_fs")]
pub async fn string_to_file_async(str_content: &str, file_name: impl AsRef<Path>) -> RocketMQResult<()> {
    let _lock = ASYNC_LOCK.lock().await;

    let file_path = file_name.as_ref();
    let mut bak_file = file_path.as_os_str().to_os_string();
    bak_file.push(".bak");

    // Create a backup if the file exists
    if tokio::fs::try_exists(file_path).await.map_err(RocketMQError::IO)? {
        tokio::fs::copy(file_path, &bak_file).await.map_err(RocketMQError::IO)?;
    }

    // Write new content to the file
    string_to_file_not_safe_async(str_content, file_path).await?;
    Ok(())
}

#[cfg(feature = "async_fs")]
async fn string_to_file_not_safe_async(str_content: &str, file_name: impl AsRef<Path>) -> RocketMQResult<()> {
    let path = file_name.as_ref();

    // Create parent directories if they don't exist
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let file = tokio::fs::File::create(file_name).await?;
    let mut writer = tokio::io::BufWriter::new(file);
    writer.write_all(str_content.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
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

    #[cfg(feature = "async_fs")]
    #[tokio::test]
    async fn test_file_to_string_async() {
        // Create a temporary file for testing
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        // Write some content to the file
        let content = "Hello, Async World!";
        tokio::fs::write(file_path, content).await.unwrap();

        // Call the file_to_string_async function
        let result = file_to_string_async(file_path).await;

        // Check if the result is Ok and contains the expected content
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), content);
    }

    #[cfg(feature = "async_fs")]
    #[tokio::test]
    async fn test_string_to_file_async() {
        // Create a temporary file for testing
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        // Call the string_to_file_async function
        let content = "Hello, Async World!";
        let result = string_to_file_async(content, file_path).await;

        // Check if the result is Ok and the file was created with the expected content
        assert!(result.is_ok());
        assert_eq!(tokio::fs::read_to_string(file_path).await.unwrap(), content);
    }

    #[test]
    fn test_file_to_string_not_found() {
        let result = file_to_string("/nonexistent/path/file.txt");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    #[cfg(feature = "async_fs")]
    #[tokio::test]
    async fn test_file_to_string_async_not_found() {
        let result = file_to_string_async("/nonexistent/path/file.txt").await;
        assert!(result.is_err());
        if let Err(RocketMQError::IO(io_err)) = result {
            assert_eq!(io_err.kind(), io::ErrorKind::NotFound);
        } else {
            panic!("Expected RocketMQError::IO with NotFound");
        }
    }
}
