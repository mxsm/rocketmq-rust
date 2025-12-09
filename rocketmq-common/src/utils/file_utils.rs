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

use std::fs::File;
use std::io::Write;
use std::io::{self};
use std::path::Path;

use parking_lot::Mutex;
#[cfg(feature = "async_fs")]
use tokio::io::AsyncReadExt;
#[cfg(feature = "async_fs")]
use tokio::io::AsyncWriteExt;
use tracing::warn;

static LOCK: Mutex<()> = Mutex::new(());

#[cfg(feature = "async_fs")]
static ASYNC_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub fn file_to_string(file_name: &str) -> Result<String, io::Error> {
    let path = Path::new(file_name);
    if !path.exists() {
        warn!("file not exist: {}", file_name);
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("File not found: {}", file_name),
        ));
    }
    std::fs::read_to_string(path)
}

pub fn string_to_file(str_content: &str, file_name: &str) -> io::Result<()> {
    let _lock = LOCK.lock();

    let file_path = Path::new(file_name);
    let bak_file = format!("{file_name}.bak");

    // Create a backup if the file exists
    if file_path.exists() {
        std::fs::copy(file_name, &bak_file)?;
    }

    // Write new content to the file
    string_to_file_not_safe(str_content, file_name)?;
    Ok(())
}

fn string_to_file_not_safe(str_content: &str, file_name: &str) -> io::Result<()> {
    let path = Path::new(file_name);

    // Create parent directories if they don't exist
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = File::create(file_name)?;
    let mut writer = io::BufWriter::new(file);
    writer.write_all(str_content.as_bytes())?;
    writer.flush()?;
    Ok(())
}

#[cfg(feature = "async_fs")]
pub async fn file_to_string_async(file_name: &str) -> Result<String, io::Error> {
    let path = Path::new(file_name);
    if !tokio::fs::try_exists(path).await.unwrap_or(false) {
        warn!("file not exist: {}", file_name);
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("File not found: {}", file_name),
        ));
    }
    tokio::fs::read_to_string(path).await
}

#[cfg(feature = "async_fs")]
pub async fn string_to_file_async(str_content: &str, file_name: &str) -> io::Result<()> {
    let _lock = ASYNC_LOCK.lock().await;

    let file_path = Path::new(file_name);
    let bak_file = format!("{file_name}.bak");

    // Create a backup if the file exists
    if tokio::fs::try_exists(file_path).await.unwrap_or(false) {
        tokio::fs::copy(file_name, &bak_file).await?;
    }

    // Write new content to the file
    string_to_file_not_safe_async(str_content, file_name).await?;
    Ok(())
}

#[cfg(feature = "async_fs")]
async fn string_to_file_not_safe_async(str_content: &str, file_name: &str) -> io::Result<()> {
    let path = Path::new(file_name);

    // Create parent directories if they don't exist
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut file = tokio::fs::File::create(file_name).await?;
    file.write_all(str_content.as_bytes()).await?;
    file.flush().await?;
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
}
