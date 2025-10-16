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
use std::io::Read;
use std::io::Write;
use std::io::{self};
use std::path::Path;
use std::path::PathBuf;

use parking_lot::Mutex;
#[cfg(feature = "async_fs")]
use tokio::io::AsyncReadExt;
use tracing::warn;

static LOCK: Mutex<()> = Mutex::new(());

pub fn file_to_string(file_name: &str) -> Result<String, io::Error> {
    if !PathBuf::from(file_name).exists() {
        warn!("file not exist:{}", file_name);
        return Ok("".to_string());
    }
    let file = File::open(file_name)?;
    file_to_string_impl(&file)
}

pub fn file_to_string_impl(file: &File) -> Result<String, io::Error> {
    let file_length = file.metadata()?.len() as usize;
    let mut data = vec![0; file_length];
    let result = file.take(file_length as u64).read_exact(&mut data);

    match result {
        Ok(_) => Ok(String::from_utf8_lossy(&data).to_string()),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Failed to read file",
        )),
    }
}

pub fn string_to_file(str_content: &str, file_name: &str) -> io::Result<()> {
    let lock = LOCK.lock(); //todo enhancement: not use global lock

    let bak_file = format!("{file_name}.bak");

    // Read previous content and create a backup
    if let Ok(prev_content) = file_to_string(file_name) {
        string_to_file_not_safe(&prev_content, &bak_file)?;
    }

    // Write new content to the file
    string_to_file_not_safe(str_content, file_name)?;
    drop(lock);
    Ok(())
}

fn string_to_file_not_safe(str_content: &str, file_name: &str) -> io::Result<()> {
    // Create parent directories if they don't exist
    if let Some(parent) = Path::new(file_name).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(file_name)?;

    write_string_to_file(&file, str_content, "UTF-8")
}

fn write_string_to_file(file: &File, data: &str, _encoding: &str) -> io::Result<()> {
    let mut os = io::BufWriter::new(file);

    os.write_all(data.as_bytes())?;

    Ok(())
}

#[cfg(feature = "async_fs")]
pub async fn file_to_string_async(file_name: &str) -> Result<String, io::Error> {
    if !tokio::fs::try_exists(file_name).await.unwrap_or(false) {
        warn!("file not exist:{}", file_name);
        return Ok("".to_string());
    }
    let mut file = tokio::fs::File::open(file_name).await?;
    file_to_string_impl_async(&mut file).await
}

#[cfg(feature = "async_fs")]
async fn file_to_string_impl_async(file: &mut tokio::fs::File) -> Result<String, io::Error> {
    let file_length = file.metadata().await?.len() as usize;
    let mut data = vec![0; file_length];
    let result = file.read_exact(&mut data).await;

    match result {
        Ok(_) => Ok(String::from_utf8_lossy(&data).to_string()),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Failed to read file",
        )),
    }
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
}
