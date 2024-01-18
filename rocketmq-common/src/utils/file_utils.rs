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
    fs::File,
    io::{self, Read, Write},
    path::Path,
};

pub fn file_to_string(file_name: &str) -> Result<String, io::Error> {
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
    let bak_file = format!("{}.bak", file_name);

    // Read previous content and create a backup
    if let Ok(prev_content) = file_to_string(file_name) {
        string_to_file_not_safe(&prev_content, &bak_file)?;
    }

    // Write new content to the file
    string_to_file_not_safe(str_content, file_name)?;

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
