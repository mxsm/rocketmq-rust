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

//! Positional reads and writes for timer storage files.
//!
//! The calls never move a shared cursor that other code relies on, so one handle can serve
//! appends and reads without a seek per operation.

use std::fs::File;

/// Writes all of `bytes` at `offset`.
pub(crate) fn write_all_at(file: &File, mut bytes: &[u8], mut offset: u64) -> std::io::Result<()> {
    while !bytes.is_empty() {
        match write_at(file, bytes, offset) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "timer storage file accepted no bytes",
                ));
            }
            Ok(written) => {
                bytes = &bytes[written..];
                offset += written as u64;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Fills `output` from `offset`, failing with `UnexpectedEof` when the file ends first.
pub(crate) fn read_exact_at(file: &File, mut output: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    while !output.is_empty() {
        match read_at(file, output, offset) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "timer storage file ended before the requested range",
                ));
            }
            Ok(read) => {
                output = &mut output[read..];
                offset += read as u64;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn write_at(file: &File, bytes: &[u8], offset: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;

    file.write_at(bytes, offset)
}

#[cfg(windows)]
fn write_at(file: &File, bytes: &[u8], offset: u64) -> std::io::Result<usize> {
    use std::os::windows::fs::FileExt;

    file.seek_write(bytes, offset)
}

#[cfg(not(any(unix, windows)))]
fn write_at(_file: &File, _bytes: &[u8], _offset: u64) -> std::io::Result<usize> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "positional timer storage writes are unsupported on this platform",
    ))
}

#[cfg(unix)]
fn read_at(file: &File, output: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;

    file.read_at(output, offset)
}

#[cfg(windows)]
fn read_at(file: &File, output: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::windows::fs::FileExt;

    file.seek_read(output, offset)
}

#[cfg(not(any(unix, windows)))]
fn read_at(_file: &File, _output: &mut [u8], _offset: u64) -> std::io::Result<usize> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "positional timer storage reads are unsupported on this platform",
    ))
}
