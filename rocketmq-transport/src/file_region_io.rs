// Copyright 2026 The RocketMQ Rust Authors
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
use std::io;

pub(crate) const FILE_REGION_READ_CHUNK_BYTES: usize = 64 * 1024;

pub(crate) fn read_file_region_chunk(
    file: &File,
    buffer: &mut [u8],
    region_offset: u64,
    progress: u64,
) -> io::Result<usize> {
    let offset = region_offset
        .checked_add(progress)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "file-region read offset overflow"))?;
    positional_read(file, buffer, offset)
}

#[cfg(unix)]
fn positional_read(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;

    file.read_at(buffer, offset)
}

#[cfg(windows)]
fn positional_read(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;

    file.seek_read(buffer, offset)
}

#[cfg(not(any(unix, windows)))]
fn positional_read(_file: &File, _buffer: &mut [u8], _offset: u64) -> io::Result<usize> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "portable positional file reads are not implemented for this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_position_rejects_offset_overflow_before_file_access() {
        let file = tempfile::tempfile().expect("temporary file");
        let error = read_file_region_chunk(&file, &mut [0_u8; 1], u64::MAX, 1).expect_err("overflow must be rejected");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[cfg(not(any(unix, windows)))]
    #[test]
    fn unsupported_platform_returns_a_typed_io_error() {
        let file = tempfile::tempfile().expect("temporary file");
        let error =
            read_file_region_chunk(&file, &mut [0_u8; 1], 0, 0).expect_err("unsupported positional read must fail");

        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    }
}
