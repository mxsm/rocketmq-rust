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

use std::io;
use std::os::fd::AsFd;
use std::os::fd::AsRawFd;
use std::os::fd::BorrowedFd;
use std::os::unix::net::UnixStream;

use tokio::io::Interest;
use tokio::net::tcp::OwnedWriteHalf;

use crate::file_region::FileRegion;

const MAX_SENDFILE_COUNT: usize = 0x7fff_f000;

pub(crate) fn is_eligible(region: &FileRegion) -> bool {
    region.is_regular_file()
        && libc::off_t::try_from(region.offset()).is_ok()
        && region
            .offset()
            .checked_add(region.len())
            .is_some_and(|end| libc::off_t::try_from(end).is_ok())
}

pub(crate) fn probe_file_region(region: &FileRegion) -> io::Result<bool> {
    if let Some(supported) = region.cached_sendfile_support() {
        return Ok(supported);
    }
    let (writer, _reader) = UnixStream::pair()?;
    let mut offset = libc::off_t::try_from(region.offset())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file-region offset exceeds off_t"))?;
    match try_sendfile(writer.as_fd(), region.lease().file().as_fd(), &mut offset, 1) {
        Ok(1) => {
            region.cache_sendfile_support(true);
            Ok(true)
        }
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "sendfile preflight could not read the first leased byte",
        )),
        Err(error) if is_unsupported(&error) => {
            region.cache_sendfile_support(false);
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

pub(crate) async fn send_file_region(writer: &mut OwnedWriteHalf, region: &FileRegion) -> io::Result<u64> {
    let mut offset = libc::off_t::try_from(region.offset())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file-region offset exceeds off_t"))?;
    let mut remaining = region.len();
    let mut sent = 0_u64;
    while remaining != 0 {
        let count = usize::try_from(remaining.min(MAX_SENDFILE_COUNT as u64))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "sendfile count exceeds usize"))?;
        let tcp_stream = writer.as_ref();
        let result = tcp_stream
            .async_io(Interest::WRITABLE, || {
                try_sendfile(tcp_stream.as_fd(), region.lease().file().as_fd(), &mut offset, count)
            })
            .await;
        match result {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "sendfile reached EOF before the leased file region ended",
                ));
            }
            Ok(written) => {
                remaining -= written as u64;
                sent += written as u64;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(sent)
}

fn is_unsupported(error: &io::Error) -> bool {
    error.raw_os_error().is_some_and(|code| {
        code == libc::EINVAL || code == libc::ENOSYS || code == libc::EOPNOTSUPP || code == libc::EXDEV
    })
}

fn try_sendfile(
    out_fd: BorrowedFd<'_>,
    in_fd: BorrowedFd<'_>,
    offset: &mut libc::off_t,
    count: usize,
) -> io::Result<usize> {
    // SAFETY: both borrowed descriptors remain valid for this call, `offset` is a valid mutable
    // off_t pointer, and `count` is capped at Linux's documented per-call sendfile maximum.
    let written = unsafe { libc::sendfile(out_fd.as_raw_fd(), in_fd.as_raw_fd(), offset, count) };
    if written < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(written as usize)
    }
}
