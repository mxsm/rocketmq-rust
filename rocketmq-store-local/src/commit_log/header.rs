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

//! Runtime-neutral CommitLog fixed-header layout and timestamp probing.

use bytes::Bytes;

const MAGIC_CODE_POSITION: usize = 4;
const SYS_FLAG_POSITION: usize = 36;
/// CommitLog born-host IPv6 flag.
pub(crate) const BORN_HOST_V6_FLAG: i32 = 0x10;
/// CommitLog store-host IPv6 flag.
pub(crate) const STORE_HOST_V6_FLAG: i32 = 0x20;
const IPV4_HOST_LENGTH: usize = 8;
const IPV6_HOST_LENGTH: usize = 20;
const IPV4_STORE_TIMESTAMP_POSITION: usize = 56;
const IPV6_STORE_TIMESTAMP_POSITION: usize = 68;

/// CommitLog V1 message magic code (`daa320a7`).
pub const MESSAGE_MAGIC_CODE: i32 = -626843481;

/// CommitLog V2 message magic code (`daa320ab`).
pub const MESSAGE_MAGIC_CODE_V2: i32 = -626843477;

/// Width of a host address encoded in a CommitLog fixed header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostWidth {
    /// Four-byte IP address followed by a four-byte port.
    Ipv4,
    /// Sixteen-byte IP address followed by a four-byte port.
    Ipv6,
}

impl HostWidth {
    /// Selects the encoded born-host width from the system flags.
    pub(crate) fn born(sys_flag: i32) -> Self {
        if sys_flag & BORN_HOST_V6_FLAG == 0 {
            Self::Ipv4
        } else {
            Self::Ipv6
        }
    }

    /// Selects the encoded store-host width from the system flags.
    pub(crate) fn store(sys_flag: i32) -> Self {
        if sys_flag & STORE_HOST_V6_FLAG == 0 {
            Self::Ipv4
        } else {
            Self::Ipv6
        }
    }

    /// Returns the encoded host-and-port length.
    pub(crate) fn encoded_len(self) -> usize {
        match self {
            Self::Ipv4 => IPV4_HOST_LENGTH,
            Self::Ipv6 => IPV6_HOST_LENGTH,
        }
    }

    /// Returns the Store timestamp position selected by the born-host width.
    pub(crate) fn store_timestamp_position(self) -> usize {
        match self {
            Self::Ipv4 => IPV4_STORE_TIMESTAMP_POSITION,
            Self::Ipv6 => IPV6_STORE_TIMESTAMP_POSITION,
        }
    }
}

/// Probes the Store timestamp through recovery's sparse fixed-header reads.
///
/// A missing read is decoded as zero. An invalid magic code or a zero timestamp returns `None`.
///
/// # Panics
///
/// Panics if `read` returns fewer bytes than the requested fixed-width field.
pub fn probe_store_timestamp<F>(mut read: F) -> Option<i64>
where
    F: FnMut(usize, usize) -> Option<Bytes>,
{
    let magic_code = read_i32_or_zero(&mut read, MAGIC_CODE_POSITION);
    if magic_code != MESSAGE_MAGIC_CODE && magic_code != MESSAGE_MAGIC_CODE_V2 {
        return None;
    }

    let sys_flag = read_i32_or_zero(&mut read, SYS_FLAG_POSITION);
    let timestamp_position = HostWidth::born(sys_flag).store_timestamp_position();
    let store_timestamp = read_i64_or_zero(&mut read, timestamp_position);
    (store_timestamp != 0).then_some(store_timestamp)
}

/// Reads the Store timestamp directly from a complete CommitLog frame.
///
/// # Panics
///
/// Panics when `frame` is too short for the fixed system-flag field or selected Store timestamp
/// field, preserving legacy direct-indexing behavior.
pub fn store_timestamp_from_frame(frame: &[u8]) -> i64 {
    let sys_flag = i32::from_be_bytes(frame[SYS_FLAG_POSITION..SYS_FLAG_POSITION + 4].try_into().unwrap());
    let timestamp_position = HostWidth::born(sys_flag).store_timestamp_position();
    i64::from_be_bytes(frame[timestamp_position..timestamp_position + 8].try_into().unwrap())
}

fn read_i32_or_zero<F>(read: &mut F, offset: usize) -> i32
where
    F: FnMut(usize, usize) -> Option<Bytes>,
{
    read(offset, 4)
        .map(|bytes| i32::from_be_bytes(bytes[0..4].try_into().unwrap()))
        .unwrap_or(0)
}

fn read_i64_or_zero<F>(read: &mut F, offset: usize) -> i64
where
    F: FnMut(usize, usize) -> Option<Bytes>,
{
    read(offset, 8)
        .map(|bytes| i64::from_be_bytes(bytes[0..8].try_into().unwrap()))
        .unwrap_or(0)
}
