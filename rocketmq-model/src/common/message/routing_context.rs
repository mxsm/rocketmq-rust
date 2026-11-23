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

use std::net::SocketAddr;

use bytes::Bytes;

use crate::common::sys_flag::message_sys_flag::MessageSysFlag;
use crate::utils::message_utils::socket_address_to_vec;

/// Message network transmission metadata
///
/// Contains metadata generated during network transmission, such as client address, timestamp, and
/// system flags.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutingContext {
    born_host: SocketAddr,
    born_timestamp: i64,
    sys_flag: i32,
}

impl RoutingContext {
    /// Creates a new routing context
    pub fn new(born_host: SocketAddr, born_timestamp: i64, sys_flag: i32) -> Self {
        Self {
            born_host,
            born_timestamp,
            sys_flag,
        }
    }

    /// Gets the message sender's client address
    #[inline]
    pub fn born_host(&self) -> SocketAddr {
        self.born_host
    }

    /// Gets the message creation timestamp
    #[inline]
    pub fn born_timestamp(&self) -> i64 {
        self.born_timestamp
    }

    /// Gets the system flag bits
    #[inline]
    pub fn sys_flag(&self) -> i32 {
        self.sys_flag
    }

    /// Sets the IPv6 flag for born_host
    #[inline]
    pub fn with_born_host_v6_flag(&mut self) {
        self.sys_flag |= MessageSysFlag::BORNHOST_V6_FLAG;
    }

    /// Serializes born_host to bytes
    pub fn born_host_bytes(&self) -> Bytes {
        socket_address_to_vec(self.born_host).into()
    }
}

impl Default for RoutingContext {
    fn default() -> Self {
        Self {
            born_host: SocketAddr::from(([127, 0, 0, 1], 0)),
            born_timestamp: 0,
            sys_flag: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routing_context_creation() {
        let addr: SocketAddr = "192.168.1.100:10911".parse().unwrap();
        let ctx = RoutingContext::new(addr, 1234567890, 0);

        assert_eq!(ctx.born_host(), addr);
        assert_eq!(ctx.born_timestamp(), 1234567890);
        assert_eq!(ctx.sys_flag(), 0);
    }

    #[test]
    fn test_with_ipv6_flag() {
        let addr: SocketAddr = "127.0.0.1:10911".parse().unwrap();
        let mut ctx = RoutingContext::new(addr, 0, 0);

        ctx.with_born_host_v6_flag();
        assert_ne!(ctx.sys_flag(), 0);
    }

    #[test]
    fn test_born_host_bytes_v4() {
        let addr: SocketAddr = "192.168.1.100:10911".parse().unwrap();
        let ctx = RoutingContext::new(addr, 0, 0);

        let bytes = ctx.born_host_bytes();
        assert_eq!(bytes.as_ref(), &[192, 168, 1, 100, 0, 0, 0x2a, 0x9f]);
    }

    #[test]
    fn test_born_host_bytes_v6() {
        let addr: SocketAddr = "[::1]:10911".parse().unwrap();
        let ctx = RoutingContext::new(addr, 0, 0);

        let bytes = ctx.born_host_bytes();
        assert_eq!(bytes.len(), 20); // 16 bytes IP + 4 bytes port
    }

    #[test]
    fn test_default() {
        let ctx = RoutingContext::default();
        assert_eq!(ctx.born_timestamp(), 0);
        assert_eq!(ctx.sys_flag(), 0);
    }
}
