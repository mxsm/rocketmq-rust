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

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;

use crate::common::sys_flag::message_sys_flag::MessageSysFlag;

/// Message storage metadata
///
/// Contains indexing and location information for messages in the Broker storage engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageMetadata {
    broker_name: CheetahString,
    queue_id: i32,
    queue_offset: i64,
    commit_log_offset: i64,
    store_timestamp: i64,
    store_host: SocketAddr,
    store_size: i32,
}

impl StorageMetadata {
    /// Creates new storage metadata
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        broker_name: CheetahString,
        queue_id: i32,
        queue_offset: i64,
        commit_log_offset: i64,
        store_timestamp: i64,
        store_host: SocketAddr,
        store_size: i32,
    ) -> Self {
        Self {
            broker_name,
            queue_id,
            queue_offset,
            commit_log_offset,
            store_timestamp,
            store_host,
            store_size,
        }
    }

    /// Gets the broker name
    #[inline]
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    /// Gets the queue ID
    #[inline]
    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }

    /// Gets the queue logical offset
    #[inline]
    pub fn queue_offset(&self) -> i64 {
        self.queue_offset
    }

    /// Gets the CommitLog physical offset
    #[inline]
    pub fn commit_log_offset(&self) -> i64 {
        self.commit_log_offset
    }

    /// Gets the storage timestamp
    #[inline]
    pub fn store_timestamp(&self) -> i64 {
        self.store_timestamp
    }

    /// Gets the storage host address
    #[inline]
    pub fn store_host(&self) -> SocketAddr {
        self.store_host
    }

    /// Gets the storage size in bytes
    #[inline]
    pub fn store_size(&self) -> i32 {
        self.store_size
    }

    /// Sets the IPv6 flag for store_host (internal use)
    #[inline]
    pub(crate) fn with_store_host_v6_flag(&mut self, sys_flag: &mut i32) {
        *sys_flag |= MessageSysFlag::STOREHOSTADDRESS_V6_FLAG;
    }

    /// Serializes store_host to bytes
    pub fn store_host_bytes(&self) -> Bytes {
        Self::socket_addr_to_bytes(&self.store_host)
    }

    fn socket_addr_to_bytes(addr: &SocketAddr) -> Bytes {
        match addr {
            SocketAddr::V4(v4) => {
                let mut buf = BytesMut::with_capacity(8);
                buf.put_slice(&v4.ip().octets());
                buf.put_i32(v4.port() as i32);
                buf.freeze()
            }
            SocketAddr::V6(v6) => {
                let mut buf = BytesMut::with_capacity(20);
                buf.put_slice(&v6.ip().octets());
                buf.put_i32(v6.port() as i32);
                buf.freeze()
            }
        }
    }

    /// Sets the broker name (internal use)
    #[inline]
    pub(crate) fn set_broker_name(&mut self, broker_name: CheetahString) {
        self.broker_name = broker_name;
    }

    /// Sets the queue offset (internal use)
    #[inline]
    pub(crate) fn set_queue_offset(&mut self, queue_offset: i64) {
        self.queue_offset = queue_offset;
    }

    /// Sets the CommitLog offset (internal use)
    #[inline]
    pub(crate) fn set_commit_log_offset(&mut self, commit_log_offset: i64) {
        self.commit_log_offset = commit_log_offset;
    }

    /// Sets the store timestamp (internal use)
    #[inline]
    pub(crate) fn set_store_timestamp(&mut self, store_timestamp: i64) {
        self.store_timestamp = store_timestamp;
    }

    /// Sets the store host (internal use)
    #[inline]
    pub(crate) fn set_store_host(&mut self, store_host: SocketAddr) {
        self.store_host = store_host;
    }

    /// Sets the store size (internal use)
    #[inline]
    pub(crate) fn set_store_size(&mut self, store_size: i32) {
        self.store_size = store_size;
    }

    /// Sets the queue ID (internal use)
    #[inline]
    pub(crate) fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }
}

impl Default for StorageMetadata {
    fn default() -> Self {
        Self {
            broker_name: CheetahString::new(),
            queue_id: 0,
            queue_offset: 0,
            commit_log_offset: 0,
            store_timestamp: 0,
            store_host: "127.0.0.1:10911".parse().unwrap(),
            store_size: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_metadata_creation() {
        let addr: SocketAddr = "192.168.1.100:10911".parse().unwrap();
        let metadata = StorageMetadata::new(
            CheetahString::from_static_str("broker-a"),
            0,
            1000,
            50000,
            1234567890,
            addr,
            1024,
        );

        assert_eq!(metadata.broker_name(), "broker-a");
        assert_eq!(metadata.queue_id(), 0);
        assert_eq!(metadata.queue_offset(), 1000);
        assert_eq!(metadata.commit_log_offset(), 50000);
        assert_eq!(metadata.store_timestamp(), 1234567890);
        assert_eq!(metadata.store_host(), addr);
        assert_eq!(metadata.store_size(), 1024);
    }

    #[test]
    fn test_store_host_bytes_v4() {
        let addr: SocketAddr = "192.168.1.100:10911".parse().unwrap();
        let metadata = StorageMetadata::new(CheetahString::new(), 0, 0, 0, 0, addr, 0);

        let bytes = metadata.store_host_bytes();
        assert_eq!(bytes.len(), 8);
    }

    #[test]
    fn test_store_host_bytes_v6() {
        let addr: SocketAddr = "[::1]:10911".parse().unwrap();
        let metadata = StorageMetadata::new(CheetahString::new(), 0, 0, 0, 0, addr, 0);

        let bytes = metadata.store_host_bytes();
        assert_eq!(bytes.len(), 20);
    }

    #[test]
    fn test_default() {
        let metadata = StorageMetadata::default();
        assert_eq!(metadata.broker_name(), "");
        assert_eq!(metadata.queue_id(), 0);
        assert_eq!(metadata.queue_offset(), 0);
        assert_eq!(metadata.commit_log_offset(), 0);
        assert_eq!(metadata.store_size(), 0);
    }

    #[test]
    fn test_setters() {
        let mut metadata = StorageMetadata::default();

        metadata.set_broker_name(CheetahString::from_static_str("test-broker"));
        metadata.set_queue_id(5);
        metadata.set_queue_offset(100);
        metadata.set_commit_log_offset(5000);
        metadata.set_store_timestamp(999999);
        metadata.set_store_size(2048);

        assert_eq!(metadata.broker_name(), "test-broker");
        assert_eq!(metadata.queue_id(), 5);
        assert_eq!(metadata.queue_offset(), 100);
        assert_eq!(metadata.commit_log_offset(), 5000);
        assert_eq!(metadata.store_timestamp(), 999999);
        assert_eq!(metadata.store_size(), 2048);
    }
}
