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

pub use rocketmq_store_local::config::FlushDiskType;

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn test_default_flush_disk_type() {
        assert_eq!(FlushDiskType::default(), FlushDiskType::AsyncFlush);
    }

    #[test]
    fn test_get_flush_disk_type_string() {
        assert_eq!(FlushDiskType::SyncFlush.get_flush_disk_type(), "SYNC_FLUSH");
        assert_eq!(FlushDiskType::AsyncFlush.get_flush_disk_type(), "ASYNC_FLUSH");
    }

    #[test]
    fn test_deserialize_sync_flush_variants() {
        assert_eq!(
            serde_json::from_str::<FlushDiskType>("\"SYNC_FLUSH\"").unwrap(),
            FlushDiskType::SyncFlush
        );
        assert_eq!(
            serde_json::from_str::<FlushDiskType>("\"SyncFlush\"").unwrap(),
            FlushDiskType::SyncFlush
        );
    }

    #[test]
    fn test_deserialize_async_flush_variants() {
        assert_eq!(
            serde_json::from_str::<FlushDiskType>("\"ASYNC_FLUSH\"").unwrap(),
            FlushDiskType::AsyncFlush
        );
        assert_eq!(
            serde_json::from_str::<FlushDiskType>("\"AsyncFlush\"").unwrap(),
            FlushDiskType::AsyncFlush
        );
    }

    #[test]
    fn test_deserialize_invalid_variant() {
        assert!(serde_json::from_str::<FlushDiskType>("\"INVALID\"").is_err());
        assert!(serde_json::from_str::<FlushDiskType>("\"sync_flush\"").is_err());
        assert!(serde_json::from_str::<FlushDiskType>("\"\"").is_err());
    }

    #[test]
    fn test_copy_clone_traits() {
        let sync_flush = FlushDiskType::SyncFlush;
        let copied = sync_flush; // copy
        let cloned: FlushDiskType = sync_flush; // clone

        assert_eq!(sync_flush, copied);
        assert_eq!(sync_flush, cloned);
    }

    #[test]
    fn test_debug_format() {
        assert_eq!(format!("{:?}", FlushDiskType::SyncFlush), "SyncFlush");
        assert_eq!(format!("{:?}", FlushDiskType::AsyncFlush), "AsyncFlush");
    }

    #[test]
    fn test_partial_eq() {
        assert_eq!(FlushDiskType::SyncFlush, FlushDiskType::SyncFlush);
        assert_eq!(FlushDiskType::AsyncFlush, FlushDiskType::AsyncFlush);
        assert_ne!(FlushDiskType::SyncFlush, FlushDiskType::AsyncFlush);
    }
}
