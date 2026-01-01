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

/// Re-export MemStorage from raft-rs
///
/// In a production implementation, this should be replaced with
/// a persistent storage implementation that writes to disk.
pub use raft::storage::MemStorage;

// Future implementations should:
// - Persist entries to disk (e.g., using RocksDB or custom log file)
// - Support efficient snapshot creation and restoration
// - Handle concurrent reads and writes
// - Provide durability guarantees
//
// Example structure:
// ```ignore
// pub struct PersistentStorage {
//     path: PathBuf,
//     db: Arc<RocksDB>,
//     // ... other fields
// }
//
// impl Storage for PersistentStorage {
//     // Implement Storage trait methods
// }
// ```
