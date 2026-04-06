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

//! Compatibility RocksDB message store.
//!
//! The full RocksDB-backed queue/index implementation is still under active
//! alignment work. For the current P1-4 scope we expose a `RocksDBMessageStore`
//! type that reuses the already-closed local file message store main path so
//! `store_type=RocksDB` can participate in `init/load/start/recover/query`
//! flows without hitting placeholder panics.
//!
//! This keeps the public type boundary stable for broker/store wiring and
//! allows the later real RocksDB backend to replace this alias without
//! reshaping callers.

pub type RocksDBMessageStore = crate::message_store::local_file_message_store::LocalFileMessageStore;
