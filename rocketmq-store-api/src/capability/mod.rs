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

//! Narrow storage capability contracts.

mod admin;
mod appender;
mod derived;
mod health;
mod lifecycle;
mod offset;
mod reader;
mod release_checkpoint;
mod replication;

pub use admin::AdminStore;
pub use appender::MessageAppender;
pub use derived::DerivedRecordSink;
pub use health::StoreHealth;
pub use lifecycle::StoreLifecycle;
pub use offset::OffsetIndex;
pub use reader::MessageReader;
pub use release_checkpoint::ReleaseCheckpointCreateOutcome;
pub use release_checkpoint::ReleaseCheckpointCreateRejection;
pub use release_checkpoint::ReleaseCheckpointRestoreOutcome;
pub use release_checkpoint::ReleaseCheckpointRestoreRejection;
pub use release_checkpoint::ReleaseCheckpointStore;
pub use replication::ReplicationControl;
