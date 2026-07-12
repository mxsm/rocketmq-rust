// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Clock-owning compatibility helpers for the canonical data-version value.

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_protocol::protocol::DataVersion;

pub fn new_data_version() -> DataVersion {
    DataVersion::with_values(0, current_millis(), 0)
}

pub trait DataVersionExt {
    fn next_version(&mut self);
    fn next_version_with(&mut self, state_version: i64);
}

impl DataVersionExt for DataVersion {
    fn next_version(&mut self) {
        self.next_version_with(0);
    }

    fn next_version_with(&mut self, state_version: i64) {
        self.next_version_with_timestamp(state_version, current_millis());
    }
}

fn current_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}
