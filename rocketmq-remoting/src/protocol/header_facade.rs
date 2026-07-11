// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_protocol::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;

pub fn default_elect_master_request_header() -> ElectMasterRequestHeader {
    ElectMasterRequestHeader::new("", "", 0, false, current_millis() as u64)
}

fn current_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}
