// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;

pub fn default_elect_master_request_header() -> ElectMasterRequestHeader {
    ElectMasterRequestHeader::new("", "", 0, false, current_millis() as u64)
}

fn current_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be later than the unix epoch")
            .as_millis() as u64
    }

    #[test]
    fn default_elect_master_request_header_matches_production_defaults() {
        let header = default_elect_master_request_header();

        assert_eq!(header.cluster_name, "");
        assert_eq!(header.broker_name, "");
        assert_eq!(header.broker_id, 0);
        assert!(!header.designate_elect);
    }

    #[test]
    fn default_elect_master_request_header_uses_wall_clock_invoke_time() {
        let before = now_millis();
        let header = default_elect_master_request_header();

        assert!(header.invoke_time > 0);
        assert!(header.invoke_time >= before);
    }
}
