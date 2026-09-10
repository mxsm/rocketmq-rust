// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use crate::protocol::route::route_data_view::BrokerData;
use cheetah_string::CheetahString;
use rand::RngExt;

pub fn select_broker_addr(broker_data: &BrokerData) -> Option<CheetahString> {
    let index = if broker_data.broker_addrs().is_empty() {
        0
    } else {
        rand::rng().random_range(0..broker_data.broker_addrs().len())
    };
    broker_data.select_broker_addr_with_index(index)
}

pub trait BrokerDataExt {
    fn select_broker_addr(&self) -> Option<CheetahString>;
}

impl BrokerDataExt for BrokerData {
    fn select_broker_addr(&self) -> Option<CheetahString> {
        select_broker_addr(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::wire_constants::MASTER_ID;
    use std::collections::HashMap;

    fn create_test_broker_data(addrs: HashMap<u64, CheetahString>) -> BrokerData {
        BrokerData::new(
            CheetahString::from("test_cluster"),
            CheetahString::from("test_broker"),
            addrs,
            None,
        )
    }

    fn make_addr(id: u64, addr: &str) -> (u64, CheetahString) {
        (id, CheetahString::from(addr))
    }

    #[test]
    fn test_empty_broker_address_map() {
        let broker_data = create_test_broker_data(HashMap::new());
        assert!(select_broker_addr(&broker_data).is_none());
        assert!(broker_data.select_broker_addr().is_none())
    }

    #[test]
    fn test_map_containing_only_one_slave() {
        let slave_id = MASTER_ID + 1;
        let slave_addr = CheetahString::from("192.168.1.101");
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(slave_id, slave_addr.clone());
        let broker_data = create_test_broker_data(broker_addrs);
        assert_eq!(broker_data.select_broker_addr(), Some(slave_addr.clone()));
        assert_eq!(select_broker_addr(&broker_data), Some(slave_addr));
    }

    #[test]
    fn test_map_containing_master_and_slaves() {
        let master_addr = CheetahString::from("192.168.1.1");
        let broker_addrs = HashMap::from([
            make_addr(MASTER_ID + 1, "192.168.1.101"),
            make_addr(MASTER_ID + 2, "192.168.1.102"),
            (MASTER_ID, master_addr.clone()),
        ]);
        let broker_data = create_test_broker_data(broker_addrs);
        assert_eq!(broker_data.select_broker_addr(), Some(master_addr.clone()));
        assert_eq!(select_broker_addr(&broker_data), Some(master_addr));
    }

    #[test]
    fn test_map_containing_multiple_slaves() {
        let broker_addrs: HashMap<u64, CheetahString> = (1..=5)
            .map(|id| (id, CheetahString::from(format!("192.168.1.10{}", id))))
            .collect();
        let broker_data = create_test_broker_data(broker_addrs.clone());
        let selected = broker_data.select_broker_addr();
        assert!(selected.is_some());
        assert!(broker_addrs.values().any(|v| v == selected.as_ref().unwrap()));
        let selected_free = select_broker_addr(&broker_data);
        assert!(selected_free.is_some());
        assert!(broker_addrs.values().any(|v| v == selected_free.as_ref().unwrap()));
    }
}
