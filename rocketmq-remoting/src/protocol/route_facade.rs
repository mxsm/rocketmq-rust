// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use cheetah_string::CheetahString;
use rand::RngExt;
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;

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
