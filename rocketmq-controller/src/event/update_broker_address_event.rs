//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use cheetah_string::CheetahString;

use crate::event::event_message::EventMessage;
use crate::event::event_type::EventType;

#[derive(Debug, Clone)]
pub struct UpdateBrokerAddressEvent {
    cluster_name: CheetahString,
    broker_name: CheetahString,
    broker_address: CheetahString,
    broker_id: Option<u64>,
}

impl UpdateBrokerAddressEvent {
    pub fn new(
        cluster_name: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        broker_address: impl Into<CheetahString>,
        broker_id: Option<u64>,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            broker_address: broker_address.into(),
            broker_id,
        }
    }

    #[inline]
    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    #[inline]
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    #[inline]
    pub fn broker_address(&self) -> &str {
        &self.broker_address
    }

    #[inline]
    pub fn broker_id(&self) -> Option<u64> {
        self.broker_id
    }
}

impl EventMessage for UpdateBrokerAddressEvent {
    fn get_event_type(&self) -> EventType {
        EventType::UpdateBrokerAddressEvent
    }
}
