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

use std::collections::HashSet;

use cheetah_string::CheetahString;

use crate::event::event_message::EventMessage;
use crate::event::event_type::EventType;

#[derive(Debug, Clone)]
pub struct AlterSyncStateSetEvent {
    broker_name: CheetahString,
    new_sync_state_set: HashSet<u64>, // BrokerId
}

impl AlterSyncStateSetEvent {
    pub fn new(
        broker_name: impl Into<CheetahString>,
        new_sync_state_set: impl IntoIterator<Item = u64>,
    ) -> Self {
        Self {
            broker_name: broker_name.into(),
            new_sync_state_set: new_sync_state_set.into_iter().collect(),
        }
    }

    #[inline]
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    #[inline]
    pub fn new_sync_state_set(&self) -> &HashSet<u64> {
        &self.new_sync_state_set
    }
}

impl EventMessage for AlterSyncStateSetEvent {
    fn get_event_type(&self) -> EventType {
        EventType::AlterSyncStateSetEvent
    }
}
