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

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OffsetWrapper {
    broker_offset: i64,
    consumer_offset: i64,
    pull_offset: i64,
    last_timestamp: i64,
}

impl OffsetWrapper {
    pub fn new() -> Self {
        Self {
            broker_offset: 0,
            consumer_offset: 0,
            pull_offset: 0,
            last_timestamp: 0,
        }
    }

    pub fn get_broker_offset(&self) -> i64 {
        self.broker_offset
    }

    pub fn set_broker_offset(&mut self, broker_offset: i64) {
        self.broker_offset = broker_offset;
    }

    pub fn get_consumer_offset(&self) -> i64 {
        self.consumer_offset
    }

    pub fn set_consumer_offset(&mut self, consumer_offset: i64) {
        self.consumer_offset = consumer_offset;
    }

    pub fn get_pull_offset(&self) -> i64 {
        self.pull_offset
    }

    pub fn set_pull_offset(&mut self, pull_offset: i64) {
        self.pull_offset = pull_offset;
    }

    pub fn get_last_timestamp(&self) -> i64 {
        self.last_timestamp
    }

    pub fn set_last_timestamp(&mut self, last_timestamp: i64) {
        self.last_timestamp = last_timestamp;
    }
}
