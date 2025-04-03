/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_rust::ArcMut;

use crate::consume_queue::consume_queue_ext::CqExtUnit;
use crate::queue::consume_queue::ConsumeQueueTrait;
use crate::queue::consume_queue_ext::ConsumeQueueExt;
use crate::queue::file_queue_life_cycle::FileQueueLifeCycle;

mod batch_consume_queue;
pub mod build_consume_queue;
pub mod consume_queue;
mod consume_queue_ext;
pub mod consume_queue_store;
mod file_queue_life_cycle;
pub mod local_file_consume_queue_store;
mod queue_offset_operator;
pub mod referred_iterator;
pub mod single_consume_queue;

pub type ArcConsumeQueue = ArcMut<Box<dyn ConsumeQueueTrait>>;
pub type ConsumeQueueTable =
    parking_lot::Mutex<HashMap<CheetahString, HashMap<i32, ArcConsumeQueue>>>;

pub struct CqUnit {
    pub queue_offset: i64,
    pub size: i32,
    pub pos: i64,
    pub batch_num: i16,
    pub tags_code: i64,
    pub cq_ext_unit: Option<CqExtUnit>,
    pub native_buffer: Vec<u8>,
    pub compacted_offset: i32,
}

impl Default for CqUnit {
    fn default() -> Self {
        CqUnit {
            queue_offset: 0,
            size: 0,
            pos: 0,
            batch_num: 1,
            tags_code: 0,
            cq_ext_unit: None,
            native_buffer: vec![],
            compacted_offset: 0,
        }
    }
}

impl CqUnit {
    pub fn get_valid_tags_code_as_long(&self) -> Option<i64> {
        if !self.is_tags_code_valid() {
            return None;
        }
        Some(self.tags_code)
    }

    pub fn is_tags_code_valid(&self) -> bool {
        !ConsumeQueueExt::is_ext_addr(self.tags_code)
    }
}
