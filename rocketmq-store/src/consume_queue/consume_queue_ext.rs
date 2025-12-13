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

const MIN_EXT_UNIT_SIZE: i16 = 2  // size, 32k max
 + 8 * 2 // msg time + tagCode
  + 2; // bitMapSize
const MAX_EXT_UNIT_SIZE: i16 = i16::MAX;

#[derive(Clone, Default)]
pub struct CqExtUnit {
    size: i16,
    tags_code: i64,
    msg_store_time: i64,
    bit_map_size: i16,
    filter_bit_map: Option<Vec<u8>>,
}

impl CqExtUnit {
    pub fn new(tags_code: i64, msg_store_time: i64, filter_bit_map: Option<Vec<u8>>) -> Self {
        let bit_map_size = if let Some(val) = filter_bit_map.as_ref() {
            val.len() as i16
        } else {
            0
        };
        let size = MIN_EXT_UNIT_SIZE + bit_map_size;
        Self {
            size,
            tags_code,
            msg_store_time,
            bit_map_size,
            filter_bit_map,
        }
    }

    pub fn size(&self) -> i16 {
        self.size
    }
    pub fn tags_code(&self) -> i64 {
        self.tags_code
    }
    pub fn msg_store_time(&self) -> i64 {
        self.msg_store_time
    }
    pub fn bit_map_size(&self) -> i16 {
        self.bit_map_size
    }
    pub fn filter_bit_map(&self) -> &Option<Vec<u8>> {
        &self.filter_bit_map
    }
}
