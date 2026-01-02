// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub struct QueryOffsetResult {
    phy_offsets: Vec<i64>,
    index_last_update_timestamp: i64,
    index_last_update_phyoffset: i64,
}

impl QueryOffsetResult {
    pub fn new(phy_offsets: Vec<i64>, index_last_update_timestamp: i64, index_last_update_phyoffset: i64) -> Self {
        QueryOffsetResult {
            phy_offsets,
            index_last_update_timestamp,
            index_last_update_phyoffset,
        }
    }

    #[inline]
    pub fn get_phy_offsets(&self) -> &Vec<i64> {
        &self.phy_offsets
    }

    #[inline]
    pub fn get_phy_offsets_mut(&mut self) -> &mut Vec<i64> {
        &mut self.phy_offsets
    }

    #[inline]
    pub fn get_index_last_update_timestamp(&self) -> i64 {
        self.index_last_update_timestamp
    }

    #[inline]
    pub fn get_index_last_update_phyoffset(&self) -> i64 {
        self.index_last_update_phyoffset
    }
}
