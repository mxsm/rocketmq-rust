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

use crate::{base::swappable::Swappable, consume_queue::mapped_file_queue::MappedFileQueue};

pub struct CommitLog {
    pub(crate) mapped_file_queue: MappedFileQueue,
}

impl CommitLog {
    pub fn load(&mut self) -> bool {
        let result = self.mapped_file_queue.load();

        result
    }
}

impl Swappable for CommitLog {
    fn swap_map(
        &self,
        reserve_num: i32,
        force_swap_interval_ms: i64,
        normal_swap_interval_ms: i64,
    ) {
        todo!()
    }

    fn clean_swapped_map(&self, force_clean_swap_interval_ms: i64) {
        todo!()
    }
}
