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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::JoinHandle;

use cheetah_string::CheetahString;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;

pub struct PopReviveService {
    queue_id: i32,
    revive_topic: CheetahString,
    current_revive_message_timestamp: i64,
    should_run_pop_revive: bool,
    inflight_revive_request_map: Arc<parking_lot::Mutex<BTreeMap<PopCheckPoint, (i64, bool)>>>,
    revive_offset: i64,
    ck_rewrite_intervals_in_seconds: [i32; 17],
    join_handle: Option<JoinHandle<()>>,
}
impl PopReviveService {
    pub fn new(revive_topic: CheetahString, queue_id: i32, revive_offset: i64) -> Self {
        Self {
            queue_id,
            revive_topic,
            current_revive_message_timestamp: -1,
            should_run_pop_revive: false,
            inflight_revive_request_map: Arc::new(Default::default()),
            revive_offset,
            ck_rewrite_intervals_in_seconds: [
                10, 20, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
            ],
            join_handle: None,
        }
    }
}

struct ConsumeReviveObj {
    map: HashMap<CheetahString, PopCheckPoint>,
    sort_list: Option<Vec<PopCheckPoint>>,
    old_offset: i64,
    end_time: i64,
    new_offset: i64,
}

impl ConsumeReviveObj {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            sort_list: None,
            old_offset: 0,
            end_time: 0,
            new_offset: 0,
        }
    }

    fn gen_sort_list(&mut self) -> &Vec<PopCheckPoint> {
        if self.sort_list.is_none() {
            let mut list: Vec<PopCheckPoint> = self.map.values().cloned().collect();
            list.sort_by_key(|ck| ck.revive_offset);
            self.sort_list = Some(list);
        }
        self.sort_list.as_ref().unwrap()
    }
}

#[cfg(test)]
mod tests {

    use cheetah_string::CheetahString;
    use rocketmq_store::pop::pop_check_point::PopCheckPoint;

    use super::*;

    #[test]
    fn new_initializes_correctly() {
        let obj = ConsumeReviveObj::new();
        assert!(obj.map.is_empty());
        assert!(obj.sort_list.is_none());
        assert_eq!(obj.old_offset, 0);
        assert_eq!(obj.end_time, 0);
        assert_eq!(obj.new_offset, 0);
    }

    #[test]
    fn gen_sort_list_creates_sorted_list() {
        let mut obj = ConsumeReviveObj::new();
        let ck1 = PopCheckPoint {
            revive_offset: 10,
            ..Default::default()
        };
        let ck2 = PopCheckPoint {
            revive_offset: 5,
            ..Default::default()
        };
        obj.map.insert(CheetahString::from("key1"), ck1.clone());
        obj.map.insert(CheetahString::from("key2"), ck2.clone());

        let sorted_list = obj.gen_sort_list();
        assert_eq!(sorted_list.len(), 2);
        assert_eq!(sorted_list[0].revive_offset, 5);
        assert_eq!(sorted_list[1].revive_offset, 10);
    }

    #[test]
    fn gen_sort_list_returns_existing_list_if_already_sorted() {
        let mut obj = ConsumeReviveObj::new();
        let ck1 = PopCheckPoint {
            revive_offset: 10,
            ..Default::default()
        };
        let ck2 = PopCheckPoint {
            revive_offset: 5,
            ..Default::default()
        };
        obj.map.insert(CheetahString::from("key1"), ck1.clone());
        obj.map.insert(CheetahString::from("key2"), ck2.clone());

        let _ = obj.gen_sort_list();
        let sorted_list = obj.gen_sort_list();
        assert_eq!(sorted_list.len(), 2);
        assert_eq!(sorted_list[0].revive_offset, 5);
        assert_eq!(sorted_list[1].revive_offset, 10);
    }
}
