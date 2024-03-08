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
 use serde::{Deserialize, Serialize};

 #[derive(Clone, Debug, Serialize, Deserialize, Default)]
 #[serde(rename_all = "camelCase")]
 pub struct BloomFilterData {
     bit_pos: Vec<i32>,
     bit_num: u32,
 }
 
 impl BloomFilterData {
     pub fn new(bit_pos: Vec<i32>, bit_num: u32) -> Self {
         Self { bit_pos, bit_num }
     }
 
     pub fn set_bit_pos(&mut self, bit_pos: Vec<i32>) {
         self.bit_pos = bit_pos;
     }
     pub fn set_bit_num(&mut self, bit_num: u32) {
         self.bit_num = bit_num;
     }
 
     pub fn bit_pos(&self) -> &Vec<i32> {
         &self.bit_pos
     }
     pub fn bit_num(&self) -> u32 {
         self.bit_num
     }
 }
 