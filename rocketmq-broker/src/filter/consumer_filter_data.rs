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
 use rocketmq_filter::utils::bloom_filter_data::BloomFilterData;
 use serde::{Deserialize, Serialize};
 
 #[derive(Clone, Debug, Serialize, Deserialize, Default)]
 #[serde(rename_all = "camelCase")]
 pub struct ConsumerFilterData {
     consumer_group: String,
     topic: String,
     expression: String,
     expression_type: String,
     //compiled_expression: Expression,
     born_time: u64,
     dead_time: u64,
     bloom_filter_data: BloomFilterData,
     client_version: u64,
 }
 
 impl ConsumerFilterData {
     pub fn consumer_group(&self) -> &str {
         &self.consumer_group
     }
     pub fn topic(&self) -> &str {
         &self.topic
     }
     pub fn expression(&self) -> &str {
         &self.expression
     }
     pub fn expression_type(&self) -> &str {
         &self.expression_type
     }
     pub fn born_time(&self) -> u64 {
         self.born_time
     }
     pub fn dead_time(&self) -> u64 {
         self.dead_time
     }
     pub fn bloom_filter_data(&self) -> &BloomFilterData {
         &self.bloom_filter_data
     }
     pub fn client_version(&self) -> u64 {
         self.client_version
     }
     pub fn set_consumer_group(&mut self, consumer_group: String) {
         self.consumer_group = consumer_group;
     }
     pub fn set_topic(&mut self, topic: String) {
         self.topic = topic;
     }
     pub fn set_expression(&mut self, expression: String) {
         self.expression = expression;
     }
     pub fn set_expression_type(&mut self, expression_type: String) {
         self.expression_type = expression_type;
     }
     pub fn set_born_time(&mut self, born_time: u64) {
         self.born_time = born_time;
     }
     pub fn set_dead_time(&mut self, dead_time: u64) {
         self.dead_time = dead_time;
     }
     pub fn set_bloom_filter_data(&mut self, bloom_filter_data: BloomFilterData) {
         self.bloom_filter_data = bloom_filter_data;
     }
     pub fn set_client_version(&mut self, client_version: u64) {
         self.client_version = client_version;
     }
 }
 