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
pub mod send_message_request_header;
pub mod send_message_response_header;

pub trait TopicRequestHeaderTrait {
    fn with_lo(&mut self, lo: Option<bool>);

    fn lo(&self) -> Option<bool>;

    fn with_topic(&mut self, topic: String);

    fn topic(&self) -> String;

    fn broker_name(&self) -> Option<String>;

    fn with_broker_name(&mut self, broker_name: String);

    fn namespace(&self) -> Option<String>;

    fn with_namespace(&mut self, namespace: String);

    fn namespaced(&self) -> Option<bool>;

    fn with_namespaced(&mut self, namespaced: bool);

    fn oneway(&self) -> Option<bool>;

    fn with_oneway(&mut self, oneway: bool);

    fn queue_id(&self) -> Option<i32>;

    fn set_queue_id(&mut self, queue_id: Option<i32>);
}
