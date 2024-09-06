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
use std::sync::Arc;

use rocketmq_common::ArcRefCellWrapper;

use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::filter_message_hook::FilterMessageHook;

#[derive(Clone)]
pub struct PullAPIWrapper;

impl PullAPIWrapper {
    pub fn new(
        mq_client_factory: ArcRefCellWrapper<MQClientInstance>,
        consumer_group: String,
        unit_mode: bool,
    ) -> Self {
        unimplemented!("PullAPIWrapper::new")
    }

    pub fn register_filter_message_hook(
        &mut self,
        filter_message_hook_list: Vec<Arc<Box<dyn FilterMessageHook + Send + Sync>>>,
    ) {
        unimplemented!("PullAPIWrapper::registerFilterMessageHook")
    }
}
