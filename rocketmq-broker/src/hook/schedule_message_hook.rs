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

 use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
 use rocketmq_rust::ArcMut;
 use rocketmq_store::base::message_result::PutMessageResult;
 use rocketmq_store::base::message_store::MessageStore;
 use rocketmq_store::hook::put_message_hook::PutMessageHook;
 use crate::broker_runtime::BrokerRuntimeInner;
 use crate::util::hook_utils::HookUtils;
 
 pub struct ScheduleMessageHook<MS> {
     broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
 }
 
 impl<MS:MessageStore> PutMessageHook for ScheduleMessageHook<MS> {
     fn hook_name(&self) -> String {
         "ScheduleMessageHook".to_string()
     }
 
     fn execute_before_put_message(&self, msg: &mut MessageExtBrokerInner) -> Option<PutMessageResult> {
         HookUtils::handle_schedule_message(&self.broker_runtime_inner, msg)
     }
 }