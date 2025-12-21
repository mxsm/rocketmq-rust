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

use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_remoting::code::response_code::ResponseCode;

use crate::event::event_message::EventMessage;

pub struct ControllerResult<T> {
    events: Vec<Arc<dyn EventMessage + Send + Sync>>,
    response: Option<T>,
    body: Option<Bytes>,
    response_code: ResponseCode,
    remark: Option<CheetahString>,
}

impl<T> ControllerResult<T> {
    pub fn new(response: Option<T>) -> Self {
        Self {
            events: Vec::new(),
            response,
            body: None,
            response_code: ResponseCode::Success,
            remark: None,
        }
    }

    pub fn of(events: Vec<Arc<dyn EventMessage + Send + Sync>>, response: Option<T>) -> Self {
        Self {
            events,
            response,
            body: None,
            response_code: ResponseCode::Success,
            remark: None,
        }
    }

    pub fn events(&self) -> &[Arc<dyn EventMessage + Send + Sync>] {
        &self.events
    }

    pub fn add_event(&mut self, event: Arc<dyn EventMessage + Send + Sync>) {
        self.events.push(event);
    }
}
