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

use std::any::Any;
use std::collections::HashMap;

use cheetah_string::CheetahString;

use crate::authentication::context::authentication_context::AuthenticationContext;
use crate::authentication::AsAny;

#[derive(Debug, Default)]
pub struct BaseAuthenticationContext {
    channel_id: Option<CheetahString>,

    rpc_code: Option<CheetahString>,

    ext_info: HashMap<CheetahString, Box<dyn Any + Send + Sync>>,
}

impl BaseAuthenticationContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn channel_id(&self) -> Option<&CheetahString> {
        self.channel_id.as_ref()
    }

    pub fn set_channel_id(&mut self, channel_id: CheetahString) {
        self.channel_id = Some(channel_id);
    }

    pub fn rpc_code(&self) -> Option<&CheetahString> {
        self.rpc_code.as_ref()
    }

    pub fn set_rpc_code(&mut self, rpc_code: CheetahString) {
        self.rpc_code = Some(rpc_code);
    }
}

impl BaseAuthenticationContext {
    pub fn get_ext_info<T: 'static>(&self, key: &CheetahString) -> Option<&T> {
        self.ext_info.get(key).and_then(|v| v.downcast_ref::<T>())
    }
    pub fn has_ext_info(&self, key: &CheetahString) -> bool {
        self.ext_info.contains_key(key)
    }
    pub fn ext_info(&self) -> &HashMap<CheetahString, Box<dyn Any + Send + Sync>> {
        &self.ext_info
    }

    pub fn ext_info_mut(&mut self) -> &mut HashMap<CheetahString, Box<dyn Any + Send + Sync>> {
        &mut self.ext_info
    }
}

impl BaseAuthenticationContext {
    pub fn set_ext_info<T>(&mut self, key: CheetahString, value: T)
    where
        T: Any + Send + Sync,
    {
        self.ext_info.insert(key, Box::new(value));
    }
}

impl AuthenticationContext for BaseAuthenticationContext {}

impl AsAny for BaseAuthenticationContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
