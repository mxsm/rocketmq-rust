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

use cheetah_string::CheetahString;

use crate::authentication::context::base_authentication_context::BaseAuthenticationContext;
use crate::authentication::AsAny;
use crate::authorization::context::authentication_context::AuthenticationContext;

#[derive(Debug, Default)]
pub struct DefaultAuthenticationContext {
    pub base: BaseAuthenticationContext,

    username: Option<CheetahString>,
    content: Option<Vec<u8>>,
    signature: Option<CheetahString>,
}

impl DefaultAuthenticationContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn username(&self) -> Option<&CheetahString> {
        self.username.as_ref()
    }

    pub fn set_username(&mut self, username: CheetahString) {
        self.username = Some(username);
    }

    pub fn content(&self) -> Option<&[u8]> {
        self.content.as_deref()
    }

    pub fn set_content(&mut self, content: Vec<u8>) {
        self.content = Some(content);
    }

    pub fn signature(&self) -> Option<&CheetahString> {
        self.signature.as_ref()
    }

    pub fn set_signature(&mut self, signature: CheetahString) {
        self.signature = Some(signature);
    }
}

impl AsAny for DefaultAuthenticationContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl AuthenticationContext for DefaultAuthenticationContext {}
