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

use std::collections::HashMap;

use anyhow::Error;

use crate::protocol::command_custom_header::{CommandCustomHeader, FromMap};

#[derive(Debug, Clone)]
pub struct PutKVConfigRequestHeader {
    namespace: String,
    key: String,
    value: String,
}

impl PutKVConfigRequestHeader {
    pub fn new(namespace: String, key: String, value: String) -> Self {
        PutKVConfigRequestHeader {
            namespace,
            key,
            value,
        }
    }

    pub fn get_namespace(&self) -> &str {
        &self.namespace
    }

    pub fn set_namespace(&mut self, namespace: String) {
        self.namespace = namespace;
    }

    pub fn get_key(&self) -> &str {
        &self.key
    }

    pub fn set_key(&mut self, key: String) {
        self.key = key;
    }

    pub fn get_value(&self) -> &str {
        &self.value
    }

    pub fn set_value(&mut self, value: String) {
        self.value = value;
    }
}

impl FromMap for PutKVConfigRequestHeader {
    type Target = PutKVConfigRequestHeader;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        todo!()
    }
}

impl CommandCustomHeader for PutKVConfigRequestHeader {
    fn check_fields(&self) -> anyhow::Result<(), Error> {
        todo!()
    }

    fn to_map(&self) -> Option<std::collections::HashMap<String, String>> {
        todo!()
    }
}
