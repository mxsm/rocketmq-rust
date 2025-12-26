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

use std::hash::Hash;

use cheetah_string::CheetahString;

use crate::migration::alc::plain_access_config::PlainAccessConfig;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct PlainAccessData {
    pub global_white_remote_addresses: Vec<CheetahString>,

    pub accounts: Vec<PlainAccessConfig>,

    pub data_version: Vec<DataVersion>,
}

impl PlainAccessData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn global_white_remote_addresses(&self) -> &[CheetahString] {
        &self.global_white_remote_addresses
    }

    pub fn set_global_white_remote_addresses(&mut self, addrs: Vec<CheetahString>) {
        self.global_white_remote_addresses = addrs;
    }

    pub fn accounts(&self) -> &[PlainAccessConfig] {
        &self.accounts
    }

    pub fn set_accounts(&mut self, accounts: Vec<PlainAccessConfig>) {
        self.accounts = accounts;
    }

    pub fn data_version(&self) -> &[DataVersion] {
        &self.data_version
    }

    pub fn set_data_version(&mut self, versions: Vec<DataVersion>) {
        self.data_version = versions;
    }
}

impl PlainAccessData {
    pub fn has_changed(&self, other: &Self) -> bool {
        self.data_version != other.data_version
    }

    pub fn latest_version(&self) -> Option<&DataVersion> {
        self.data_version.last()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct DataVersion {
    pub timestamp: u64,
    pub counter: u64,
}
