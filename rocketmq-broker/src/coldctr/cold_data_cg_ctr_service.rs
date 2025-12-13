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

use tracing::warn;

#[derive(Default)]
pub struct ColdDataCgCtrService;

impl ColdDataCgCtrService {
    pub fn start(&mut self) {
        warn!("ColdDataCgCtrService started not implemented");
    }

    pub fn is_cg_need_cold_data_flow_ctr(&self, _consumer_group: &str) -> bool {
        false
    }

    pub fn shutdown(&mut self) {
        warn!("ColdDataCgCtrService shutdown not implemented");
    }
}
