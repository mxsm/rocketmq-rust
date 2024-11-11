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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct GetAllTopicConfigResponseHeader;

impl CommandCustomHeader for GetAllTopicConfigResponseHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        None
    }
}
impl FromMap for GetAllTopicConfigResponseHeader {
    type Target = Self;

    fn from(_map: &HashMap<CheetahString, CheetahString>) -> Option<Self::Target> {
        Some(Self {})
    }
}
