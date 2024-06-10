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

use rocketmq_remoting::protocol::RemotingSerializable;
use serde::Deserialize;
use serde::Serialize;

pub mod kvconfig_mananger;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVConfigSerializeWrapper {
    #[serde(rename = "configTable")]
    pub config_table:
        Option<HashMap<String /* Namespace */, HashMap<String /* Key */, String /* Value */>>>,
}

impl KVConfigSerializeWrapper {
    pub fn new_with_config_table(
        config_table: HashMap<String, HashMap<String, String>>,
    ) -> KVConfigSerializeWrapper {
        KVConfigSerializeWrapper {
            config_table: Some(config_table),
        }
    }

    pub fn new() -> KVConfigSerializeWrapper {
        KVConfigSerializeWrapper {
            config_table: Some(HashMap::new()),
        }
    }
}

impl RemotingSerializable for KVConfigSerializeWrapper {
    type Output = Self;
}
