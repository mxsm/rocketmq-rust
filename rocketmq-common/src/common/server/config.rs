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
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    pub listen_port: u32,
    pub bind_address: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            listen_port: 10911,
            bind_address: "0.0.0.0".to_string(),
        }
    }
}

impl ServerConfig {
    pub fn bind_address(&self) -> String {
        self.bind_address.clone()
    }

    pub fn listen_port(&self) -> u32 {
        self.listen_port
    }
}
