/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License; Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing; software
 * distributed under the License is distributed on an "AS IS" BASIS;
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND; either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;

use tracing::{error, info, warn};

use crate::FileUtils;

// Define the trait ConfigManager
pub trait ConfigManager {
    fn load(&self) -> bool {
        let file_name = self.config_file_path();
        info!("Config file Path: {}", file_name);
        let result = FileUtils::file_to_string(file_name.as_str());
        match result {
            Ok(ref content) => {
                if content.is_empty() {
                    warn!("load back config file");
                    self.load_bak()
                } else {
                    self.decode(content);
                    true
                }
            }
            Err(_) => self.load_bak(),
        }
    }

    fn load_bak(&self) -> bool {
        let file_name = self.config_file_path();
        return if let Ok(ref content) =
            FileUtils::file_to_string(format!("{}{}", file_name, ".bak").as_str())
        {
            if !content.is_empty() {
                self.decode(content);
            }
            true
        } else {
            false
        };
    }

    fn persist_with_topic<T>(&mut self, _topic_name: &str, _t: T) {
        self.persist()
    }
    fn persist_map<T>(&mut self, _m: &HashMap<String, T>) {
        self.persist()
    }
    fn persist(&mut self) {
        let json = self.encode_pretty(true);
        if !json.is_empty() {
            let file_name = self.config_file_path();
            if FileUtils::string_to_file(json.as_str(), file_name.as_str()).is_err() {
                error!("persist file {} exception", file_name);
            }
        }
    }
    fn decode0(&mut self, key: &[u8], body: &[u8]);
    fn stop(&mut self) -> bool;
    fn config_file_path(&self) -> String;
    fn encode(&mut self) -> String {
        self.encode_pretty(false)
    }
    fn encode_pretty(&mut self, pretty_format: bool) -> String;
    fn decode(&self, json_string: &str);
}
