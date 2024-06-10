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

use tracing::error;
use tracing::info;
use tracing::warn;

use crate::FileUtils;

// Define the trait ConfigManager
pub trait ConfigManager {
    /// Loads the configuration from a file.
    ///
    /// This method attempts to load the configuration from a file whose path is returned by
    /// `config_file_path`. If the file content is empty, it attempts to load from a backup
    /// file. If the file content is not empty, it decodes the content and logs a success
    /// message.
    ///
    /// # Returns
    /// * `true` if the configuration is successfully loaded and decoded.
    /// * `false` if the configuration loading fails.
    fn load(&self) -> bool {
        let file_name = self.config_file_path();
        let result = FileUtils::file_to_string(file_name.as_str());
        match result {
            Ok(ref content) => {
                if content.is_empty() {
                    warn!("load bak config file");
                    self.load_bak()
                } else {
                    self.decode(content);
                    info!("load Config file: {} -----OK", file_name);
                    true
                }
            }
            Err(_) => self.load_bak(),
        }
    }

    /// Loads the configuration from a backup file.
    ///
    /// This method attempts to load the configuration from a backup file whose path is returned by
    /// `config_file_path` with ".bak" appended. If the file content is not empty, it decodes
    /// the content and logs a success message.
    ///
    /// # Returns
    /// * `true` if the configuration is successfully loaded and decoded.
    /// * `false` if the configuration loading fails.
    fn load_bak(&self) -> bool {
        let file_name = self.config_file_path();
        return if let Ok(ref content) =
            FileUtils::file_to_string(format!("{}{}", file_name, ".bak").as_str())
        {
            if !content.is_empty() {
                self.decode(content);
                info!("load Config file: {}.bak -----OK", file_name);
            }
            true
        } else {
            error!("load Config file: {}.bak -----Failed", file_name);
            false
        };
    }

    /// Persists the configuration with a topic.
    ///
    /// This method persists the configuration with a given topic.
    /// The actual implementation is delegated to the `persist` method.
    fn persist_with_topic<T>(&mut self, _topic_name: &str, _t: T) {
        self.persist()
    }

    /// Persists the configuration with a map.
    ///
    /// This method persists the configuration with a given map.
    /// The actual implementation is delegated to the `persist` method.
    fn persist_map<T>(&mut self, _m: &HashMap<String, T>) {
        self.persist()
    }

    /// Persists the configuration.
    ///
    /// This method persists the configuration to a file whose path is returned by
    /// `config_file_path`. If the encoded configuration is not empty, it writes the
    /// configuration to the file.
    fn persist(&self) {
        let json = self.encode_pretty(true);
        if !json.is_empty() {
            let file_name = self.config_file_path();
            if FileUtils::string_to_file(json.as_str(), file_name.as_str()).is_err() {
                error!("persist file {} exception", file_name);
            }
        }
    }

    /// Decodes the configuration.
    ///
    /// This method is a placeholder for decoding the configuration.
    /// The actual implementation should be provided by the implementer of the trait.
    fn decode0(&mut self, _key: &[u8], _body: &[u8]) {
        //nothing to do
    }

    /// Stops the configuration manager.
    ///
    /// This method is a placeholder for stopping the configuration manager.
    /// The actual implementation should be provided by the implementer of the trait.
    ///
    /// # Returns
    /// * `true` by default.
    fn stop(&mut self) -> bool {
        true
    }

    /// Returns the path of the configuration file.
    ///
    /// This method is a placeholder for returning the path of the configuration file.
    /// The actual implementation should be provided by the implementer of the trait.
    fn config_file_path(&self) -> String;

    /// Encodes the configuration.
    ///
    /// This method is a placeholder for encoding the configuration.
    /// The actual implementation should be provided by the implementer of the trait.
    fn encode(&mut self) -> String {
        self.encode_pretty(false)
    }

    /// Encodes the configuration with pretty format.
    ///
    /// This method is a placeholder for encoding the configuration with pretty format.
    /// The actual implementation should be provided by the implementer of the trait.
    fn encode_pretty(&self, pretty_format: bool) -> String;

    /// Decodes the configuration from a JSON string.
    ///
    /// This method is a placeholder for decoding the configuration from a JSON string.
    /// The actual implementation should be provided by the implementer of the trait.
    fn decode(&self, json_string: &str);
}
