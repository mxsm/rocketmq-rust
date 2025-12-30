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
use std::any::Any;
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
        if let Ok(ref content) = FileUtils::file_to_string(format!("{}{}", file_name, ".bak").as_str()) {
            if !content.is_empty() {
                self.decode(content);
                info!("load Config file: {}.bak -----OK", file_name);
            }
            true
        } else {
            error!("load Config file: {}.bak -----Failed", file_name);
            false
        }
    }

    /// Persists the configuration with a topic.
    ///
    /// This method persists the configuration with a given topic.
    /// The actual implementation is delegated to the `persist` method.
    fn persist_with_topic(&mut self, _topic_name: &str, _t: Box<dyn Any>) {
        self.persist()
    }

    /// Persists the configuration with a map.
    ///
    /// This method persists the configuration with a given map.
    /// The actual implementation is delegated to the `persist` method.
    fn persist_map(&mut self, _m: &HashMap<String, Box<dyn Any>>) {
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

    /// Returns the file path for the configuration file.
    ///
    /// This method should be implemented to return the path of the configuration file
    /// that the `ConfigManager` will use to load or persist the configuration.
    ///
    /// # Returns
    /// A `String` representing the path of the configuration file.
    fn config_file_path(&self) -> String;

    /// Encodes the current configuration into a `String`.
    ///
    /// This method leverages `encode_pretty` with `pretty_format` set to `false` to encode
    /// the current configuration into a compact `String` representation.
    ///
    /// # Returns
    /// A `String` representing the encoded configuration in a compact format.
    fn encode(&self) -> String {
        self.encode_pretty(false)
    }

    /// Encodes the current configuration into a `String` with an option for pretty formatting.
    ///
    /// This method encodes the current configuration into a `String`. It offers an option to
    /// format the output in a more readable (pretty) format if `pretty_format` is `true`.
    ///
    /// # Arguments
    /// * `pretty_format` - A boolean indicating whether the output should be pretty formatted.
    ///
    /// # Returns
    /// A `String` representing the encoded configuration, optionally in a pretty format.
    fn encode_pretty(&self, pretty_format: bool) -> String;

    /// Decodes the configuration from a JSON string.
    ///
    /// This method takes a JSON string representation of the configuration and decodes it
    /// into the internal representation used by the `ConfigManager`. Implementations should
    /// update the internal state based on the provided JSON string.
    ///
    /// # Arguments
    /// * `json_string` - A `&str` representing the configuration in JSON format.
    fn decode(&self, json_string: &str);
}
