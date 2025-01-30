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

use crate::common::mix_all::ROCKETMQ_HOME_ENV;

/// Utility functions related to environment variables.
pub struct EnvUtils;

impl EnvUtils {
    /// Gets the value of the specified environment variable.
    ///
    /// # Arguments
    ///
    /// * `key` - The name of the environment variable to retrieve.
    ///
    /// # Returns
    ///
    /// An `Option` containing the value of the environment variable, or `None` if the variable is
    /// not set.
    pub fn get_property(key: impl Into<String>) -> Option<String> {
        std::env::var(key.into()).ok()
    }

    /// Gets the value of the ROCKETMQ_HOME environment variable.
    ///
    /// If ROCKETMQ_HOME is not set, it defaults to the current directory and sets ROCKETMQ_HOME
    /// accordingly.
    ///
    /// # Returns
    ///
    /// The value of the ROCKETMQ_HOME environment variable as a `String`.
    pub fn get_rocketmq_home() -> String {
        std::env::var(ROCKETMQ_HOME_ENV).unwrap_or_else(|_| {
            // If ROCKETMQ_HOME is not set, use the current directory as the default value
            let rocketmq_home_dir = std::env::current_dir()
                .unwrap()
                .into_os_string()
                .to_string_lossy()
                .to_string();

            // Set ROCKETMQ_HOME to the current directory
            std::env::set_var(ROCKETMQ_HOME_ENV, rocketmq_home_dir.clone());
            rocketmq_home_dir
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_get_property_existing_variable() {
        // Set up
        let key = "HOME";
        let expected_value = "/home/user";

        std::env::set_var(key, expected_value);

        // Test
        let result = EnvUtils::get_property(key);

        // Assert
        assert_eq!(result, Some(expected_value.to_string()));
    }

    #[test]
    fn test_get_property_non_existing_variable() {
        // Set up
        let key = "NON_EXISTING_VARIABLE";

        // Test
        let result = EnvUtils::get_property(key);

        // Assert
        assert_eq!(result, None);
    }

    /*    #[test]
    fn test_get_rocketmq_home_existing_variable() {
        // Set up
        let expected_value = PathBuf::from("/path/to/rocketmq_home");

        std::env::set_var(ROCKETMQ_HOME_ENV, expected_value.clone());

        // Test
        let result = EnvUtils::get_rocketmq_home();

        // Assert
        assert_eq!(result, expected_value.to_string_lossy().to_string());
    }*/

    #[test]
    fn test_get_rocketmq_home_non_existing_variable() {
        // Set up
        std::env::remove_var(ROCKETMQ_HOME_ENV);

        // Test
        let result = EnvUtils::get_rocketmq_home();

        // Assert
        assert_eq!(
            result,
            std::env::current_dir()
                .unwrap()
                .to_string_lossy()
                .to_string()
        );
    }
}
