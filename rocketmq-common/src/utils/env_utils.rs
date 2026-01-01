// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ffi::OsStr;

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
    pub fn get_property<K: AsRef<OsStr>>(key: K) -> Option<String> {
        std::env::var(key).ok()
    }

    /// Retrieves the value of the specified environment variable or returns a default value if the
    /// variable is not set.
    ///
    /// # Arguments
    /// * `key` - The name of the environment variable to retrieve.
    /// * `default` - The default value to return if the environment variable is not set.
    ///
    /// # Returns
    /// A `String` containing the value of the environment variable, or the default value.
    pub fn get_property_or_default<K: AsRef<OsStr>>(key: K, default: impl Into<String>) -> String {
        std::env::var(key).unwrap_or_else(|_| default.into())
    }

    /// Retrieves the value of the specified environment variable as an `i32`, or returns a default
    /// value if the variable is not set or cannot be parsed.
    ///
    /// # Arguments
    /// * `key` - The name of the environment variable to retrieve.
    /// * `default` - The default value to return if the environment variable is not set or cannot
    ///   be parsed.
    ///
    /// # Returns
    /// An `i32` containing the value of the environment variable, or the default value.
    pub fn get_property_as_i32<K: AsRef<OsStr>>(key: K, default: i32) -> i32 {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(default)
    }

    /// Retrieves the value of the specified environment variable as a `bool`, or returns a default
    /// value if the variable is not set or cannot be parsed.
    ///
    /// # Arguments
    /// * `key` - The name of the environment variable to retrieve.
    /// * `default` - The default value to return if the environment variable is not set or cannot
    ///   be parsed.
    ///
    /// # Returns
    /// A `bool` containing the value of the environment variable, or the default value.
    ///
    /// # Notes
    /// The function considers the following values as `true`: `"true"`, `"1"`.
    /// The function considers the following values as `false`: `"false"`, `"0"`.
    pub fn get_property_as_bool<K: AsRef<OsStr>>(key: K, default: bool) -> bool {
        std::env::var(key)
            .ok()
            .and_then(|v| {
                let lower = v.to_lowercase();
                match lower.as_str() {
                    "true" | "1" => Some(true),
                    "false" | "0" => Some(false),
                    _ => None,
                }
            })
            .unwrap_or(default)
    }

    /// Sets the value of the specified environment variable.
    ///
    /// # Arguments
    ///
    /// * `key` - The name of the environment variable to set.
    /// * `value` - The value to set the environment variable to.
    ///
    /// # Safety
    ///
    /// This function uses `unsafe` because it modifies the environment variables,
    /// which can have side effects on the entire process.
    pub fn put_property<K: AsRef<OsStr>, V: AsRef<OsStr>>(key: K, value: V) {
        unsafe {
            std::env::set_var(key, value);
        }
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
        std::env::var(ROCKETMQ_HOME_ENV).unwrap_or_else(|_| unsafe {
            // If ROCKETMQ_HOME is not set, use the current directory as the default value
            let rocketmq_home_dir = std::env::current_dir()
                .ok()
                .and_then(|p| p.into_os_string().into_string().ok())
                .unwrap_or_else(|| ".".to_string());

            // Set ROCKETMQ_HOME to the current directory
            std::env::set_var(ROCKETMQ_HOME_ENV, &rocketmq_home_dir);
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

        unsafe {
            std::env::set_var(key, expected_value);
        }

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
        unsafe {
            std::env::remove_var(ROCKETMQ_HOME_ENV);
        }

        // Test
        let result = EnvUtils::get_rocketmq_home();

        // Assert
        assert_eq!(result, std::env::current_dir().unwrap().to_string_lossy().to_string());
    }

    #[test]
    fn put_property_sets_value() {
        let key = "TEST_ENV_VAR";
        let value = "test_value";

        EnvUtils::put_property(key, value);

        let result = std::env::var(key).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn put_property_overwrites_existing_value() {
        let key = "TEST_ENV_VAR1";
        let initial_value = "initial_value";
        let new_value = "new_value";

        unsafe {
            std::env::set_var(key, initial_value);
        }

        EnvUtils::put_property(key, new_value);

        let result = std::env::var(key).unwrap();
        assert_eq!(result, new_value);
    }

    #[test]
    fn put_property_handles_empty_value() {
        let key = "TEST_ENV_VAR2";
        let value = "";

        EnvUtils::put_property(key, value);

        let result = std::env::var(key).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn retrieves_env_variable_value() {
        std::env::set_var("TEST_KEY", "test_value");
        assert_eq!(
            EnvUtils::get_property_or_default("TEST_KEY", "default_value"),
            "test_value"
        );
        std::env::remove_var("TEST_KEY");
    }

    #[test]
    fn returns_default_when_env_variable_not_set() {
        assert_eq!(
            EnvUtils::get_property_or_default("NON_EXISTENT_KEY", "default_value"),
            "default_value"
        );
    }

    #[test]
    fn retrieves_env_variable_as_i32() {
        std::env::set_var("TEST_INT_KEY", "42");
        assert_eq!(EnvUtils::get_property_as_i32("TEST_INT_KEY", 0), 42);
        std::env::remove_var("TEST_INT_KEY");
    }

    #[test]
    fn returns_default_when_env_variable_as_i32_not_set() {
        assert_eq!(EnvUtils::get_property_as_i32("NON_EXISTENT_INT_KEY", 10), 10);
    }

    #[test]
    fn returns_default_when_env_variable_as_i32_invalid() {
        std::env::set_var("INVALID_INT_KEY", "not_a_number");
        assert_eq!(EnvUtils::get_property_as_i32("INVALID_INT_KEY", 5), 5);
        std::env::remove_var("INVALID_INT_KEY");
    }

    #[test]
    fn returns_default_when_env_variable_as_bool_not_set() {
        assert!(EnvUtils::get_property_as_bool("NON_EXISTENT_BOOL_KEY", true));
        assert!(!EnvUtils::get_property_as_bool("NON_EXISTENT_BOOL_KEY", false));
    }

    #[test]
    fn returns_default_when_env_variable_as_bool_invalid() {
        std::env::set_var("INVALID_BOOL_KEY", "not_a_bool");
        assert!(EnvUtils::get_property_as_bool("INVALID_BOOL_KEY", true));
        assert!(!EnvUtils::get_property_as_bool("INVALID_BOOL_KEY", false));
        std::env::remove_var("INVALID_BOOL_KEY");
    }
}
