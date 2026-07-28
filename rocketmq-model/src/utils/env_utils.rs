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

    /// Gets the value of the ROCKETMQ_HOME environment variable.
    ///
    /// If ROCKETMQ_HOME is not set, it defaults to the current directory without
    /// mutating the process environment.
    ///
    /// # Returns
    ///
    /// The value of the ROCKETMQ_HOME environment variable as a `String`.
    pub fn get_rocketmq_home() -> String {
        std::env::var(ROCKETMQ_HOME_ENV).unwrap_or_else(|_| {
            std::env::current_dir()
                .ok()
                .and_then(|path| path.into_os_string().into_string().ok())
                .unwrap_or_else(|| ".".to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::sync::Mutex;

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvironmentRestore {
        key: &'static str,
        original: Option<OsString>,
    }

    impl Drop for EnvironmentRestore {
        fn drop(&mut self) {
            // SAFETY: ENV_LOCK serializes every environment mutation in this
            // module, and the guard restores the original value before release.
            unsafe {
                match &self.original {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    fn with_environment<R>(key: &'static str, value: Option<&str>, test: impl FnOnce() -> R) -> R {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let restore = EnvironmentRestore {
            key,
            original: std::env::var_os(key),
        };
        // SAFETY: ENV_LOCK prevents concurrent mutations made by this module;
        // EnvironmentRestore restores the original process value after the test.
        unsafe {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        let result = test();
        drop(restore);
        result
    }

    #[test]
    fn test_get_property_existing_variable() {
        with_environment("ROCKETMQ_ENV_UTILS_EXISTING", Some("expected"), || {
            assert_eq!(
                EnvUtils::get_property("ROCKETMQ_ENV_UTILS_EXISTING"),
                Some("expected".to_string())
            );
        });
    }

    #[test]
    fn test_get_property_non_existing_variable() {
        // Set up
        with_environment("ROCKETMQ_ENV_UTILS_MISSING", None, || {
            assert_eq!(EnvUtils::get_property("ROCKETMQ_ENV_UTILS_MISSING"), None);
        });
    }

    #[test]
    fn rocketmq_home_fallback_does_not_write_the_environment() {
        with_environment(ROCKETMQ_HOME_ENV, None, || {
            let result = EnvUtils::get_rocketmq_home();

            assert_eq!(result, std::env::current_dir().unwrap().to_string_lossy());
            assert_eq!(std::env::var_os(ROCKETMQ_HOME_ENV), None);
        });
    }

    #[test]
    fn retrieves_env_variable_value() {
        with_environment("ROCKETMQ_ENV_UTILS_STRING", Some("test_value"), || {
            assert_eq!(
                EnvUtils::get_property_or_default("ROCKETMQ_ENV_UTILS_STRING", "default_value"),
                "test_value"
            );
        });
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
        with_environment("ROCKETMQ_ENV_UTILS_INT", Some("42"), || {
            assert_eq!(EnvUtils::get_property_as_i32("ROCKETMQ_ENV_UTILS_INT", 0), 42);
        });
    }

    #[test]
    fn returns_default_when_env_variable_as_i32_not_set() {
        assert_eq!(EnvUtils::get_property_as_i32("NON_EXISTENT_INT_KEY", 10), 10);
    }

    #[test]
    fn returns_default_when_env_variable_as_i32_invalid() {
        with_environment("ROCKETMQ_ENV_UTILS_INVALID_INT", Some("not_a_number"), || {
            assert_eq!(EnvUtils::get_property_as_i32("ROCKETMQ_ENV_UTILS_INVALID_INT", 5), 5);
        });
    }

    #[test]
    fn returns_default_when_env_variable_as_bool_not_set() {
        assert!(EnvUtils::get_property_as_bool("NON_EXISTENT_BOOL_KEY", true));
        assert!(!EnvUtils::get_property_as_bool("NON_EXISTENT_BOOL_KEY", false));
    }

    #[test]
    fn returns_default_when_env_variable_as_bool_invalid() {
        with_environment("ROCKETMQ_ENV_UTILS_INVALID_BOOL", Some("not_a_bool"), || {
            assert!(EnvUtils::get_property_as_bool("ROCKETMQ_ENV_UTILS_INVALID_BOOL", true));
            assert!(!EnvUtils::get_property_as_bool(
                "ROCKETMQ_ENV_UTILS_INVALID_BOOL",
                false
            ));
        });
    }
}
