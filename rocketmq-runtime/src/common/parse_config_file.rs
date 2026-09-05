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

use std::fmt::Debug;
use std::path::PathBuf;

use config::Config;
use serde::Deserialize;

use crate::RuntimeError;
use crate::RuntimeOperation;
use crate::RuntimeResult;

/// Redacts a [`config::ConfigError`] into a stable, safe diagnostic category.
#[must_use]
pub fn render_safe_config_error(error: &config::ConfigError) -> String {
    match error {
        config::ConfigError::Frozen => "configuration is frozen".to_string(),
        config::ConfigError::NotFound(_) => "configuration field is missing".to_string(),
        config::ConfigError::PathParse { .. } => "configuration path parse failed".to_string(),
        config::ConfigError::FileParse { .. } => "configuration file parse failed".to_string(),
        config::ConfigError::Type { .. } => "configuration value has an invalid type".to_string(),
        config::ConfigError::At { error, .. } => render_safe_config_error(error),
        config::ConfigError::Message(message) => {
            safe_unknown_field_message(message).unwrap_or_else(|| "configuration processing failed".to_string())
        }
        config::ConfigError::Foreign(_) => "configuration processing failed".to_string(),
        _ => "configuration processing failed".to_string(),
    }
}

fn safe_unknown_field_message(message: &str) -> Option<String> {
    let (rest, delimiter) = message
        .strip_prefix("unknown field `")
        .map(|rest| (rest, '`'))
        .or_else(|| message.strip_prefix("unknown field \"").map(|rest| (rest, '"')))?;
    let field = rest.split_once(delimiter)?.0;
    if field.is_empty()
        || field.len() > 128
        || !field
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return None;
    }
    Some(format!("unknown configuration field `{field}`"))
}

/// Parses config file.
pub fn parse_config_file<'de, C>(config_file: PathBuf) -> RuntimeResult<C>
where
    C: Debug + Deserialize<'de>,
{
    let cfg = Config::builder()
        .add_source(config::File::from(config_file.as_path()))
        .build()
        .map_err(|error| RuntimeError::configuration_failure(RuntimeOperation::LoadConfigFile, error))?;
    let config_file = cfg
        .try_deserialize::<C>()
        .map_err(|error| RuntimeError::configuration_failure(RuntimeOperation::DeserializeConfigFile, error))?;
    Ok(config_file)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;

    use super::*;

    #[test]
    fn safe_config_error_renderer_redacts_invalid_values() {
        let config = Config::builder()
            .add_source(config::File::from_str(
                "headers = \"RUNTIME_HEADER_CANARY\"",
                config::FileFormat::Toml,
            ))
            .build()
            .expect("test configuration should parse");
        let error = config
            .try_deserialize::<HashMap<String, HashMap<String, String>>>()
            .expect_err("wrong-shaped headers must fail");

        let output = render_safe_config_error(&error);

        assert!(!output.contains("RUNTIME_HEADER_CANARY"));
        assert_eq!(output, "configuration processing failed");
    }

    #[test]
    fn safe_config_error_renderer_redacts_malformed_source_lines() {
        let error = Config::builder()
            .add_source(config::File::from_str(
                "endpoint = \"https://collector.invalid?token=RUNTIME_ENDPOINT_CANARY\" trailing",
                config::FileFormat::Toml,
            ))
            .build()
            .expect_err("malformed TOML must fail");

        let output = render_safe_config_error(&error);

        assert!(!output.contains("RUNTIME_ENDPOINT_CANARY"));
        assert_eq!(output, "configuration file parse failed");
    }

    #[test]
    fn config_file_load_failure_preserves_the_config_source() {
        let directory = tempfile::tempdir().expect("temporary config directory");
        let error = parse_config_file::<HashMap<String, bool>>(directory.path().join("missing.toml"))
            .expect_err("missing configuration must fail");

        assert_eq!(error.operation(), RuntimeOperation::LoadConfigFile);
        assert!(std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<config::ConfigError>())
            .is_some());
    }

    #[test]
    fn config_file_deserialization_failure_preserves_the_config_source() {
        let directory = tempfile::tempdir().expect("temporary config directory");
        let path = directory.path().join("invalid.toml");
        fs::write(&path, "enabled = \"not-a-bool\"\n").expect("write invalid config");

        let error = parse_config_file::<HashMap<String, bool>>(path).expect_err("invalid configuration must fail");

        assert_eq!(error.operation(), RuntimeOperation::DeserializeConfigFile);
        assert!(std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<config::ConfigError>())
            .is_some());
    }
}
