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

use std::fmt;
use std::fmt::Debug;
use std::path::PathBuf;

use config::Config;
use serde::Deserialize;

use crate::RuntimeError;
use crate::RuntimeResult;

/// A configuration error summary that contains no source value, location, or upstream diagnostic.
#[derive(Clone, Eq, PartialEq)]
pub struct RedactedConfigError {
    message: String,
}

impl fmt::Display for RedactedConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl fmt::Debug for RedactedConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl std::error::Error for RedactedConfigError {}

/// Redacts a [`config::ConfigError`] into a stable, safe diagnostic category.
#[must_use]
pub fn render_safe_config_error(error: &config::ConfigError) -> RedactedConfigError {
    let message = match error {
        config::ConfigError::Frozen => "configuration is frozen".to_string(),
        config::ConfigError::NotFound(_) => "configuration field is missing".to_string(),
        config::ConfigError::PathParse { .. } => "configuration path parse failed".to_string(),
        config::ConfigError::FileParse { .. } => "configuration file parse failed".to_string(),
        config::ConfigError::Type { .. } => "configuration value has an invalid type".to_string(),
        config::ConfigError::At { error, .. } => return render_safe_config_error(error),
        config::ConfigError::Message(message) => {
            safe_unknown_field_message(message).unwrap_or_else(|| "configuration processing failed".to_string())
        }
        config::ConfigError::Foreign(_) => "configuration processing failed".to_string(),
        _ => "configuration processing failed".to_string(),
    };
    RedactedConfigError { message }
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
        .map_err(|error| RuntimeError::Configuration(render_safe_config_error(&error).to_string()))?;
    let config_file = cfg
        .try_deserialize::<C>()
        .map_err(|error| RuntimeError::Configuration(render_safe_config_error(&error).to_string()))?;
    Ok(config_file)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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

        let output = render_safe_config_error(&error).to_string();

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

        let output = render_safe_config_error(&error).to_string();

        assert!(!output.contains("RUNTIME_ENDPOINT_CANARY"));
        assert_eq!(output, "configuration file parse failed");
    }
}
