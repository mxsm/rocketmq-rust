// Copyright 2026 The RocketMQ Rust Authors
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

/// An MCP operational failure with a fixed, safe public projection.
///
/// Sources remain available for typed inspection through [`std::error::Error::source`].
/// Source formatting is private diagnostic data and must never be sent to clients or logs.
/// The one addition to the fixed text is the name and location of an unknown configuration
/// key, which carries no configured value.
pub struct McpError {
    kind: ErrorKind,
    unknown_key: Option<UnknownConfigKey>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// An unknown configuration key, reduced to plain names so that it can be shown.
struct UnknownConfigKey {
    key: String,
    /// Dotted path of the table that holds the key; `None` at the top level.
    section: Option<String>,
}

impl UnknownConfigKey {
    /// Extracts the key that serde rejected, or nothing when any part is not a plain name.
    ///
    /// TOML and YAML accept arbitrary text in a quoted key, so only names made of ASCII
    /// letters, digits, `_`, `-`, and the index and path punctuation `[`, `]`, `.` are kept.
    fn from_config_error(error: &config::ConfigError) -> Option<Self> {
        let (section, message) = match error {
            config::ConfigError::At { error, key, .. } => match error.as_ref() {
                config::ConfigError::Message(message) => (key.as_deref(), message.as_str()),
                _ => return None,
            },
            config::ConfigError::Message(message) => (None, message.as_str()),
            _ => return None,
        };
        let key = message.strip_prefix("unknown field `")?.split('`').next()?;
        let plain = |name: &str, punctuation: &[u8]| {
            (1..=128).contains(&name.len())
                && name.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-') || punctuation.contains(&byte)
                })
        };
        if !plain(key, b"") || section.is_some_and(|section| !plain(section, b"[].")) {
            return None;
        }
        Some(Self {
            key: key.to_owned(),
            section: section.map(ToOwned::to_owned),
        })
    }
}

pub type McpResult<T> = Result<T, McpError>;

enum ErrorKind {
    Configuration,
    UnsupportedTransport,
    FeatureDisabled,
    Operational,
}

impl McpError {
    pub(crate) fn into_error_data(self) -> rmcp::ErrorData {
        rmcp::ErrorData::internal_error(self.to_string(), None)
    }
    /// Retains an operational source without exposing its text in Display or Debug.
    pub fn from_source(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            kind: ErrorKind::Operational,
            unknown_key: None,
            source: Some(Box::new(source)),
        }
    }

    pub(crate) fn invalid_config(_detail: String) -> Self {
        Self {
            kind: ErrorKind::Configuration,
            unknown_key: None,
            source: None,
        }
    }

    pub(crate) fn unsupported_transport(_detail: String) -> Self {
        Self {
            kind: ErrorKind::UnsupportedTransport,
            unknown_key: None,
            source: None,
        }
    }

    pub fn feature_disabled() -> Self {
        Self {
            kind: ErrorKind::FeatureDisabled,
            unknown_key: None,
            source: None,
        }
    }

    pub(crate) fn infrastructure(
        _operation: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::from_source(source)
    }
}

impl From<config::ConfigError> for McpError {
    fn from(source: config::ConfigError) -> Self {
        Self {
            kind: ErrorKind::Configuration,
            unknown_key: UnknownConfigKey::from_config_error(&source),
            source: Some(Box::new(source)),
        }
    }
}

impl std::fmt::Display for McpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.kind {
            ErrorKind::Configuration => "MCP configuration is invalid",
            ErrorKind::UnsupportedTransport => "MCP transport is unsupported",
            ErrorKind::FeatureDisabled => "MCP transport feature is disabled",
            ErrorKind::Operational => "MCP operation failed",
        })?;
        match &self.unknown_key {
            Some(UnknownConfigKey {
                key,
                section: Some(section),
            }) => write!(f, ": unknown key `{key}` in `{section}`"),
            Some(UnknownConfigKey { key, section: None }) => write!(f, ": unknown key `{key}`"),
            None => Ok(()),
        }
    }
}

impl std::fmt::Debug for McpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("McpError").field(&self.to_string()).finish()
    }
}

impl std::error::Error for McpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::McpError;

    #[test]
    fn infrastructure_error_preserves_its_source() {
        let error = McpError::infrastructure("bind HTTP listener", std::io::Error::other("busy"));

        assert_eq!("MCP operation failed", error.to_string());
        assert!(!format!("{error:?}").contains("busy"));
        assert!(error.source().unwrap().is::<std::io::Error>());
        assert_eq!(Some("busy"), error.source().map(ToString::to_string).as_deref());
    }
}
