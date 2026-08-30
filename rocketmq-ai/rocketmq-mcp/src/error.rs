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

#[derive(Debug, thiserror::Error)]
pub enum McpError {
    #[error("configuration error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("unsupported transport: {0}")]
    UnsupportedTransport(String),

    #[error("{operation} failed")]
    Infrastructure {
        operation: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("transport `{transport}` requires Cargo feature `{feature}`")]
    FeatureDisabled {
        transport: &'static str,
        feature: &'static str,
    },
}

impl McpError {
    pub(crate) fn infrastructure(
        operation: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Infrastructure {
            operation,
            source: Box::new(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::McpError;

    #[test]
    fn infrastructure_error_preserves_its_source() {
        let error = McpError::infrastructure("bind HTTP listener", std::io::Error::other("busy"));

        assert_eq!("bind HTTP listener failed", error.to_string());
        assert_eq!(Some("busy"), error.source().map(ToString::to_string).as_deref());
    }
}
