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

//! A stop for mutation calls that takes effect without a restart.

use std::path::Path;
use std::sync::Arc;

use crate::config::MutationPolicyConfig;

/// Refuses mutation calls while a sentinel file exists.
///
/// The configuration is read once at startup, so switching `mutations_enabled` off takes a
/// restart. This is the faster way to stop: an operator creates the configured file, and every
/// mutation call from then on is refused until the file is removed.
///
/// The stop only tightens. Without the file the configuration decides alone, so a missing file
/// never enables what the configuration left disabled.
#[derive(Debug, Clone, Default)]
pub(crate) struct EmergencyStop {
    sentinel: Option<Arc<Path>>,
}

impl EmergencyStop {
    pub(crate) fn new(policy: &MutationPolicyConfig) -> Self {
        Self {
            sentinel: policy
                .emergency_stop_file
                .as_deref()
                .map(|path| Arc::from(Path::new(path))),
        }
    }

    /// Returns whether mutation calls are refused now.
    ///
    /// The file is looked up for every call and nothing is remembered, so creating it stops the
    /// next call and removing it lifts the stop for the next call. A lookup that fails counts as
    /// a stop: the server then cannot tell whether an operator asked for one.
    pub(crate) async fn is_engaged(&self) -> bool {
        match &self.sentinel {
            None => false,
            Some(sentinel) => !matches!(tokio::fs::try_exists(sentinel).await, Ok(false)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stop_at(path: &Path) -> EmergencyStop {
        EmergencyStop {
            sentinel: Some(Arc::from(path)),
        }
    }

    #[tokio::test]
    async fn the_stop_follows_the_file_from_one_call_to_the_next() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("stop-writes");
        let stop = stop_at(&file);

        assert!(!stop.is_engaged().await);
        tokio::fs::write(&file, b"").await.unwrap();
        assert!(stop.is_engaged().await);
        // What the file holds does not matter, and a directory of that name stops as well.
        tokio::fs::write(&file, b"incident 42").await.unwrap();
        assert!(stop.is_engaged().await);
        tokio::fs::remove_file(&file).await.unwrap();
        assert!(!stop.is_engaged().await);
        tokio::fs::create_dir(&file).await.unwrap();
        assert!(stop.is_engaged().await);
    }

    #[tokio::test]
    async fn no_configured_file_never_stops_and_a_failed_lookup_does() {
        assert!(!EmergencyStop::default().is_engaged().await);
        assert!(!EmergencyStop::new(&MutationPolicyConfig::default()).is_engaged().await);

        // A name no file system accepts cannot be looked up. That is not "the file is absent".
        let unreadable = stop_at(Path::new("stop\0writes"));
        assert!(unreadable.is_engaged().await);
    }
}
