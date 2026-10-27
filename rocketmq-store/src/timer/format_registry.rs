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

//! Which file format of the Java-compatible timer store indexes new timers, and which one is
//! still delivering the timers it indexed before a switch.
//!
//! Both formats read the same Timer topic. When `timerFileFormat` changes, the format that was
//! active stops indexing at its committed Timer CQ offset; the new active format starts there,
//! and the old one keeps delivering what it holds until it is empty.

use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use rocketmq_store_api::TimerFileFormat;
use serde::Deserialize;
use serde::Serialize;

const REGISTRY_FILE: &str = "formats.json";

/// The persisted roles of the timer file formats (`timer/formats.json`).
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TimerFormatRegistry {
    /// Format that indexes new timers.
    pub(crate) active: TimerFileFormat,
    /// Format that still delivers timers it indexed before the switch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) draining: Option<TimerFileFormat>,
}

/// Formats one load opens.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimerFormatPlan {
    pub(crate) active: TimerFileFormat,
    /// Format to open for delivery only.
    pub(crate) draining: Option<TimerFileFormat>,
    /// Format whose committed Timer CQ offset the active format starts from, on a switch.
    pub(crate) handover_from: Option<TimerFileFormat>,
}

impl TimerFormatRegistry {
    fn path(root_dir: &str) -> PathBuf {
        Path::new(root_dir).join("timer").join(REGISTRY_FILE)
    }

    /// Reads the registry of the store at `root_dir`, or `None` before it is first written.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the file cannot be read or does not decode.
    pub(crate) fn load(root_dir: &str) -> std::io::Result<Option<Self>> {
        match std::fs::read(Self::path(root_dir)) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Replaces the registry of the store at `root_dir` atomically.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the file cannot be written, synced or renamed.
    pub(crate) fn persist(&self, root_dir: &str) -> std::io::Result<()> {
        let path = Self::path(root_dir);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension("json.tmp");
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(&serde_json::to_vec(self).map_err(std::io::Error::other)?)?;
        file.sync_data()?;
        drop(file);
        std::fs::rename(temporary, path)
    }
}

/// Decides which formats a load opens for the `configured` format.
///
/// A recorded active format other than `configured` is a switch: it drains, and the configured
/// format starts from its Timer CQ offset. Switching back to a format that is still draining
/// makes it active again and drains the other one. Without a registry, timer records of the
/// other format mean the store was used with it, which is a switch too.
pub(crate) fn plan_formats(
    configured: TimerFileFormat,
    registry: Option<TimerFormatRegistry>,
    other_has_records: bool,
) -> TimerFormatPlan {
    let previous = match registry {
        Some(registry) if registry.active == configured => {
            return TimerFormatPlan {
                active: configured,
                draining: registry.draining.filter(|format| *format != configured),
                handover_from: None,
            };
        }
        Some(registry) => Some(registry.active),
        None if other_has_records => Some(other_format(configured)),
        None => None,
    };
    TimerFormatPlan {
        active: configured,
        draining: previous,
        handover_from: previous,
    }
}

/// The format that is not `format`.
pub(crate) fn other_format(format: TimerFileFormat) -> TimerFileFormat {
    match format {
        TimerFileFormat::RustV2 => TimerFileFormat::Java,
        TimerFileFormat::Java => TimerFileFormat::RustV2,
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn a_new_active_format_drains_the_previous_one_and_takes_its_cursor() {
        let java = TimerFileFormat::Java;
        let v2 = TimerFileFormat::RustV2;
        let unchanged = TimerFormatRegistry {
            active: java,
            draining: Some(v2),
        };
        assert_eq!(
            plan_formats(java, Some(unchanged), true),
            TimerFormatPlan {
                active: java,
                draining: Some(v2),
                handover_from: None
            }
        );
        // Switching back to the format still draining.
        assert_eq!(
            plan_formats(v2, Some(unchanged), true),
            TimerFormatPlan {
                active: v2,
                draining: Some(java),
                handover_from: Some(java)
            }
        );
        // A store used before the registry existed.
        assert_eq!(
            plan_formats(v2, None, true),
            TimerFormatPlan {
                active: v2,
                draining: Some(java),
                handover_from: Some(java)
            }
        );
        assert_eq!(
            plan_formats(v2, None, false),
            TimerFormatPlan {
                active: v2,
                draining: None,
                handover_from: None
            }
        );
    }

    #[test]
    fn the_registry_round_trips_through_its_file() {
        let directory = tempdir().unwrap();
        let root = directory.path().to_string_lossy().into_owned();
        assert_eq!(TimerFormatRegistry::load(&root).unwrap(), None);
        let registry = TimerFormatRegistry {
            active: TimerFileFormat::Java,
            draining: Some(TimerFileFormat::RustV2),
        };
        registry.persist(&root).unwrap();
        assert_eq!(TimerFormatRegistry::load(&root).unwrap(), Some(registry));
        let text = std::fs::read_to_string(directory.path().join("timer").join("formats.json")).unwrap();
        assert_eq!(text, r#"{"active":"java","draining":"rust_v2"}"#);

        let settled = TimerFormatRegistry {
            active: TimerFileFormat::Java,
            draining: None,
        };
        settled.persist(&root).unwrap();
        assert_eq!(TimerFormatRegistry::load(&root).unwrap(), Some(settled));
    }
}
