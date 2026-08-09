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

use std::path::Path;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerMigrationLayout {
    Empty,
    V1Only,
    V2Complete,
    IncompleteV2,
}

pub fn detect_timer_log_layout(
    timer_log_root: &Path,
    legacy_file_name: &str,
    v2_directory_name: &str,
    committed_marker: &str,
) -> std::io::Result<TimerMigrationLayout> {
    let v2_root = timer_log_root.join(v2_directory_name);
    if v2_root.join(committed_marker).exists() {
        return Ok(TimerMigrationLayout::V2Complete);
    }
    if v2_root.exists() {
        return Ok(TimerMigrationLayout::IncompleteV2);
    }
    let legacy = timer_log_root.join(legacy_file_name);
    if legacy.exists() && legacy.metadata()?.len() > 0 {
        Ok(TimerMigrationLayout::V1Only)
    } else {
        Ok(TimerMigrationLayout::Empty)
    }
}

/// Returns an empty, retry-safe temporary migration directory.
pub fn reset_migration_directory(timer_log_root: &Path, directory_name: &str) -> std::io::Result<PathBuf> {
    let path = timer_log_root.join(directory_name);
    if path.exists() {
        std::fs::remove_dir_all(&path)?;
    }
    std::fs::create_dir_all(&path)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn layout_detection_fails_closed_for_uncommitted_v2() {
        let directory = tempdir().unwrap();
        std::fs::create_dir_all(directory.path().join("v2")).unwrap();
        assert_eq!(
            detect_timer_log_layout(directory.path(), "legacy", "v2", "COMMITTED").unwrap(),
            TimerMigrationLayout::IncompleteV2
        );
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(directory.path().join("v2/COMMITTED"))
            .unwrap()
            .write_all(b"ok")
            .unwrap();
        assert_eq!(
            detect_timer_log_layout(directory.path(), "legacy", "v2", "COMMITTED").unwrap(),
            TimerMigrationLayout::V2Complete
        );
    }
}
