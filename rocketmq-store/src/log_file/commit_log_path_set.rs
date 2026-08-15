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

//! CommitLog path normalization, recovery inventory, and allocation policy.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;

/// Store I/O boundary used by deterministic failure tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StoreFaultPoint {
    CreateSegment,
    Preallocate,
    Append,
    Flush,
    Read,
    Truncate,
    Delete,
}

/// A per-Store fault source. Production uses [`NoopStoreFaultInjector`].
pub trait StoreFaultInjector: Send + Sync + std::fmt::Debug {
    fn should_fail(&self, point: StoreFaultPoint, root: &Path) -> bool;
}

#[derive(Debug, Default)]
struct NoopStoreFaultInjector;

impl StoreFaultInjector for NoopStoreFaultInjector {
    fn should_fail(&self, _point: StoreFaultPoint, _root: &Path) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitLogSegmentOwner {
    pub(crate) offset: u64,
    pub(crate) root: PathBuf,
    pub(crate) path: PathBuf,
}

#[derive(Debug)]
struct CommitLogPathState {
    last_selected: Option<usize>,
    unhealthy: BTreeSet<PathBuf>,
    retired: BTreeSet<PathBuf>,
}

/// Validated CommitLog roots shared by recovery and allocation.
#[derive(Debug)]
pub(crate) struct CommitLogPathSet {
    roots: Vec<PathBuf>,
    writable: BTreeSet<PathBuf>,
    readonly: BTreeSet<PathBuf>,
    minimum_remaining_bytes: u64,
    state: Mutex<CommitLogPathState>,
    fault_injector: Arc<dyn StoreFaultInjector>,
}

impl CommitLogPathSet {
    pub(crate) fn try_new(
        writable: impl IntoIterator<Item = PathBuf>,
        readonly: impl IntoIterator<Item = PathBuf>,
        minimum_remaining_bytes: u64,
    ) -> io::Result<Self> {
        Self::try_new_with_fault_injector(
            writable,
            readonly,
            minimum_remaining_bytes,
            Arc::new(NoopStoreFaultInjector),
        )
    }

    pub(crate) fn try_new_with_fault_injector(
        writable: impl IntoIterator<Item = PathBuf>,
        readonly: impl IntoIterator<Item = PathBuf>,
        minimum_remaining_bytes: u64,
        fault_injector: Arc<dyn StoreFaultInjector>,
    ) -> io::Result<Self> {
        let mut writable_roots = BTreeSet::new();
        for root in writable {
            fs::create_dir_all(&root)?;
            writable_roots.insert(fs::canonicalize(root)?);
        }
        if writable_roots.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "CommitLog requires at least one writable root",
            ));
        }

        let mut readonly_roots = BTreeSet::new();
        for root in readonly {
            let canonical = fs::canonicalize(&root).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("read-only CommitLog root {} is unavailable: {error}", root.display()),
                )
            })?;
            readonly_roots.insert(canonical);
        }

        let writable_roots = writable_roots
            .difference(&readonly_roots)
            .cloned()
            .collect::<BTreeSet<_>>();
        if writable_roots.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "all configured CommitLog roots are read-only",
            ));
        }
        let mut roots = writable_roots.union(&readonly_roots).cloned().collect::<Vec<_>>();
        roots.sort();

        Ok(Self {
            roots,
            writable: writable_roots,
            readonly: readonly_roots,
            minimum_remaining_bytes,
            state: Mutex::new(CommitLogPathState {
                last_selected: None,
                unhealthy: BTreeSet::new(),
                retired: BTreeSet::new(),
            }),
            fault_injector,
        })
    }

    pub(crate) fn is_multipath(&self) -> bool {
        self.roots.len() > 1 || !self.readonly.is_empty()
    }

    pub(crate) fn roots(&self) -> &[PathBuf] {
        &self.roots
    }

    pub(crate) fn primary_writable_root(&self) -> &Path {
        self.writable.first().expect("validated writable CommitLog root")
    }

    pub(crate) fn scan_segments(&self, mapped_file_size: u64) -> io::Result<Vec<CommitLogSegmentOwner>> {
        let mut owners = BTreeMap::<u64, CommitLogSegmentOwner>::new();
        for root in &self.roots {
            let entries = fs::read_dir(root).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("failed to enumerate CommitLog root {}: {error}", root.display()),
                )
            })?;
            for entry in entries {
                let path = entry?.path();
                let metadata = fs::symlink_metadata(&path)?;
                if !metadata.file_type().is_file() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("non-file entry blocks CommitLog recovery: {}", path.display()),
                    ));
                }
                let offset = parse_segment_offset(&path)?;
                let owner = CommitLogSegmentOwner {
                    offset,
                    root: root.clone(),
                    path: path.clone(),
                };
                if let Some(existing) = owners.insert(offset, owner) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "duplicate CommitLog segment owner for offset {offset}: {} and {}",
                            existing.path.display(),
                            path.display()
                        ),
                    ));
                }
            }
        }

        let owners = owners.into_values().collect::<Vec<_>>();
        if mapped_file_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "CommitLog mapped file size must be non-zero",
            ));
        }
        for pair in owners.windows(2) {
            let expected = pair[0]
                .offset
                .checked_add(mapped_file_size)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "CommitLog segment offset overflow"))?;
            if pair[1].offset != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "non-tail CommitLog gap or overlap: expected {expected}, found {}",
                        pair[1].offset
                    ),
                ));
            }
        }
        Ok(owners)
    }

    pub(crate) fn creation_candidates(&self, point: StoreFaultPoint) -> io::Result<Vec<PathBuf>> {
        let state = self.state.lock();
        let mut candidates = self
            .roots
            .iter()
            .enumerate()
            .filter(|(_, root)| {
                self.writable.contains(*root) && !state.retired.contains(*root) && !state.unhealthy.contains(*root)
            })
            .filter_map(|(index, root)| {
                if self.fault_injector.should_fail(point, root) {
                    return None;
                }
                let total = fs2::total_space(root).ok()?;
                let available = fs2::available_space(root).ok()?;
                if total == 0 || available < self.minimum_remaining_bytes {
                    return None;
                }
                let used = total.saturating_sub(available);
                let ratio = (u128::from(used) * 1_000_000_u128) / u128::from(total);
                Some((ratio, index, root.clone()))
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "no healthy writable CommitLog root has sufficient free space",
            ));
        }

        let start = state.last_selected.map_or(0, |last| (last + 1) % self.roots.len());
        candidates.sort_by_key(|(ratio, index, path)| {
            (*ratio, rotation_distance(*index, start, self.roots.len()), path.clone())
        });
        Ok(candidates.into_iter().map(|(_, _, root)| root).collect())
    }

    pub(crate) fn record_selected(&self, root: &Path) {
        if let Some(index) = self.roots.iter().position(|candidate| candidate == root) {
            self.state.lock().last_selected = Some(index);
        }
    }

    pub(crate) fn mark_unhealthy(&self, root: &Path) {
        self.state.lock().unhealthy.insert(root.to_path_buf());
    }

    pub(crate) fn retire(&self, root: &Path) -> io::Result<()> {
        let canonical = fs::canonicalize(root)?;
        if !self.roots.contains(&canonical) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("CommitLog root is not configured: {}", root.display()),
            ));
        }
        self.state.lock().retired.insert(canonical);
        Ok(())
    }

    pub(crate) fn should_fail(&self, point: StoreFaultPoint, path: &Path) -> bool {
        path.parent()
            .is_some_and(|root| self.fault_injector.should_fail(point, root))
    }
}

fn rotation_distance(index: usize, start: usize, len: usize) -> usize {
    if index >= start {
        index - start
    } else {
        len - start + index
    }
}

fn parse_segment_offset(path: &Path) -> io::Result<u64> {
    let name = path.file_name().and_then(|name| name.to_str()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("CommitLog segment has no UTF-8 file name: {}", path.display()),
        )
    })?;
    if name.len() != 20 || !name.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown CommitLog segment entry: {}", path.display()),
        ));
    }
    name.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid CommitLog segment offset {}: {error}", path.display()),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_utilization_tie_rotates_from_the_last_selected_root() {
        assert_eq!(rotation_distance(1, 1, 3), 0);
        assert_eq!(rotation_distance(2, 1, 3), 1);
        assert_eq!(rotation_distance(0, 1, 3), 2);
    }

    #[test]
    fn duplicate_offsets_and_non_tail_gaps_fail_closed() {
        let temp = tempfile::tempdir().expect("tempdir");
        let first = temp.path().join("a");
        let second = temp.path().join("b");
        let paths = CommitLogPathSet::try_new([first.clone(), second.clone()], [], 0).expect("path set");
        fs::write(first.join("00000000000000000000"), [0_u8; 16]).expect("first segment");
        fs::write(second.join("00000000000000000000"), [0_u8; 16]).expect("duplicate segment");
        assert!(paths.scan_segments(16).is_err());

        fs::remove_file(second.join("00000000000000000000")).expect("remove duplicate");
        fs::write(second.join("00000000000000000032"), [0_u8; 16]).expect("gapped segment");
        assert!(paths.scan_segments(16).is_err());
    }
}
