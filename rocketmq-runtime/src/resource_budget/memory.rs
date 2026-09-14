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

//! Process memory limit discovery and the managed memory policy.
//!
//! Discovery answers what the kernel actually enforces for this process: the
//! cgroup membership from `/proc/self/cgroup`, the hierarchy mounts and their
//! roots from `/proc/self/mountinfo`, and the smallest finite hard limit visible
//! along the membership path. The managed memory policy answers a different
//! question: how many bytes the resource ledger may charge. The two numbers are
//! deliberately separate, because thread stacks, allocator retention, the
//! components that never register, page cache, and other processes in the same
//! cgroup all consume the same limit.

use std::fmt;
use std::io;
use std::path::Path;
#[cfg(any(target_os = "linux", test))]
use std::path::PathBuf;

use sysinfo::System;

use crate::RuntimeContractPolicy;
use crate::RuntimeContractViolation;
use crate::RuntimeError;
use crate::RuntimeResult;

const EXPLICIT_MEMORY_LIMIT_ENV: &str = "ROCKETMQ_PROCESS_MEMORY_LIMIT_BYTES";
#[cfg(any(target_os = "linux", test))]
const CGROUP_MEMBERSHIP_PATH: &str = "/proc/self/cgroup";
#[cfg(any(target_os = "linux", test))]
const CGROUP_MOUNT_PATH: &str = "/proc/self/mountinfo";
#[cfg(any(target_os = "linux", test))]
const CGROUP_V2_HARD_LIMIT_FILE: &str = "memory.max";
#[cfg(any(target_os = "linux", test))]
const CGROUP_V1_HARD_LIMIT_FILE: &str = "memory.limit_in_bytes";
/// The cgroup v1 sentinel that means "unlimited" rather than a byte count.
///
/// The kernel reports `PAGE_COUNTER_MAX` for a group without a memory limit.
/// Any value at or above it is an unbounded marker, not a finite constraint.
#[cfg(any(target_os = "linux", test))]
const CGROUP_V1_UNLIMITED_SENTINEL: u64 = 0x7FFF_FFFF_FFFF_F000;

/// A read-only view of the files that describe process memory constraints.
///
/// Production uses [`HostMemoryFileView`]. The abstraction exists so cgroup
/// layouts, namespace roots, and read failures can be exercised deterministically
/// without a host that is configured that way.
pub trait MemoryFileView: fmt::Debug + Send + Sync + 'static {
    /// Reads one file, returning `None` when the path does not exist.
    ///
    /// # Errors
    ///
    /// Returns an operational error for a file that exists and cannot be read,
    /// so a permission or I/O failure is not mistaken for an absent constraint.
    fn read_optional(&self, path: &Path) -> RuntimeResult<Option<String>>;
}

/// The production view over the host filesystem.
#[derive(Debug, Default, Clone, Copy)]
pub struct HostMemoryFileView;

impl MemoryFileView for HostMemoryFileView {
    fn read_optional(&self, path: &Path) -> RuntimeResult<Option<String>> {
        match std::fs::read_to_string(path) {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(RuntimeError::io(
                crate::RuntimeOperation::DetectProcessMemoryLimit,
                error,
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Identifies the memory limit source state.
pub enum MemoryLimitSource {
    /// Represents the configured case.
    Configured,
    /// Represents the environment case.
    Environment,
    /// Represents the cgroup v2 case.
    CgroupV2,
    /// Represents the cgroup v1 case.
    CgroupV1,
    /// Represents the host physical memory case.
    HostPhysicalMemory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Represents process memory limit.
pub struct ProcessMemoryLimit {
    bytes: u64,
    source: MemoryLimitSource,
}

impl ProcessMemoryLimit {
    /// Detects the effective process memory limit.
    ///
    /// # Errors
    ///
    /// Returns an operational error when process, cgroup, or environment
    /// discovery cannot provide a finite limit.
    pub fn detect() -> RuntimeResult<Self> {
        Self::detect_with_view(&HostMemoryFileView)
    }

    /// Detects the effective process memory limit through an injectable file view.
    ///
    /// The explicit environment override wins. Otherwise the cgroup membership
    /// and mounts are read from the view when the platform uses them, and the
    /// smallest finite hard limit wins over the host physical memory fallback.
    /// A `HostPhysicalMemory` source on Linux means no finite cgroup hard limit
    /// was visible from this process's membership, so the kernel may enforce a
    /// tighter constraint than the reported one.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the explicit environment value is not
    /// a positive byte count, and an operational error when the view cannot read
    /// a file that exists or when no finite constraint is discoverable.
    pub fn detect_with_view(view: &dyn MemoryFileView) -> RuntimeResult<Self> {
        if let Some(value) = std::env::var_os(EXPLICIT_MEMORY_LIMIT_ENV) {
            let value = value.to_string_lossy();
            let bytes = parse_positive_bytes(&value)
                .ok_or_else(|| RuntimeError::configuration(crate::RuntimeOperation::DetectProcessMemoryLimit))?;
            return Ok(Self {
                bytes,
                source: MemoryLimitSource::Environment,
            });
        }

        detect_platform_limit(view)
    }

    /// Creates an explicitly configured process memory limit.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when `bytes` is zero.
    pub fn configured(bytes: u64) -> Result<Self, RuntimeContractViolation> {
        if bytes == 0 {
            return Err(RuntimeContractViolation::InvalidMemoryLimit {
                policy: RuntimeContractPolicy::ConfiguredMemoryLimitPositive,
            });
        }
        Ok(Self {
            bytes,
            source: MemoryLimitSource::Configured,
        })
    }

    #[must_use]
    /// Returns the bytes.
    pub const fn bytes(self) -> u64 {
        self.bytes
    }

    #[must_use]
    /// Returns the source.
    pub const fn source(self) -> MemoryLimitSource {
        self.source
    }

    /// Returns a positive bounded fraction of this limit.
    ///
    /// # Errors
    ///
    /// Returns a contract violation for a zero, inverted, or out-of-range
    /// fraction.
    pub fn fraction(self, numerator: u64, denominator: u64) -> Result<u64, RuntimeContractViolation> {
        if numerator == 0 || denominator == 0 || numerator > denominator {
            return Err(RuntimeContractViolation::InvalidMemoryLimit {
                policy: RuntimeContractPolicy::MemoryFractionPositiveAndBounded,
            });
        }
        let bytes = (u128::from(self.bytes) * u128::from(numerator)) / u128::from(denominator);
        Ok(bytes as u64)
    }
}

/// How the requested managed bytes are derived from the effective limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ManagedMemoryRequest {
    /// Charge the effective limit itself, the behavior before this policy existed.
    #[default]
    WholeLimit,
    /// Charge an explicit byte count that is independent of the detected limit.
    ExplicitBytes(u64),
    /// Charge a bounded fraction of the effective limit.
    Fraction {
        /// The fraction numerator.
        numerator: u64,
        /// The fraction denominator.
        denominator: u64,
    },
}

/// The policy that derives the chargeable managed memory budget.
///
/// The resolved budget is
/// `min(requested_managed, effective_limit - headroom_bytes)`, where
/// `effective_limit` is the smaller of the detected limit and an explicitly
/// configured limit. No ratio or headroom is imposed by default: the default
/// charges the whole effective limit, which is the behavior that existed before
/// this policy, so adopting a measured ratio stays an explicit opt-in. Lowering
/// a budget does not revoke permits that were already granted; it bounds new
/// admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ManagedMemoryPolicy {
    request: ManagedMemoryRequest,
    headroom_bytes: u64,
}

impl ManagedMemoryPolicy {
    /// Charges the entire effective limit.
    #[must_use]
    pub const fn whole_limit() -> Self {
        Self {
            request: ManagedMemoryRequest::WholeLimit,
            headroom_bytes: 0,
        }
    }

    /// Charges an explicit byte count.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when `bytes` is zero, which could only
    /// produce a zero budget.
    pub fn explicit_bytes(bytes: u64) -> Result<Self, RuntimeContractViolation> {
        if bytes == 0 {
            return Err(RuntimeContractViolation::InvalidMemoryLimit {
                policy: RuntimeContractPolicy::ManagedMemoryBudgetPositive,
            });
        }
        Ok(Self {
            request: ManagedMemoryRequest::ExplicitBytes(bytes),
            headroom_bytes: 0,
        })
    }

    /// Charges a fraction of the effective limit.
    ///
    /// # Errors
    ///
    /// Returns a contract violation for a zero, inverted, or out-of-range
    /// fraction.
    pub fn fraction(numerator: u64, denominator: u64) -> Result<Self, RuntimeContractViolation> {
        if numerator == 0 || denominator == 0 || numerator > denominator {
            return Err(RuntimeContractViolation::InvalidMemoryLimit {
                policy: RuntimeContractPolicy::ManagedMemoryRatioPositiveAndBounded,
            });
        }
        Ok(Self {
            request: ManagedMemoryRequest::Fraction { numerator, denominator },
            headroom_bytes: 0,
        })
    }

    /// Reserves bytes that the ledger must leave unused inside the limit.
    #[must_use]
    pub const fn with_headroom(mut self, headroom_bytes: u64) -> Self {
        self.headroom_bytes = headroom_bytes;
        self
    }

    /// Returns how the requested managed bytes are derived.
    #[must_use]
    pub const fn request(self) -> ManagedMemoryRequest {
        self.request
    }

    /// Returns the reserved headroom.
    #[must_use]
    pub const fn headroom_bytes(self) -> u64 {
        self.headroom_bytes
    }
}

/// The resolved memory numbers that drive the process resource budget.
///
/// The detected limit stays available for reporting while [`Self::managed_bytes`]
/// is what the ledger may charge. The budget covers the ledger objects that
/// register with the shared budget tree only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagedMemoryBudget {
    detected: ProcessMemoryLimit,
    effective_limit_bytes: u64,
    requested_managed_bytes: u64,
    managed_bytes: u64,
}

impl ManagedMemoryBudget {
    /// Resolves the effective limit and the managed budget.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the policy ratio is out of range, when
    /// the headroom leaves no room inside the effective limit, or when the
    /// resolved managed budget is zero.
    pub fn resolve(
        detected: ProcessMemoryLimit,
        configured: Option<ProcessMemoryLimit>,
        policy: ManagedMemoryPolicy,
    ) -> Result<Self, RuntimeContractViolation> {
        let effective_limit_bytes = configured.map_or(detected.bytes(), |limit| detected.bytes().min(limit.bytes()));
        let requested_managed_bytes = match policy.request {
            ManagedMemoryRequest::WholeLimit => effective_limit_bytes,
            ManagedMemoryRequest::ExplicitBytes(bytes) => bytes,
            ManagedMemoryRequest::Fraction { numerator, denominator } => {
                if numerator == 0 || denominator == 0 || numerator > denominator {
                    return Err(RuntimeContractViolation::InvalidMemoryLimit {
                        policy: RuntimeContractPolicy::ManagedMemoryRatioPositiveAndBounded,
                    });
                }
                u64::try_from(u128::from(effective_limit_bytes) * u128::from(numerator) / u128::from(denominator))
                    .unwrap_or(u64::MAX)
            }
        };
        let after_headroom = effective_limit_bytes.checked_sub(policy.headroom_bytes).ok_or(
            RuntimeContractViolation::InvalidMemoryLimit {
                policy: RuntimeContractPolicy::ManagedMemoryHeadroomBelowEffectiveLimit,
            },
        )?;
        let managed_bytes = requested_managed_bytes.min(after_headroom);
        if managed_bytes == 0 {
            return Err(RuntimeContractViolation::InvalidMemoryLimit {
                policy: RuntimeContractPolicy::ManagedMemoryBudgetPositive,
            });
        }
        Ok(Self {
            detected,
            effective_limit_bytes,
            requested_managed_bytes,
            managed_bytes,
        })
    }

    /// Returns the detected or explicitly configured process memory limit.
    #[must_use]
    pub const fn detected(self) -> ProcessMemoryLimit {
        self.detected
    }

    /// Returns the smaller of the detected limit and an explicit limit.
    #[must_use]
    pub const fn effective_limit_bytes(self) -> u64 {
        self.effective_limit_bytes
    }

    /// Returns the managed bytes the policy asked for before headroom applied.
    #[must_use]
    pub const fn requested_managed_bytes(self) -> u64 {
        self.requested_managed_bytes
    }

    /// Returns the managed bytes the ledger may charge.
    #[must_use]
    pub const fn managed_bytes(self) -> u64 {
        self.managed_bytes
    }
}

fn host_physical_memory() -> Option<u64> {
    let mut system = System::new();
    system.refresh_memory();
    let bytes = system.total_memory();
    (bytes > 0).then_some(bytes)
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CgroupHierarchy {
    /// A cgroup v2 hierarchy.
    V2,
    /// A cgroup v1 hierarchy that carries the memory controller.
    V1Memory,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct CgroupMembership {
    controllers: Vec<String>,
    path: PathBuf,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct CgroupMount {
    root: PathBuf,
    mount_point: PathBuf,
    hierarchy: CgroupHierarchy,
}

/// Parses `/proc/self/cgroup` into the process's memberships.
///
/// Malformed lines are skipped: the file is kernel-generated, and a layout this
/// process cannot interpret is a missing constraint rather than a fatal error.
#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_membership(content: &str) -> Vec<CgroupMembership> {
    content
        .lines()
        .filter_map(|line| {
            let mut fields = line.trim().splitn(3, ':');
            let _hierarchy = fields.next()?;
            let controllers = fields.next()?;
            let path = fields.next()?;
            if path.is_empty() {
                return None;
            }
            Some(CgroupMembership {
                controllers: controllers
                    .split(',')
                    .filter(|controller| !controller.is_empty())
                    .map(str::to_string)
                    .collect(),
                path: PathBuf::from(path),
            })
        })
        .collect()
}

/// Parses `/proc/self/mountinfo` into the cgroup hierarchies visible to the
/// process.
///
/// Only cgroup v2 mounts and cgroup v1 mounts that carry the memory controller
/// are kept, because the others cannot bound this process's memory.
#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_mounts(content: &str) -> Vec<CgroupMount> {
    content
        .lines()
        .filter_map(|line| {
            let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
            let separator = fields.iter().position(|field| *field == "-")?;
            let fs_type = *fields.get(separator + 1)?;
            let super_options = *fields.get(separator + 3)?;
            let hierarchy = match fs_type {
                "cgroup2" => CgroupHierarchy::V2,
                "cgroup" if super_options.split(',').any(|option| option == "memory") => CgroupHierarchy::V1Memory,
                _ => return None,
            };
            Some(CgroupMount {
                root: PathBuf::from(unescape_mount_field(fields.get(3)?)),
                mount_point: PathBuf::from(unescape_mount_field(fields.get(4)?)),
                hierarchy,
            })
        })
        .collect()
}

/// Decodes the octal escapes `mountinfo` uses for space, tab, newline, and
/// backslash in paths.
#[cfg(any(target_os = "linux", test))]
fn unescape_mount_field(value: &str) -> String {
    let characters = value.chars().collect::<Vec<_>>();
    let mut decoded = String::with_capacity(value.len());
    let mut index = 0;
    while index < characters.len() {
        if characters[index] == '\\' && index + 3 < characters.len() {
            let digits = characters[index + 1..=index + 3].iter().collect::<String>();
            if digits.bytes().all(|byte| (b'0'..=b'7').contains(&byte)) {
                if let Ok(byte) = u8::from_str_radix(&digits, 8) {
                    decoded.push(byte as char);
                    index += 4;
                    continue;
                }
            }
        }
        decoded.push(characters[index]);
        index += 1;
    }
    decoded
}

/// Returns the on-disk directory of the membership and of every visible ancestor
/// up to the mount root.
///
/// A finite limit set on an ancestor applies to the process, so the smallest
/// finite value along the path is the constraint the kernel enforces.
#[cfg(any(target_os = "linux", test))]
fn visible_membership_paths(mount: &CgroupMount, membership: &Path) -> Vec<PathBuf> {
    let Ok(relative) = membership.strip_prefix(&mount.root) else {
        return Vec::new();
    };
    let mut paths = vec![mount.mount_point.join(relative)];
    let mut current = relative;
    while let Some(parent) = current.parent() {
        if parent.as_os_str().is_empty() {
            paths.push(mount.mount_point.clone());
            break;
        }
        paths.push(mount.mount_point.join(parent));
        current = parent;
    }
    paths
}

/// Returns whether one mount can constrain one membership.
#[cfg(any(target_os = "linux", test))]
fn mount_applies(mount: &CgroupMount, membership: &CgroupMembership) -> bool {
    if !membership.path.starts_with(&mount.root) {
        return false;
    }
    match mount.hierarchy {
        CgroupHierarchy::V2 => membership.controllers.is_empty(),
        CgroupHierarchy::V1Memory => membership.controllers.iter().any(|controller| controller == "memory"),
    }
}

/// Reads the smallest finite hard limit visible to one membership.
///
/// `memory.high` is a pressure threshold rather than a cap, so it is never read
/// here; only the hard limit files can bound the budget.
#[cfg(any(target_os = "linux", test))]
fn cgroup_hard_limit(view: &dyn MemoryFileView, mount: &CgroupMount, membership: &Path) -> RuntimeResult<Option<u64>> {
    let file = match mount.hierarchy {
        CgroupHierarchy::V2 => CGROUP_V2_HARD_LIMIT_FILE,
        CgroupHierarchy::V1Memory => CGROUP_V1_HARD_LIMIT_FILE,
    };
    let mut limit: Option<u64> = None;
    for directory in visible_membership_paths(mount, membership) {
        let Some(content) = view.read_optional(&directory.join(file))? else {
            continue;
        };
        let Some(bytes) = parse_cgroup_hard_limit(mount.hierarchy, &content) else {
            continue;
        };
        limit = Some(limit.map_or(bytes, |current: u64| current.min(bytes)));
    }
    Ok(limit)
}

#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_hard_limit(hierarchy: CgroupHierarchy, content: &str) -> Option<u64> {
    // `parse_positive_bytes` rejects the v2 `max` value, and the v1 sentinel is
    // rejected here, so both mean "no limit at this level".
    let bytes = parse_positive_bytes(content)?;
    match hierarchy {
        CgroupHierarchy::V2 => Some(bytes),
        CgroupHierarchy::V1Memory => (bytes < CGROUP_V1_UNLIMITED_SENTINEL).then_some(bytes),
    }
}

/// Resolves the cgroup constraint visible to this process, if any.
#[cfg(any(target_os = "linux", test))]
fn discover_cgroup_limit(view: &dyn MemoryFileView) -> RuntimeResult<Option<ProcessMemoryLimit>> {
    let Some(membership_content) = view.read_optional(Path::new(CGROUP_MEMBERSHIP_PATH))? else {
        return Ok(None);
    };
    let Some(mount_content) = view.read_optional(Path::new(CGROUP_MOUNT_PATH))? else {
        return Ok(None);
    };
    let memberships = parse_cgroup_membership(&membership_content);
    let mounts = parse_cgroup_mounts(&mount_content);
    let mut discovered: Option<(u64, MemoryLimitSource)> = None;
    for membership in &memberships {
        for mount in &mounts {
            if !mount_applies(mount, membership) {
                continue;
            }
            let source = match mount.hierarchy {
                CgroupHierarchy::V2 => MemoryLimitSource::CgroupV2,
                CgroupHierarchy::V1Memory => MemoryLimitSource::CgroupV1,
            };
            let Some(bytes) = cgroup_hard_limit(view, mount, &membership.path)? else {
                continue;
            };
            discovered = Some(match discovered {
                Some(current) if current.0 <= bytes => current,
                _ => (bytes, source),
            });
        }
    }
    Ok(discovered.map(|(bytes, source)| ProcessMemoryLimit { bytes, source }))
}

fn detect_platform_limit(view: &dyn MemoryFileView) -> RuntimeResult<ProcessMemoryLimit> {
    let host = host_physical_memory().map(|bytes| ProcessMemoryLimit {
        bytes,
        source: MemoryLimitSource::HostPhysicalMemory,
    });
    #[cfg(any(target_os = "linux", test))]
    let cgroup = discover_cgroup_limit(view)?;
    #[cfg(not(any(target_os = "linux", test)))]
    let cgroup: Option<ProcessMemoryLimit> = {
        // The cgroup layout is a Linux mechanism; other platforms keep their own
        // platform implementation and rely on explicit configuration for
        // additional container constraints.
        let _ = view;
        None
    };

    [cgroup, host]
        .into_iter()
        .flatten()
        .min_by_key(|limit| limit.bytes)
        .ok_or_else(|| {
            RuntimeError::internal(
                crate::RuntimeOperation::DetectProcessMemoryLimit,
                MemoryLimitDiscoveryFailure::Unavailable,
            )
        })
}

#[derive(Debug, thiserror::Error)]
enum MemoryLimitDiscoveryFailure {
    #[error("no finite process, cgroup, or host memory limit is available")]
    Unavailable,
}

fn parse_positive_bytes(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok().filter(|bytes| *bytes > 0)
}

#[cfg(all(target_os = "linux", test))]
fn parse_meminfo_bytes(value: &str) -> Option<u64> {
    let line = value.lines().find(|line| line.starts_with("MemTotal:"))?;
    let kibibytes = line.split_ascii_whitespace().nth(1)?.parse::<u64>().ok()?;
    kibibytes.checked_mul(1024).filter(|bytes| *bytes > 0)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// A view over an in-memory layout.
    #[derive(Debug, Default)]
    struct FixtureFileView {
        files: HashMap<PathBuf, String>,
        unreadable: Vec<PathBuf>,
    }

    impl FixtureFileView {
        fn with_files(files: &[(&str, &str)]) -> Self {
            Self {
                files: files
                    .iter()
                    .map(|(path, content)| (PathBuf::from(*path), (*content).to_string()))
                    .collect(),
                unreadable: Vec::new(),
            }
        }

        fn with_unreadable(mut self, path: &str) -> Self {
            self.unreadable.push(PathBuf::from(path));
            self
        }
    }

    impl MemoryFileView for FixtureFileView {
        fn read_optional(&self, path: &Path) -> RuntimeResult<Option<String>> {
            if self.unreadable.iter().any(|candidate| candidate == path) {
                return Err(RuntimeError::io(
                    crate::RuntimeOperation::DetectProcessMemoryLimit,
                    io::Error::new(io::ErrorKind::PermissionDenied, "injected unreadable constraint"),
                ));
            }
            Ok(self.files.get(path).cloned())
        }
    }

    const V2_MOUNT: &str = "29 23 0:26 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup2 rw\n";
    const GIB: u64 = 1024 * 1024 * 1024;

    fn limit(bytes: u64, source: MemoryLimitSource) -> ProcessMemoryLimit {
        ProcessMemoryLimit { bytes, source }
    }

    #[test]
    fn fractions_remain_bounded_without_multiplication_overflow() {
        let limit = ProcessMemoryLimit::configured(u64::MAX).expect("configured limit");
        assert_eq!(limit.fraction(1, 4).expect("quarter"), u64::MAX / 4);
        assert_eq!(limit.fraction(4, 4).expect("whole"), u64::MAX);
        assert!(limit.fraction(0, 4).is_err());
        assert!(limit.fraction(5, 4).is_err());
    }

    #[test]
    fn platform_memory_detection_returns_a_positive_limit() {
        let limit = ProcessMemoryLimit::detect().expect("test host must expose a process memory limit");

        assert!(limit.bytes() > 0);
    }

    #[test]
    fn the_default_policy_charges_the_whole_detected_limit() {
        let detected = limit(8 * GIB, MemoryLimitSource::HostPhysicalMemory);

        let budget = ManagedMemoryBudget::resolve(detected, None, ManagedMemoryPolicy::default())
            .expect("the whole-limit policy resolves");

        assert_eq!(budget.detected(), detected);
        assert_eq!(budget.effective_limit_bytes(), 8 * GIB);
        assert_eq!(budget.requested_managed_bytes(), 8 * GIB);
        assert_eq!(budget.managed_bytes(), 8 * GIB);
        assert_eq!(ManagedMemoryPolicy::default().headroom_bytes(), 0);
    }

    #[test]
    fn the_managed_budget_honours_a_ratio_headroom_and_an_explicit_limit() {
        let detected = limit(8 * GIB, MemoryLimitSource::CgroupV2);
        let configured = ProcessMemoryLimit::configured(4 * GIB).expect("configured limit");

        let fraction = ManagedMemoryPolicy::fraction(1, 2)
            .expect("a half is bounded")
            .with_headroom(GIB);
        let budget = ManagedMemoryBudget::resolve(detected, None, fraction).expect("ratio budget resolves");
        assert_eq!(budget.effective_limit_bytes(), 8 * GIB);
        assert_eq!(budget.requested_managed_bytes(), 4 * GIB);
        assert_eq!(budget.managed_bytes(), 4 * GIB);

        // The explicit limit lowers the effective limit before headroom applies.
        let tightened =
            ManagedMemoryBudget::resolve(detected, Some(configured), fraction).expect("the tightened budget resolves");
        assert_eq!(tightened.effective_limit_bytes(), 4 * GIB);
        assert_eq!(tightened.requested_managed_bytes(), 2 * GIB);
        assert_eq!(tightened.managed_bytes(), 2 * GIB);

        let explicit = ManagedMemoryPolicy::explicit_bytes(GIB).expect("a positive request is valid");
        let budget = ManagedMemoryBudget::resolve(detected, None, explicit).expect("explicit budget resolves");
        assert_eq!(budget.requested_managed_bytes(), GIB);
        assert_eq!(budget.managed_bytes(), GIB);
    }

    #[test]
    fn headroom_bounds_the_requested_managed_bytes() {
        let detected = limit(2 * GIB, MemoryLimitSource::CgroupV2);

        let budget = ManagedMemoryBudget::resolve(
            detected,
            None,
            ManagedMemoryPolicy::whole_limit().with_headroom(1536 * 1024 * 1024),
        )
        .expect("headroom below the limit resolves");

        assert_eq!(budget.requested_managed_bytes(), 2 * GIB);
        assert_eq!(budget.managed_bytes(), 512 * 1024 * 1024);
    }

    #[test]
    fn an_unrepresentable_managed_memory_policy_is_rejected() {
        let detected = limit(8 * GIB, MemoryLimitSource::HostPhysicalMemory);

        assert!(ManagedMemoryPolicy::fraction(0, 8).is_err());
        assert!(ManagedMemoryPolicy::fraction(1, 0).is_err());
        assert!(ManagedMemoryPolicy::fraction(9, 8).is_err());
        assert!(ManagedMemoryPolicy::explicit_bytes(0).is_err());

        let headroom_greater_than_limit = ManagedMemoryBudget::resolve(
            detected,
            None,
            ManagedMemoryPolicy::whole_limit().with_headroom(9 * GIB),
        )
        .expect_err("headroom above the effective limit cannot be represented");
        assert_eq!(
            headroom_greater_than_limit.to_string(),
            "process memory limit violates managed-memory-headroom-below-effective-limit"
        );

        let zero_budget = ManagedMemoryBudget::resolve(
            detected,
            None,
            ManagedMemoryPolicy::whole_limit().with_headroom(8 * GIB),
        )
        .expect_err("a zero managed budget must be rejected");
        assert_eq!(
            zero_budget.to_string(),
            "process memory limit violates managed-memory-budget-positive"
        );
    }

    #[test]
    fn a_nested_cgroup_limit_is_discovered_through_the_membership_path() {
        // The fixed paths only see the hierarchy root, whose v2 value is `max`,
        // so this layout used to fall back to host physical memory.
        let view = FixtureFileView::with_files(&[
            ("/proc/self/cgroup", "0::/user.slice/nested.service\n"),
            ("/proc/self/mountinfo", V2_MOUNT),
            ("/sys/fs/cgroup/memory.max", "max\n"),
            ("/sys/fs/cgroup/user.slice/memory.max", "max\n"),
            ("/sys/fs/cgroup/user.slice/nested.service/memory.max", "1073741824\n"),
        ]);

        let discovered = discover_cgroup_limit(&view)
            .expect("the fixture is readable")
            .expect("a finite nested limit is discovered");

        assert_eq!(discovered.bytes(), GIB);
        assert_eq!(discovered.source(), MemoryLimitSource::CgroupV2);
    }

    #[test]
    fn a_tighter_ancestor_limit_wins_over_a_leaf_without_a_limit() {
        let view = FixtureFileView::with_files(&[
            ("/proc/self/cgroup", "0::/parent/leaf\n"),
            ("/sys/fs/cgroup/parent/leaf/memory.max", "max\n"),
            ("/sys/fs/cgroup/parent/memory.max", "536870912\n"),
            ("/proc/self/mountinfo", V2_MOUNT),
        ]);

        let discovered = discover_cgroup_limit(&view)
            .expect("the fixture is readable")
            .expect("the ancestor limit applies");

        assert_eq!(discovered.bytes(), 512 * 1024 * 1024);
    }

    #[test]
    fn a_mount_root_other_than_the_hierarchy_root_is_resolved() {
        let view = FixtureFileView::with_files(&[
            ("/proc/self/cgroup", "0::/docker/abc123\n"),
            (
                "/proc/self/mountinfo",
                "29 23 0:26 /docker/abc123 /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup2 rw\n",
            ),
            ("/sys/fs/cgroup/memory.max", "2147483648\n"),
        ]);

        let discovered = discover_cgroup_limit(&view)
            .expect("the fixture is readable")
            .expect("the mounted root carries the constraint");

        assert_eq!(discovered.bytes(), 2 * GIB);
    }

    #[test]
    fn escaped_mount_paths_are_decoded() {
        let view = FixtureFileView::with_files(&[
            ("/proc/self/cgroup", "0::/nested/service\n"),
            (
                "/proc/self/mountinfo",
                "29 23 0:26 / /sys/fs/cgroup\\040v2 rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup2 rw\n",
            ),
            ("/sys/fs/cgroup v2/nested/service/memory.max", "1073741824\n"),
        ]);

        let discovered = discover_cgroup_limit(&view)
            .expect("the fixture is readable")
            .expect("the escaped mount point resolves");

        assert_eq!(discovered.bytes(), GIB);
    }

    #[test]
    fn a_v1_mount_needs_the_memory_controller_and_ignores_the_unlimited_sentinel() {
        let membership = "5:memory:/docker/abc\n";
        let memory_mount =
            "30 23 0:27 / /sys/fs/cgroup/memory rw,nosuid,nodev,noexec,relatime - cgroup cgroup rw,memory\n";
        let unlimited = "9223372036854771712\n";

        let unlimited_view = FixtureFileView::with_files(&[
            ("/proc/self/cgroup", membership),
            ("/proc/self/mountinfo", memory_mount),
            ("/sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes", unlimited),
            ("/sys/fs/cgroup/memory/docker/memory.limit_in_bytes", unlimited),
            ("/sys/fs/cgroup/memory/memory.limit_in_bytes", unlimited),
        ]);
        assert!(
            discover_cgroup_limit(&unlimited_view)
                .expect("the fixture is readable")
                .is_none(),
            "the v1 unlimited sentinel is not a constraint"
        );

        let finite_view = FixtureFileView::with_files(&[
            ("/proc/self/cgroup", membership),
            ("/proc/self/mountinfo", memory_mount),
            ("/sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes", unlimited),
            ("/sys/fs/cgroup/memory/docker/memory.limit_in_bytes", "1073741824\n"),
        ]);
        let discovered = discover_cgroup_limit(&finite_view)
            .expect("the fixture is readable")
            .expect("the finite ancestor is the constraint");
        assert_eq!(discovered.bytes(), GIB);
        assert_eq!(discovered.source(), MemoryLimitSource::CgroupV1);

        let cpu_only_view = FixtureFileView::with_files(&[
            ("/proc/self/cgroup", membership),
            (
                "/proc/self/mountinfo",
                "31 23 0:28 / /sys/fs/cgroup/cpu rw,nosuid,nodev,noexec,relatime - cgroup cgroup rw,cpu\n",
            ),
        ]);
        assert!(
            discover_cgroup_limit(&cpu_only_view)
                .expect("the fixture is readable")
                .is_none(),
            "a mount without the memory controller cannot bound memory"
        );
    }

    #[test]
    fn an_unreadable_constraint_is_an_error_rather_than_a_missing_one() {
        let view = FixtureFileView::with_files(&[
            ("/proc/self/cgroup", "0::/leaf\n"),
            ("/proc/self/mountinfo", V2_MOUNT),
            ("/sys/fs/cgroup/leaf/memory.max", "1073741824\n"),
        ])
        .with_unreadable("/sys/fs/cgroup/leaf/memory.max");

        let error = discover_cgroup_limit(&view).expect_err("a permission failure must not read as absent");

        assert_eq!(error.operation(), crate::RuntimeOperation::DetectProcessMemoryLimit);
    }

    #[test]
    fn a_missing_membership_file_yields_no_cgroup_constraint() {
        let view = FixtureFileView::with_files(&[("/proc/self/mountinfo", V2_MOUNT)]);

        assert!(discover_cgroup_limit(&view)
            .expect("a missing file is not an error")
            .is_none());
    }

    #[test]
    fn the_visible_paths_include_the_mount_root_and_every_ancestor() {
        let mount = CgroupMount {
            root: PathBuf::from("/docker"),
            mount_point: PathBuf::from("/sys/fs/cgroup"),
            hierarchy: CgroupHierarchy::V2,
        };

        let paths = visible_membership_paths(&mount, Path::new("/docker/abc/leaf"));

        assert_eq!(
            paths,
            vec![
                PathBuf::from("/sys/fs/cgroup/abc/leaf"),
                PathBuf::from("/sys/fs/cgroup/abc"),
                PathBuf::from("/sys/fs/cgroup"),
            ]
        );
        assert!(visible_membership_paths(&mount, Path::new("/other/leaf")).is_empty());
    }

    #[test]
    fn membership_parsing_records_controllers_and_skips_malformed_lines() {
        let memberships = parse_cgroup_membership("0::/v2/leaf\n5:memory,cpu:/v1/leaf\nmalformed\n\n");

        assert_eq!(memberships.len(), 2);
        assert!(memberships[0].controllers.is_empty());
        assert_eq!(memberships[0].path, PathBuf::from("/v2/leaf"));
        assert_eq!(
            memberships[1].controllers,
            vec!["memory".to_string(), "cpu".to_string()]
        );
        assert_eq!(memberships[1].path, PathBuf::from("/v1/leaf"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_limit_parsers_reject_unbounded_and_accept_finite_values() {
        assert_eq!(parse_cgroup_hard_limit(CgroupHierarchy::V2, "max\n"), None);
        assert_eq!(
            parse_cgroup_hard_limit(CgroupHierarchy::V2, "1048576\n"),
            Some(1_048_576)
        );
        assert_eq!(
            parse_cgroup_hard_limit(CgroupHierarchy::V1Memory, "9223372036854771712\n"),
            None
        );
        assert_eq!(
            parse_cgroup_hard_limit(CgroupHierarchy::V1Memory, "1048576\n"),
            Some(1_048_576)
        );
        assert_eq!(
            parse_meminfo_bytes("MemTotal:       2048 kB\nMemFree: 1 kB"),
            Some(2_097_152)
        );
    }
}
