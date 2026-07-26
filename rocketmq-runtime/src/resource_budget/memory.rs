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

use std::io;
#[cfg(target_os = "linux")]
use std::path::Path;

use sysinfo::System;

const EXPLICIT_MEMORY_LIMIT_ENV: &str = "ROCKETMQ_PROCESS_MEMORY_LIMIT_BYTES";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryLimitSource {
    Configured,
    Environment,
    CgroupV2,
    CgroupV1,
    HostPhysicalMemory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessMemoryLimit {
    bytes: u64,
    source: MemoryLimitSource,
}

impl ProcessMemoryLimit {
    pub fn detect() -> Result<Self, MemoryLimitError> {
        if let Some(value) = std::env::var_os(EXPLICIT_MEMORY_LIMIT_ENV) {
            let value = value.to_string_lossy();
            let bytes = parse_positive_bytes(&value).ok_or_else(|| MemoryLimitError::InvalidEnvironment {
                name: EXPLICIT_MEMORY_LIMIT_ENV,
                value: value.into_owned(),
            })?;
            return Ok(Self {
                bytes,
                source: MemoryLimitSource::Environment,
            });
        }

        detect_platform_limit()
    }

    pub fn configured(bytes: u64) -> Result<Self, MemoryLimitError> {
        if bytes == 0 {
            return Err(MemoryLimitError::ZeroConfiguredLimit);
        }
        Ok(Self {
            bytes,
            source: MemoryLimitSource::Configured,
        })
    }

    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.bytes
    }

    #[must_use]
    pub const fn source(self) -> MemoryLimitSource {
        self.source
    }

    pub fn fraction(self, numerator: u64, denominator: u64) -> Result<u64, MemoryLimitError> {
        if numerator == 0 || denominator == 0 || numerator > denominator {
            return Err(MemoryLimitError::InvalidFraction { numerator, denominator });
        }
        let bytes = (u128::from(self.bytes) * u128::from(numerator)) / u128::from(denominator);
        Ok(bytes as u64)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MemoryLimitError {
    #[error("environment variable {name} must contain a positive byte count, got {value:?}")]
    InvalidEnvironment { name: &'static str, value: String },
    #[error("configured process memory limit must be greater than zero")]
    ZeroConfiguredLimit,
    #[error("invalid memory budget fraction {numerator}/{denominator}")]
    InvalidFraction { numerator: u64, denominator: u64 },
    #[error("failed to read process memory limit from {path}: {source}")]
    Read {
        path: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("no finite process, cgroup, or host memory limit is available")]
    Unavailable,
}

#[cfg(target_os = "linux")]
fn detect_platform_limit() -> Result<ProcessMemoryLimit, MemoryLimitError> {
    const CGROUP_V2: &str = "/sys/fs/cgroup/memory.max";
    const CGROUP_V1: &str = "/sys/fs/cgroup/memory/memory.limit_in_bytes";

    let host = host_physical_memory().map(|bytes| (bytes, MemoryLimitSource::HostPhysicalMemory));
    let cgroup_v2 = read_optional(CGROUP_V2)?
        .as_deref()
        .and_then(parse_cgroup_bytes)
        .map(|bytes| (bytes, MemoryLimitSource::CgroupV2));
    let cgroup_v1 = read_optional(CGROUP_V1)?
        .as_deref()
        .and_then(parse_cgroup_bytes)
        .filter(|bytes| host.is_none_or(|(host_bytes, _)| *bytes < host_bytes))
        .map(|bytes| (bytes, MemoryLimitSource::CgroupV1));

    [cgroup_v2, cgroup_v1, host]
        .into_iter()
        .flatten()
        .min_by_key(|(bytes, _)| *bytes)
        .map(|(bytes, source)| ProcessMemoryLimit { bytes, source })
        .ok_or(MemoryLimitError::Unavailable)
}

#[cfg(not(target_os = "linux"))]
fn detect_platform_limit() -> Result<ProcessMemoryLimit, MemoryLimitError> {
    host_physical_memory()
        .map(|bytes| ProcessMemoryLimit {
            bytes,
            source: MemoryLimitSource::HostPhysicalMemory,
        })
        .ok_or(MemoryLimitError::Unavailable)
}

fn host_physical_memory() -> Option<u64> {
    let mut system = System::new();
    system.refresh_memory();
    let bytes = system.total_memory();
    (bytes > 0).then_some(bytes)
}

#[cfg(target_os = "linux")]
fn read_optional(path: &'static str) -> Result<Option<String>, MemoryLimitError> {
    match std::fs::read_to_string(Path::new(path)) {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(MemoryLimitError::Read { path, source }),
    }
}

fn parse_positive_bytes(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok().filter(|bytes| *bytes > 0)
}

#[cfg(target_os = "linux")]
fn parse_cgroup_bytes(value: &str) -> Option<u64> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("max") {
        return None;
    }
    parse_positive_bytes(value)
}

#[cfg(all(target_os = "linux", test))]
fn parse_meminfo_bytes(value: &str) -> Option<u64> {
    let line = value.lines().find(|line| line.starts_with("MemTotal:"))?;
    let kibibytes = line.split_ascii_whitespace().nth(1)?.parse::<u64>().ok()?;
    kibibytes.checked_mul(1024).filter(|bytes| *bytes > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fractions_remain_bounded_without_multiplication_overflow() {
        let limit = ProcessMemoryLimit::configured(u64::MAX).expect("configured limit");
        assert_eq!(limit.fraction(1, 4).expect("quarter"), u64::MAX / 4);
        assert_eq!(limit.fraction(4, 4).expect("whole"), u64::MAX);
        assert!(limit.fraction(0, 4).is_err());
        assert!(limit.fraction(5, 4).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_limit_parsers_reject_unbounded_and_accept_finite_values() {
        assert_eq!(parse_cgroup_bytes("max\n"), None);
        assert_eq!(parse_cgroup_bytes("1048576\n"), Some(1_048_576));
        assert_eq!(
            parse_meminfo_bytes("MemTotal:       2048 kB\nMemFree: 1 kB"),
            Some(2_097_152)
        );
    }
}
