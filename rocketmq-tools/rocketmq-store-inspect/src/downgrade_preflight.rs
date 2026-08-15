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

//! Offline compatibility checks that run before starting an older Broker binary.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use rocketmq_error::RocketMQError;
use rocketmq_store_rocksdb::read_only::PopConsumerProfileState;
use rocketmq_store_rocksdb::read_only::ReadOnlyRocksDb;
use serde::Deserialize;
use serde::Serialize;

const EXTENDED_TIMER_OWNER_MARKER: &str = "config/timer-store-owner.meta";
const EXTENDED_TIMER_OWNER_PREFIX: &str = "extended_timeline:v1:";
const STORAGE_FORMAT_INVENTORY: &str = "config/storage-format-inventory.json";
const COMPACTION_CURRENT_MAGIC: u32 = 0x4343_5547;
const COMPACTION_CURRENT_SIZE: usize = 40;

/// Inputs for one offline downgrade decision.
#[derive(Debug, Clone)]
pub struct DowngradePreflightRequest {
    /// Target Rust release that would be started after this check.
    pub target_version: String,
    /// Canonical Broker TOML configuration.
    pub config_path: PathBuf,
}

impl DowngradePreflightRequest {
    /// Creates a request.
    pub fn new(target_version: impl Into<String>, config_path: impl Into<PathBuf>) -> Self {
        Self {
            target_version: target_version.into(),
            config_path: config_path.into(),
        }
    }
}

/// One stable compatibility check result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreflightCheck {
    /// Stable check identifier.
    pub id: String,
    /// Machine-readable status.
    pub status: String,
    /// Human-readable evidence or reason.
    pub detail: String,
}

/// Complete downgrade decision. A denied report is a hard listener-start fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DowngradePreflightReport {
    /// Requested target release.
    pub target_version: String,
    /// Whether the target may be started using the inspected state.
    pub allowed: bool,
    /// Per-format decisions.
    pub checks: Vec<PreflightCheck>,
    /// Required operator actions before retrying a denied downgrade.
    pub actions: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct BrokerFile {
    store: StoreSection,
}

#[derive(Debug, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct StoreSection {
    store_path_root_dir: PathBuf,
    store_path_commit_log: Option<String>,
    read_only_commit_log_store_paths: Option<String>,
    mapped_file_size_commit_log: u64,
    enable_compaction: bool,
    timer_store_mode: String,
    timer_extended_activation_epoch: u64,
}

impl Default for StoreSection {
    fn default() -> Self {
        Self {
            store_path_root_dir: PathBuf::from("store"),
            store_path_commit_log: None,
            read_only_commit_log_store_paths: None,
            mapped_file_size_commit_log: 1024 * 1024 * 1024,
            enable_compaction: false,
            timer_store_mode: "java_compat".to_owned(),
            timer_extended_activation_epoch: 0,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct StorageFormatInventory {
    pop_consumer_profile: DeclaredFormat,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct DeclaredFormat {
    declared: bool,
}

/// Inspects every Rust-owned format relevant to a downgrade and returns a fail-closed report.
pub fn run_preflight(request: &DowngradePreflightRequest) -> Result<DowngradePreflightReport, RocketMQError> {
    let target_major = parse_target_major(&request.target_version)?;
    let loaded = config::Config::builder()
        .add_source(config::File::from(request.config_path.clone()))
        .build()
        .map_err(|error| read_error(&request.config_path, format!("load Broker config: {error}")))?;
    let broker: BrokerFile = loaded
        .try_deserialize()
        .map_err(|error| read_error(&request.config_path, format!("decode Broker config: {error}")))?;
    let root = canonical_existing_root(&broker.store.store_path_root_dir)?;
    let _lock = OfflineLock::acquire(&root)?;
    let inventory = load_inventory(&root)?;
    let mut checks = Vec::new();
    let mut actions = Vec::new();

    check_multipath(&broker.store, target_major, &mut checks, &mut actions)?;
    check_pop(&root, &inventory, target_major, &mut checks, &mut actions)?;
    check_timer(&root, &broker.store, target_major, &mut checks, &mut actions)?;
    check_compaction(&root, &broker.store, target_major, &mut checks, &mut actions)?;
    check_tiered(&root, target_major, &mut checks, &mut actions)?;

    let allowed = checks.iter().all(|check| {
        !matches!(
            check.status.as_str(),
            "incompatible" | "declared-present-invalid" | "corrupt"
        )
    });
    Ok(DowngradePreflightReport {
        target_version: request.target_version.clone(),
        allowed,
        checks,
        actions,
    })
}

fn check_multipath(
    store: &StoreSection,
    target_major: u64,
    checks: &mut Vec<PreflightCheck>,
    actions: &mut Vec<String>,
) -> Result<(), RocketMQError> {
    let primary_default = store.store_path_root_dir.join("commitlog").display().to_string();
    let writable = split_paths(store.store_path_commit_log.as_deref().unwrap_or(&primary_default));
    let mut roots = writable.clone();
    roots.extend(split_paths(
        store.read_only_commit_log_store_paths.as_deref().unwrap_or_default(),
    ));
    roots.sort();
    roots.dedup();
    let primary = writable
        .into_iter()
        .next()
        .ok_or_else(|| RocketMQError::illegal_argument("CommitLog has no primary path"))?;
    let primary = fs::canonicalize(&primary).map_err(|error| read_error(&primary, error.to_string()))?;
    let mut outside_primary = false;
    let mut offsets = Vec::new();
    for root in roots {
        let canonical = fs::canonicalize(&root).map_err(|error| read_error(&root, error.to_string()))?;
        for entry in fs::read_dir(&canonical).map_err(|error| read_error(&canonical, error.to_string()))? {
            let path = entry.map_err(|error| read_error(&canonical, error.to_string()))?.path();
            let name = path.file_name().and_then(|name| name.to_str()).unwrap_or_default();
            if name.len() != 20 || !name.bytes().all(|byte| byte.is_ascii_digit()) || !path.is_file() {
                return Err(read_error(&path, "unknown or non-file CommitLog entry".to_owned()));
            }
            offsets.push(
                name.parse::<u64>()
                    .map_err(|error| read_error(&path, error.to_string()))?,
            );
            outside_primary |= canonical != primary;
        }
    }
    offsets.sort_unstable();
    for pair in offsets.windows(2) {
        let expected = pair[0]
            .checked_add(store.mapped_file_size_commit_log)
            .ok_or_else(|| RocketMQError::storage_read_failed("CommitLog", "segment offset overflow"))?;
        if pair[1] != expected {
            return Err(RocketMQError::storage_read_failed(
                "CommitLog",
                format!("non-contiguous segments: expected {expected}, found {}", pair[1]),
            ));
        }
    }
    let incompatible = target_major < 1 && outside_primary;
    checks.push(check(
        "multipath",
        if incompatible { "incompatible" } else { "compatible" },
        if outside_primary {
            "CommitLog segments exist outside the primary root"
        } else {
            "all existing CommitLog segments are readable from the primary root"
        },
    ));
    if incompatible {
        actions.push("run rocketmq-cli-rust consolidate-multipath while the Broker is stopped".to_owned());
    }
    Ok(())
}

fn check_pop(
    root: &Path,
    inventory: &StorageFormatInventory,
    target_major: u64,
    checks: &mut Vec<PreflightCheck>,
    actions: &mut Vec<String>,
) -> Result<(), RocketMQError> {
    let declared = inventory.pop_consumer_profile.declared;
    let database = ReadOnlyRocksDb::open_existing(root.join("kvStore"))?;
    let state = match database {
        Some(database) => database.inspect_pop_consumer_profile(declared)?,
        None if declared => PopConsumerProfileState::DeclaredPresentInvalid {
            reason: "format inventory declares POP consumer profiles but kvStore is absent".to_owned(),
        },
        None => PopConsumerProfileState::LegacyAbsent,
    };
    match state {
        PopConsumerProfileState::LegacyAbsent => checks.push(check(
            "pop",
            "legacy-absent",
            "persistent POP consumer-profile state is not initialized",
        )),
        PopConsumerProfileState::PresentValid(marker) if target_major >= 1 => checks.push(check(
            "pop",
            "present-valid",
            format!(
                "POP profile format {} generation {}",
                marker.format_version, marker.generation
            ),
        )),
        PopConsumerProfileState::PresentValid(marker) => {
            checks.push(check(
                "pop",
                "incompatible",
                format!("target cannot read POP profile format {}", marker.format_version),
            ));
            actions.push("drain POP inflight/profile ownership using a 1.0 dual-reader before downgrade".to_owned());
        }
        PopConsumerProfileState::DeclaredPresentInvalid { reason } => {
            checks.push(check("pop", "declared-present-invalid", reason));
            actions.push("repair the declared POP profile database before starting any Broker".to_owned());
        }
    }
    Ok(())
}

fn check_timer(
    root: &Path,
    store: &StoreSection,
    target_major: u64,
    checks: &mut Vec<PreflightCheck>,
    actions: &mut Vec<String>,
) -> Result<(), RocketMQError> {
    let path = root.join(EXTENDED_TIMER_OWNER_MARKER);
    let marker = match fs::read_to_string(&path) {
        Ok(value) => Some(value),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(read_error(&path, error.to_string())),
    };
    let configured_extended =
        store.timer_store_mode == "extended_timeline" || store.timer_extended_activation_epoch > 0;
    match marker {
        None if configured_extended => {
            checks.push(check(
                "timer",
                "declared-present-invalid",
                "Extended Timeline is configured but its owner marker is absent",
            ));
            actions.push("repair the Extended Timeline owner marker before restart".to_owned());
        }
        None => checks.push(check("timer", "legacy-absent", "Java-compatible Timer remains owner")),
        Some(marker) => {
            let epoch = marker
                .trim()
                .strip_prefix(EXTENDED_TIMER_OWNER_PREFIX)
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|epoch| *epoch > 0);
            let Some(epoch) = epoch else {
                checks.push(check("timer", "corrupt", "Extended Timeline owner marker is invalid"));
                actions.push("repair the Extended Timeline owner marker before restart".to_owned());
                return Ok(());
            };
            if target_major < 1 {
                checks.push(check(
                    "timer",
                    "incompatible",
                    format!("Extended Timeline owns timer delivery at epoch {epoch}"),
                ));
                actions.push(
                    "quiesce Timer admissions, drain outstanding delivery, and produce a clean checkpoint before downgrade"
                        .to_owned(),
                );
            } else {
                checks.push(check(
                    "timer",
                    "present-valid",
                    format!("Extended Timeline owner epoch {epoch}"),
                ));
            }
        }
    }
    Ok(())
}

fn check_compaction(
    root: &Path,
    store: &StoreSection,
    target_major: u64,
    checks: &mut Vec<PreflightCheck>,
    actions: &mut Vec<String>,
) -> Result<(), RocketMQError> {
    let compaction = root.join("compaction");
    let current = compaction.join("CURRENT");
    let bytes = match fs::read(&current) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(read_error(&current, error.to_string())),
    };
    let generations = compaction.join("generations");
    let generations_present = generations.is_dir()
        && fs::read_dir(&generations)
            .map_err(|error| read_error(&generations, error.to_string()))?
            .next()
            .is_some();
    let Some(bytes) = bytes else {
        if store.enable_compaction && generations_present {
            checks.push(check(
                "compaction",
                "declared-present-invalid",
                "Compaction generations exist but CURRENT is absent",
            ));
            actions.push("recover or roll back the last complete Compaction generation with the 1.0 tool".to_owned());
        } else {
            checks.push(check(
                "compaction",
                "legacy-absent",
                "no published Compaction generation",
            ));
        }
        return Ok(());
    };
    let Some(version) = decode_compaction_current(&bytes) else {
        checks.push(check("compaction", "corrupt", "Compaction CURRENT is invalid"));
        actions.push("recover the last complete Compaction generation before restart".to_owned());
        return Ok(());
    };
    let status = match version {
        1 => "present-valid",
        2 if target_major >= 1 => "present-valid",
        2 => "incompatible",
        _ => "corrupt",
    };
    checks.push(check(
        "compaction",
        status,
        format!("Compaction CURRENT format version {version}"),
    ));
    if matches!(status, "incompatible" | "corrupt") {
        actions.push("recover or convert Compaction state with the 1.0 tool before downgrade".to_owned());
    }
    Ok(())
}

fn decode_compaction_current(bytes: &[u8]) -> Option<u16> {
    if bytes.len() != COMPACTION_CURRENT_SIZE
        || u32::from_be_bytes(bytes[0..4].try_into().ok()?) != COMPACTION_CURRENT_MAGIC
        || rocketmq_model::utils::crc32_utils::crc32(&bytes[..32]) != u32::from_be_bytes(bytes[32..36].try_into().ok()?)
    {
        return None;
    }
    let version = u16::from_be_bytes(bytes[4..6].try_into().ok()?);
    let current = u64::from_be_bytes(bytes[8..16].try_into().ok()?);
    let previous = u64::from_be_bytes(bytes[16..24].try_into().ok()?);
    let next = u64::from_be_bytes(bytes[24..32].try_into().ok()?);
    if next <= current || previous == current || !matches!(version, 1 | 2) {
        return None;
    }
    Some(version)
}

fn check_tiered(
    root: &Path,
    target_major: u64,
    checks: &mut Vec<PreflightCheck>,
    actions: &mut Vec<String>,
) -> Result<(), RocketMQError> {
    let path = root.join("config/tieredStoreMetadata.json");
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            checks.push(check("tiered", "legacy-absent", "no Tiered metadata"));
            return Ok(());
        }
        Err(error) => return Err(read_error(&path, error.to_string())),
    };
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| read_error(&path, format!("decode Tiered metadata: {error}")))?;
    let format = value.get("format").and_then(serde_json::Value::as_str);
    let version = value.get("version").and_then(serde_json::Value::as_u64);
    let status = if format != Some("rocketmq-tiered-metadata") || version != Some(1) {
        "corrupt"
    } else if target_major < 1 {
        "incompatible"
    } else {
        "present-valid"
    };
    checks.push(check(
        "tiered",
        status,
        format!("Tiered metadata format={format:?} version={version:?}"),
    ));
    if matches!(status, "incompatible" | "corrupt") {
        actions.push("retain the 1.0 Tiered reader or migrate metadata before downgrade".to_owned());
    }
    Ok(())
}

fn load_inventory(root: &Path) -> Result<StorageFormatInventory, RocketMQError> {
    let path = root.join(STORAGE_FORMAT_INVENTORY);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| read_error(&path, format!("decode storage format inventory: {error}"))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(StorageFormatInventory::default()),
        Err(error) => Err(read_error(&path, error.to_string())),
    }
}

fn parse_target_major(version: &str) -> Result<u64, RocketMQError> {
    let numeric = version.trim_start_matches('v');
    numeric
        .split('.')
        .next()
        .and_then(|major| major.parse().ok())
        .ok_or_else(|| RocketMQError::illegal_argument(format!("invalid target version: {version}")))
}

fn split_paths(value: &str) -> Vec<PathBuf> {
    value
        .split(',')
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .collect()
}

fn canonical_existing_root(path: &Path) -> Result<PathBuf, RocketMQError> {
    let canonical = fs::canonicalize(path).map_err(|error| read_error(path, format!("open Store root: {error}")))?;
    Ok(platform_compatible_canonical_path(canonical))
}

#[cfg(windows)]
fn platform_compatible_canonical_path(path: PathBuf) -> PathBuf {
    let value = path.to_string_lossy();
    if let Some(unc) = value.strip_prefix(r"\\?\UNC\") {
        return PathBuf::from(format!(r"\\{unc}"));
    }
    value.strip_prefix(r"\\?\").map_or(path.clone(), PathBuf::from)
}

#[cfg(not(windows))]
fn platform_compatible_canonical_path(path: PathBuf) -> PathBuf {
    path
}

fn check(id: &str, status: &str, detail: impl Into<String>) -> PreflightCheck {
    PreflightCheck {
        id: id.to_owned(),
        status: status.to_owned(),
        detail: detail.into(),
    }
}

fn read_error(path: &Path, reason: String) -> RocketMQError {
    RocketMQError::storage_read_failed(path.display().to_string(), reason)
}

struct OfflineLock {
    file: File,
}

impl OfflineLock {
    fn acquire(store_root: &Path) -> Result<Self, RocketMQError> {
        let path = store_root.join("lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| read_error(&path, error.to_string()))?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
            RocketMQError::storage_read_failed(
                path.display().to_string(),
                format!("Broker must be stopped before downgrade preflight: {error}"),
            )
        })?;
        Ok(Self { file })
    }
}

impl Drop for OfflineLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}
