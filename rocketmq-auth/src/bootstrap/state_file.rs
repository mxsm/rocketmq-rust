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

use serde::Deserialize;
use serde::Serialize;

use super::BootstrapAdminIdentity;
use super::BootstrapError;
use super::BootstrapGrant;
use super::BootstrapStatus;

const STATE_SCHEMA_VERSION: u8 = 1;
const MAXIMUM_STATE_BYTES: u64 = 16 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PersistedBootstrapStatus {
    Available,
    Claimed,
    Consumed,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BootstrapStateRecord {
    schema_version: u8,
    grant_id: String,
    cluster_id: String,
    listener: String,
    expires_at_unix_seconds: u64,
    status: PersistedBootstrapStatus,
    admin_principal: Option<String>,
    certificate_sha256: Option<String>,
}

impl BootstrapStateRecord {
    pub(super) fn available(grant: &BootstrapGrant) -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            grant_id: grant.grant_id(),
            cluster_id: grant.cluster_id().to_owned(),
            listener: grant.listener().to_owned(),
            expires_at_unix_seconds: grant.expires_at_unix_seconds(),
            status: PersistedBootstrapStatus::Available,
            admin_principal: None,
            certificate_sha256: None,
        }
    }

    pub(super) fn status(&self) -> BootstrapStatus {
        match self.status {
            PersistedBootstrapStatus::Available => BootstrapStatus::Available,
            PersistedBootstrapStatus::Claimed => BootstrapStatus::Claimed,
            PersistedBootstrapStatus::Consumed => BootstrapStatus::Consumed,
        }
    }

    pub(super) fn claimed(&self, identity: &BootstrapAdminIdentity) -> Self {
        let mut claimed = self.clone();
        claimed.status = PersistedBootstrapStatus::Claimed;
        claimed.admin_principal = Some(identity.principal_id().to_owned());
        claimed.certificate_sha256 = Some(hex::encode(identity.certificate_sha256()));
        claimed
    }

    pub(super) fn consumed(&self, identity: &BootstrapAdminIdentity) -> Self {
        let mut consumed = self.claimed(identity);
        consumed.status = PersistedBootstrapStatus::Consumed;
        consumed
    }

    fn has_same_grant(&self, expected: &Self) -> bool {
        self.schema_version == STATE_SCHEMA_VERSION
            && self.grant_id == expected.grant_id
            && self.cluster_id == expected.cluster_id
            && self.listener == expected.listener
            && self.expires_at_unix_seconds == expected.expires_at_unix_seconds
    }

    fn is_well_formed(&self) -> bool {
        match self.status {
            PersistedBootstrapStatus::Available => self.admin_principal.is_none() && self.certificate_sha256.is_none(),
            PersistedBootstrapStatus::Claimed | PersistedBootstrapStatus::Consumed => {
                self.admin_principal
                    .as_deref()
                    .is_some_and(|principal| !principal.is_empty())
                    && self.certificate_sha256.as_deref().is_some_and(is_sha256_hex)
            }
        }
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) struct BootstrapStateStore {
    state_path: PathBuf,
    claim_path: PathBuf,
}

impl BootstrapStateStore {
    pub(super) fn open(
        state_path: PathBuf,
        initial: BootstrapStateRecord,
    ) -> Result<(Self, BootstrapStateRecord), BootstrapError> {
        let store = Self {
            claim_path: claim_path(&state_path),
            state_path,
        };
        prepare_parent(&store.state_path)?;
        let state = match read_record_if_present(&store.state_path)? {
            Some(state) => state,
            None => match publish_new_record(&store.state_path, &initial) {
                Ok(()) => initial.clone(),
                Err(BootstrapError::AlreadyClaimed) => read_record(&store.state_path)?,
                Err(error) => return Err(error),
            },
        };
        if !state.has_same_grant(&initial) {
            return Err(BootstrapError::InvalidConfiguration);
        }
        if !state.is_well_formed() {
            return Err(BootstrapError::StateUnavailable);
        }
        let effective = match read_record_if_present(&store.claim_path)? {
            Some(claimed) => {
                if !claimed.has_same_grant(&initial)
                    || !claimed.is_well_formed()
                    || !matches!(claimed.status, PersistedBootstrapStatus::Claimed)
                {
                    return Err(BootstrapError::StateUnavailable);
                }
                if matches!(state.status, PersistedBootstrapStatus::Consumed) {
                    state
                } else {
                    claimed
                }
            }
            None => state,
        };
        Ok((store, effective))
    }

    pub(super) fn claim(&self, claimed: &BootstrapStateRecord) -> Result<(), BootstrapError> {
        publish_new_record(&self.claim_path, claimed)
    }

    pub(super) fn consume(&self, consumed: &BootstrapStateRecord) -> Result<(), BootstrapError> {
        replace_record(&self.state_path, consumed)
    }

    pub(super) fn claim_exists(&self) -> Result<bool, BootstrapError> {
        match std::fs::symlink_metadata(&self.claim_path) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(_) => Err(BootstrapError::StateUnavailable),
        }
    }
}

fn claim_path(state_path: &Path) -> PathBuf {
    let file_name = state_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("bootstrap-state");
    state_path.with_file_name(format!("{file_name}.claim"))
}

#[cfg(windows)]
fn prepare_parent(_path: &Path) -> Result<(), BootstrapError> {
    Err(BootstrapError::UnsupportedPlatform)
}

#[cfg(unix)]
fn prepare_parent(path: &Path) -> Result<(), BootstrapError> {
    use std::fs;
    use std::os::unix::fs::DirBuilderExt;
    use std::os::unix::fs::PermissionsExt;

    let parent = path.parent().ok_or(BootstrapError::InvalidConfiguration)?;
    if !parent.exists() {
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder.create(parent).map_err(|_| BootstrapError::StateUnavailable)?;
    }
    let metadata = fs::symlink_metadata(parent).map_err(|_| BootstrapError::StateUnavailable)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(BootstrapError::InsecurePermissions);
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(BootstrapError::InsecurePermissions);
    }
    Ok(())
}

#[cfg(windows)]
fn read_record_if_present(_path: &Path) -> Result<Option<BootstrapStateRecord>, BootstrapError> {
    Err(BootstrapError::UnsupportedPlatform)
}

#[cfg(unix)]
fn read_record_if_present(path: &Path) -> Result<Option<BootstrapStateRecord>, BootstrapError> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => read_record(path).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(BootstrapError::StateUnavailable),
    }
}

#[cfg(unix)]
fn read_record(path: &Path) -> Result<BootstrapStateRecord, BootstrapError> {
    use std::fs;
    use std::fs::File;
    use std::io::Read;
    use std::os::unix::fs::PermissionsExt;

    let link_metadata = fs::symlink_metadata(path).map_err(|_| BootstrapError::StateUnavailable)?;
    if link_metadata.file_type().is_symlink() {
        return Err(BootstrapError::InsecurePermissions);
    }
    let mut file = File::open(path).map_err(|_| BootstrapError::StateUnavailable)?;
    let metadata = file.metadata().map_err(|_| BootstrapError::StateUnavailable)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(BootstrapError::InsecurePermissions);
    }
    if metadata.len() > MAXIMUM_STATE_BYTES {
        return Err(BootstrapError::StateUnavailable);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(MAXIMUM_STATE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| BootstrapError::StateUnavailable)?;
    if bytes.len() as u64 > MAXIMUM_STATE_BYTES {
        return Err(BootstrapError::StateUnavailable);
    }
    serde_json::from_slice(&bytes).map_err(|_| BootstrapError::StateUnavailable)
}

#[cfg(windows)]
fn read_record(_path: &Path) -> Result<BootstrapStateRecord, BootstrapError> {
    Err(BootstrapError::UnsupportedPlatform)
}

#[cfg(windows)]
fn publish_new_record(_path: &Path, _record: &BootstrapStateRecord) -> Result<(), BootstrapError> {
    Err(BootstrapError::UnsupportedPlatform)
}

#[cfg(unix)]
fn publish_new_record(path: &Path, record: &BootstrapStateRecord) -> Result<(), BootstrapError> {
    let temporary = write_temporary_record(path, record)?;
    let result = std::fs::hard_link(&temporary, path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            BootstrapError::AlreadyClaimed
        } else {
            BootstrapError::StateUnavailable
        }
    });
    let _ = std::fs::remove_file(&temporary);
    result?;
    sync_parent(path)
}

#[cfg(unix)]
fn replace_record(path: &Path, record: &BootstrapStateRecord) -> Result<(), BootstrapError> {
    let temporary = write_temporary_record(path, record)?;
    let result = std::fs::rename(&temporary, path)
        .map_err(|_| BootstrapError::StateUnavailable)
        .and_then(|()| sync_parent(path));
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(windows)]
fn replace_record(_path: &Path, _record: &BootstrapStateRecord) -> Result<(), BootstrapError> {
    Err(BootstrapError::UnsupportedPlatform)
}

#[cfg(unix)]
fn write_temporary_record(path: &Path, record: &BootstrapStateRecord) -> Result<PathBuf, BootstrapError> {
    use std::fs;
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::time::UNIX_EPOCH;

    static NEXT_TEMPORARY_ID: AtomicU64 = AtomicU64::new(0);

    let bytes = serde_json::to_vec(record).map_err(|_| BootstrapError::StateUnavailable)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("bootstrap-state");
    let timestamp = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let id = NEXT_TEMPORARY_ID.fetch_add(1, Ordering::Relaxed);
    let temporary = path.with_file_name(format!(".{file_name}.{}.{timestamp}.{id}.tmp", std::process::id()));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true).mode(0o600);
    let mut file = options.open(&temporary).map_err(|_| BootstrapError::StateUnavailable)?;
    let result = (|| {
        file.write_all(&bytes).map_err(|_| BootstrapError::StateUnavailable)?;
        file.sync_all().map_err(|_| BootstrapError::StateUnavailable)?;
        file.set_permissions(fs::Permissions::from_mode(0o400))
            .map_err(|_| BootstrapError::StateUnavailable)
    })();
    drop(file);
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map(|()| temporary)
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), BootstrapError> {
    let parent = path.parent().ok_or(BootstrapError::InvalidConfiguration)?;
    std::fs::File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|_| BootstrapError::StateUnavailable)
}
