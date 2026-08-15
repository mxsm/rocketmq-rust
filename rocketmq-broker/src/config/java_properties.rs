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

use std::collections::HashSet;

use rocketmq_store::StoreType;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value;

use super::error::BrokerConfigError;
use super::raw::RawBrokerConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum JavaConfigEntryStatus {
    Mapped,
    Renamed,
    AlternativeEquivalent,
    NotApplicable,
}

impl JavaConfigEntryStatus {
    pub const fn is_mapped(self) -> bool {
        matches!(self, Self::Mapped | Self::Renamed | Self::AlternativeEquivalent)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JavaConfigEntry {
    original_key: String,
    canonical_key: String,
    owner: String,
    status: JavaConfigEntryStatus,
    sensitive: bool,
    value_state: &'static str,
}

impl JavaConfigEntry {
    pub fn original_key(&self) -> &str {
        &self.original_key
    }

    pub fn canonical_key(&self) -> &str {
        &self.canonical_key
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub const fn status(&self) -> JavaConfigEntryStatus {
        self.status
    }

    pub const fn sensitive(&self) -> bool {
        self.sensitive
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JavaConfigWarning {
    key: String,
    action: String,
}

impl JavaConfigWarning {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn action(&self) -> &str {
        &self.action
    }
}

#[derive(Debug)]
pub struct JavaConfigConversion {
    config: RawBrokerConfig,
    entries: Vec<JavaConfigEntry>,
    warnings: Vec<JavaConfigWarning>,
}

impl JavaConfigConversion {
    pub const fn config(&self) -> &RawBrokerConfig {
        &self.config
    }

    pub fn entries(&self) -> &[JavaConfigEntry] {
        &self.entries
    }

    pub fn warnings(&self) -> &[JavaConfigWarning] {
        &self.warnings
    }

    pub fn into_config(self) -> RawBrokerConfig {
        self.config
    }

    pub fn report_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&serde_json::json!({
            "entries": self.entries,
            "warnings": self.warnings,
        }))
    }
}

pub struct JavaBrokerProperties;

impl JavaBrokerProperties {
    pub fn parse(input: &str) -> Result<JavaConfigConversion, BrokerConfigError> {
        let mut root = Map::new();
        root.insert("broker".to_owned(), Value::Object(Map::new()));
        root.insert("store".to_owned(), Value::Object(Map::new()));
        let mut seen_original = HashSet::new();
        let mut seen_canonical = HashSet::new();
        let mut entries = Vec::new();
        let mut warnings = Vec::new();

        for (line_index, line) in input.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
                continue;
            }
            let separator = line.find(['=', ':']).or_else(|| line.find(char::is_whitespace));
            let Some(separator) = separator else {
                return Err(invalid_property(
                    format!("line {}", line_index + 1),
                    "non-empty text",
                    "key=value, key:value, or key value",
                ));
            };
            let key = line[..separator].trim();
            let value = line[separator + 1..]
                .trim_start_matches(char::is_whitespace)
                .strip_prefix(['=', ':'])
                .unwrap_or(&line[separator + 1..])
                .trim();
            if key.is_empty() {
                return Err(invalid_property(
                    format!("line {}", line_index + 1),
                    "empty key",
                    "non-empty Java property key",
                ));
            }
            if !seen_original.insert(key.to_owned()) {
                return Err(invalid_property(key, "duplicate", "one value per Java property"));
            }
            if key.to_ascii_lowercase().contains("dledger") {
                return Err(invalid_property(
                    key,
                    "DLedger configuration",
                    "DLedger CommitLog is outside the Rust 1.0 scope",
                ));
            }

            let mapping = resolve_mapping(key, value, &root)?;
            if !seen_canonical.insert(mapping.canonical_key.clone()) {
                return Err(invalid_property(
                    key,
                    "alias conflict",
                    "one Java source for each canonical property",
                ));
            }
            for path in &mapping.paths {
                if !insert_path(&mut root, path, mapping.value.clone()) {
                    return Err(invalid_property(
                        key,
                        "canonical owner conflict",
                        "a non-conflicting broker/store/logging owner path",
                    ));
                }
            }
            if mapping.sensitive_file_reference {
                warnings.push(JavaConfigWarning {
                    key: key.to_owned(),
                    action: "copy or mount the referenced file separately; its contents are not embedded".to_owned(),
                });
            }
            entries.push(JavaConfigEntry {
                original_key: key.to_owned(),
                canonical_key: mapping.canonical_key,
                owner: mapping.owner.to_owned(),
                status: mapping.status,
                sensitive: mapping.sensitive,
                value_state: if mapping.sensitive { "configured" } else { "validated" },
            });
        }

        let config = RawBrokerConfig::from_serde_value(Value::Object(root)).map_err(|_| {
            invalid_property(
                "java properties",
                "invalid typed value",
                "a complete Java 5.5 configuration that maps to the canonical Rust schema",
            )
        })?;
        Ok(JavaConfigConversion {
            config,
            entries,
            warnings,
        })
    }
}

struct ResolvedMapping {
    paths: Vec<Vec<String>>,
    canonical_key: String,
    owner: &'static str,
    status: JavaConfigEntryStatus,
    value: Value,
    sensitive: bool,
    sensitive_file_reference: bool,
}

fn resolve_mapping(key: &str, value: &str, root: &Map<String, Value>) -> Result<ResolvedMapping, BrokerConfigError> {
    let manual = match key {
        "brokerName" | "brokerClusterName" | "brokerId" => Some((
            vec!["broker", "brokerIdentity", key],
            "broker.identity",
            JavaConfigEntryStatus::Renamed,
            value_candidates(value),
        )),
        "brokerIP1" => Some((
            vec!["broker", "brokerIp1"],
            "broker.network",
            JavaConfigEntryStatus::Renamed,
            value_candidates(value),
        )),
        "brokerIP2" => Some((
            vec!["broker", "brokerIp2"],
            "broker.network",
            JavaConfigEntryStatus::Renamed,
            value_candidates(value),
        )),
        "brokerRole" | "flushDiskType" | "maxMessageSize" => Some((
            vec!["store", key],
            "store",
            JavaConfigEntryStatus::Mapped,
            value_candidates(value),
        )),
        "storeType" => {
            let store_type = StoreType::from_java_alias(value).ok_or_else(|| {
                invalid_property(
                    key,
                    "unknown store type",
                    "default or defaultRocksDB (ASCII case-insensitive)",
                )
            })?;
            Some((
                vec!["store", "storeType"],
                "store",
                JavaConfigEntryStatus::Renamed,
                vec![Value::String(store_type.get_store_type().to_owned())],
            ))
        }
        "storePathRootDir" => {
            let candidate = Value::String(value.to_owned());
            return Ok(ResolvedMapping {
                paths: vec![
                    vec!["broker".to_owned(), key.to_owned()],
                    vec!["store".to_owned(), key.to_owned()],
                ],
                canonical_key: "broker.storePathRootDir+store.storePathRootDir".to_owned(),
                owner: "broker+store",
                status: JavaConfigEntryStatus::AlternativeEquivalent,
                value: candidate,
                sensitive: false,
                sensitive_file_reference: false,
            });
        }
        "logFilter" => Some((
            vec!["logFilter"],
            "logging",
            JavaConfigEntryStatus::Mapped,
            vec![Value::String(value.to_owned())],
        )),
        _ => None,
    };
    if let Some((path, owner, status, candidates)) = manual {
        return select_candidate(key, path, owner, status, candidates, root);
    }

    for owner in ["broker", "store"] {
        let path = vec![owner, key];
        if let Ok(mapping) = select_candidate(
            key,
            path,
            owner,
            JavaConfigEntryStatus::Mapped,
            value_candidates(value),
            root,
        ) {
            return Ok(mapping);
        }
    }
    Err(BrokerConfigError::UnsupportedKeys { keys: key.to_owned() })
}

fn select_candidate(
    original_key: &str,
    path: Vec<&str>,
    owner: &'static str,
    status: JavaConfigEntryStatus,
    candidates: Vec<Value>,
    root: &Map<String, Value>,
) -> Result<ResolvedMapping, BrokerConfigError> {
    for candidate in candidates {
        let mut trial = root.clone();
        let owned_path = path.iter().map(|part| (*part).to_owned()).collect::<Vec<_>>();
        if !insert_path(&mut trial, &owned_path, candidate.clone()) {
            continue;
        }
        if RawBrokerConfig::from_serde_value(Value::Object(trial)).is_ok() {
            let canonical_key = owned_path.join(".");
            let sensitive_file_reference = matches!(original_key, "authConfigPath" | "aclFile");
            let sensitive = sensitive_file_reference
                || original_key.to_ascii_lowercase().contains("password")
                || original_key.to_ascii_lowercase().contains("secret");
            return Ok(ResolvedMapping {
                paths: vec![owned_path],
                canonical_key,
                owner,
                status,
                value: candidate,
                sensitive,
                sensitive_file_reference,
            });
        }
    }
    Err(invalid_property(
        original_key,
        "invalid typed value",
        "the Java 5.5 property type",
    ))
}

fn value_candidates(value: &str) -> Vec<Value> {
    let mut candidates = vec![Value::String(value.to_owned())];
    if value.eq_ignore_ascii_case("true") {
        candidates.push(Value::Bool(true));
    } else if value.eq_ignore_ascii_case("false") {
        candidates.push(Value::Bool(false));
    }
    if let Ok(number) = value.parse::<i64>() {
        candidates.push(Value::Number(number.into()));
    }
    if let Ok(number) = value.parse::<u64>() {
        candidates.push(Value::Number(number.into()));
    }
    if let Ok(number) = value.parse::<f64>() {
        if let Some(number) = serde_json::Number::from_f64(number) {
            candidates.push(Value::Number(number));
        }
    }
    candidates
}

fn insert_path(root: &mut Map<String, Value>, path: &[String], value: Value) -> bool {
    let mut current = root;
    for part in &path[..path.len().saturating_sub(1)] {
        let entry = current.entry(part.clone()).or_insert_with(|| Value::Object(Map::new()));
        let Some(object) = entry.as_object_mut() else {
            return false;
        };
        current = object;
    }
    if let Some(last) = path.last() {
        current.insert(last.clone(), value);
    }
    true
}

fn invalid_property(
    key: impl Into<String>,
    value_class: impl Into<String>,
    expected: &'static str,
) -> BrokerConfigError {
    BrokerConfigError::InvalidProperty {
        key: key.into(),
        value: value_class.into(),
        expected,
    }
}
