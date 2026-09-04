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

use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::str::FromStr;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::error::ControlError;

pub const CAPABILITY_SCHEMA_VERSION: &str = "rocketmq-mcp-control.capability.v1";
pub const MUTATION_ARGUMENTS_SCHEMA_VERSION: &str = "rocketmq-mcp-control.arguments.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ControlOperation {
    TopicUpsert,
    ConsumerGroupUpsert,
    ConsumerOffsetReset,
    BrokerConfigPatch,
    ConsumerRequestMode,
}

impl ControlOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TopicUpsert => "topic_upsert",
            Self::ConsumerGroupUpsert => "consumer_group_upsert",
            Self::ConsumerOffsetReset => "consumer_offset_reset",
            Self::BrokerConfigPatch => "broker_config_patch",
            Self::ConsumerRequestMode => "consumer_request_mode",
        }
    }
}

impl FromStr for ControlOperation {
    type Err = ControlError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "topic_upsert" => Ok(Self::TopicUpsert),
            "consumer_group_upsert" => Ok(Self::ConsumerGroupUpsert),
            "consumer_offset_reset" => Ok(Self::ConsumerOffsetReset),
            "broker_config_patch" => Ok(Self::BrokerConfigPatch),
            "consumer_request_mode" => Ok(Self::ConsumerRequestMode),
            _ => Err(ControlError::permission_denied()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct ClusterName(String);

impl ClusterName {
    pub fn try_new(value: impl Into<String>) -> Result<Self, ControlError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(ControlError::permission_denied());
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ClusterName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Principal {
    pub subject: String,
    pub scopes: BTreeSet<String>,
    pub allowed_operations: BTreeSet<ControlOperation>,
    pub allowed_clusters: BTreeSet<ClusterName>,
}

impl fmt::Debug for Principal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Principal")
            .field("authenticated", &true)
            .field("scope_count", &self.scopes.len())
            .field("operation_count", &self.allowed_operations.len())
            .field("cluster_count", &self.allowed_clusters.len())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MutationArguments {
    pub schema_version: String,
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,
    #[serde(default)]
    pub confirm: bool,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub request_key: Option<String>,
}

impl MutationArguments {
    pub fn validate(&self) -> Result<(), ControlError> {
        if self.schema_version != MUTATION_ARGUMENTS_SCHEMA_VERSION
            || self
                .request_key
                .as_deref()
                .is_some_and(|value| !valid_request_key(value))
        {
            return Err(ControlError::invalid_argument());
        }
        if !self.dry_run && !self.confirm {
            return Err(ControlError::confirmation_required());
        }
        if self.reason.as_deref().is_some_and(|value| !valid_reason(value)) || (!self.dry_run && self.reason.is_none())
        {
            return Err(ControlError::invalid_argument());
        }
        Ok(())
    }
}

pub(crate) fn valid_operator(value: &str) -> bool {
    let bytes = value.as_bytes();
    if !(1..=128).contains(&bytes.len()) || !bytes.first().is_some_and(u8::is_ascii_alphanumeric) {
        return false;
    }
    if !bytes
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'@' | b'-'))
        || contains_bearer_material(value)
        || contains_credential_identifier(value)
    {
        return false;
    }
    match value.split_once('@') {
        Some((local, domain)) if value.matches('@').count() == 1 => valid_email_like_operator(local, domain),
        Some(_) => false,
        None => !contains_jwt_material(value) && !is_network_endpoint(value),
    }
}

fn valid_email_like_operator(local: &str, domain: &str) -> bool {
    let domain_without_root = domain.strip_suffix('.').unwrap_or(domain);
    if domain_without_root.parse::<IpAddr>().is_ok() || domain_without_root != domain {
        return false;
    }
    let Some(top_level) = domain.rsplit('.').next() else {
        return false;
    };
    !local.is_empty()
        && local.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        && local.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
        && !local.contains("..")
        && !unsafe_email_local_token(local)
        && domain.contains('.')
        && is_hostname(domain)
        && top_level.bytes().any(|byte| byte.is_ascii_alphabetic())
        && !["internal", "local", "localhost", "lan"]
            .iter()
            .any(|reserved| top_level.eq_ignore_ascii_case(reserved))
}

fn unsafe_email_local_token(local: &str) -> bool {
    compact_token_segments(local)
        .is_some_and(|(header, _, signature)| signature.is_empty() || signature == "_" || has_jose_header(header))
}

fn has_jose_header(header: &str) -> bool {
    let Ok(decoded) = URL_SAFE_NO_PAD.decode(header) else {
        return false;
    };
    let Ok(serde_json::Value::Object(object)) = serde_json::from_slice(&decoded) else {
        return false;
    };
    [
        "alg", "typ", "kid", "jwk", "jku", "x5u", "x5c", "x5t", "x5t#S256", "crit", "cty", "enc", "zip", "b64",
    ]
    .iter()
    .any(|field| object.contains_key(*field))
}

pub(crate) fn valid_reason(value: &str) -> bool {
    let bytes = value.as_bytes();
    (5..=256).contains(&bytes.len())
        && value.trim() == value
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b' ' | b'.' | b'_' | b',' | b'#' | b'-'))
        && !contains_bearer_material(value)
        && !contains_reason_jwt_material(value)
        && !contains_reason_network_endpoint(value)
}

fn contains_bearer_material(value: &str) -> bool {
    value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .any(|word| word.eq_ignore_ascii_case("bearer"))
}

fn contains_jwt_material(value: &str) -> bool {
    value
        .split(|character: char| !character.is_ascii_alphanumeric() && !matches!(character, '_' | '-' | '.'))
        .any(|token| compact_token_segments(token).is_some())
}

fn contains_reason_jwt_material(value: &str) -> bool {
    contains_unsafe_reason_candidate(value, |candidate| compact_token_segments(candidate).is_some())
}

fn compact_token_segments(value: &str) -> Option<(&str, &str, &str)> {
    let mut segments = value.split('.');
    let header = segments.next()?;
    let payload = segments.next()?;
    let signature = segments.next()?;
    if segments.next().is_some()
        || header.is_empty()
        || payload.is_empty()
        || ![header, payload, signature].iter().all(|segment| {
            segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        })
    {
        None
    } else {
        Some((header, payload, signature))
    }
}

fn contains_credential_identifier(value: &str) -> bool {
    let words = value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    words.iter().enumerate().any(|(start, _)| {
        (1..=3.min(words.len() - start)).any(|count| is_credential_key(&words[start..start + count].concat()))
    })
}

fn is_credential_key(key: &str) -> bool {
    matches!(
        key,
        "ak" | "sk"
            | "accesskey"
            | "secret"
            | "secretkey"
            | "securitytoken"
            | "token"
            | "password"
            | "passwd"
            | "apikey"
            | "clientsecret"
            | "authorization"
            | "credential"
            | "credentials"
    )
}

fn contains_reason_network_endpoint(value: &str) -> bool {
    contains_unsafe_reason_candidate(value, is_network_endpoint)
}

fn contains_unsafe_reason_candidate(value: &str, is_unsafe: impl Fn(&str) -> bool) -> bool {
    value.split_ascii_whitespace().any(|token| {
        std::iter::once(token)
            .chain(token.split([',', '#', '_', '-']))
            .any(|part| {
                std::iter::once(part)
                    .chain(part.split(".."))
                    .map(|candidate| candidate.trim_matches(|character: char| !character.is_ascii_alphanumeric()))
                    .filter(|candidate| !candidate.is_empty())
                    .any(&is_unsafe)
            })
    })
}

fn is_network_endpoint(token: &str) -> bool {
    let token = token.trim_matches(|character: char| matches!(character, '.' | '!' | '?'));
    let authority = token.split(['/', '?', '#']).next().unwrap_or(token);
    if authority.parse::<IpAddr>().is_ok() || authority.parse::<SocketAddr>().is_ok() {
        return true;
    }
    if let Some(bracketed) = authority.strip_prefix('[') {
        if let Some((address, suffix)) = bracketed.split_once(']') {
            let address = address.split_once('%').map_or(address, |(address, _)| address);
            if address.parse::<IpAddr>().is_ok()
                && (suffix.is_empty()
                    || suffix
                        .strip_prefix(':')
                        .is_some_and(|port| port.parse::<u16>().is_ok_and(|port| port != 0)))
            {
                return true;
            }
        }
    }
    if let Some((address, zone)) = authority.split_once('%') {
        if !zone.is_empty() && address.parse::<IpAddr>().is_ok() {
            return true;
        }
    }
    let Some((host, port)) = authority.rsplit_once(':') else {
        return is_fqdn(authority);
    };
    let valid_port = port.parse::<u16>().is_ok_and(|port| port != 0);
    let host = host.strip_suffix('.').unwrap_or(host);
    let valid_host = host.parse::<IpAddr>().is_ok() || is_hostname(host);
    valid_port && valid_host
}

fn is_fqdn(value: &str) -> bool {
    let value = value.strip_suffix('.').unwrap_or(value);
    value.contains('.') && value.bytes().any(|byte| byte.is_ascii_alphabetic()) && is_hostname(value)
}

fn is_hostname(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
                && label.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
                && label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

const fn default_dry_run() -> bool {
    true
}

fn valid_request_key(value: &str) -> bool {
    (8..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

/// Capability state derived only from the closed operation catalog.
///
/// Fields are intentionally private so callers cannot fabricate support.
///
/// ```compile_fail
/// use rocketmq_mcp_control::model::ControlCapabilities;
/// let _ = ControlCapabilities {
///     schema_version: "fabricated",
///     write_tools_compiled: true,
///     mutations_runtime_enabled: true,
///     registered_operations: 1,
///     mutation_supported: true,
///     transport: "other",
///     authentication: "other",
///     max_request_bytes: 0,
///     request_timeout_seconds: 0,
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ControlCapabilities {
    schema_version: &'static str,
    write_tools_compiled: bool,
    mutations_runtime_enabled: bool,
    registered_operations: u32,
    mutation_supported: bool,
    transport: &'static str,
    authentication: &'static str,
    max_request_bytes: usize,
    request_timeout_seconds: u64,
}

impl ControlCapabilities {
    pub(crate) fn from_catalog(mutations_runtime_enabled: bool, catalog: &crate::catalog::OperationCatalog) -> Self {
        let write_tools_compiled = cfg!(feature = "write-tools");
        let registered_operations = catalog.registered_operations();
        Self {
            schema_version: CAPABILITY_SCHEMA_VERSION,
            write_tools_compiled,
            mutations_runtime_enabled,
            registered_operations,
            mutation_supported: write_tools_compiled && mutations_runtime_enabled && registered_operations > 0,
            transport: "streamable_https",
            authentication: "oauth_rs256_jwks",
            max_request_bytes: 1024 * 1024,
            request_timeout_seconds: 30,
        }
    }

    pub const fn schema_version(&self) -> &str {
        self.schema_version
    }

    pub const fn write_tools_compiled(&self) -> bool {
        self.write_tools_compiled
    }

    pub const fn mutations_runtime_enabled(&self) -> bool {
        self.mutations_runtime_enabled
    }

    pub const fn registered_operations(&self) -> u32 {
        self.registered_operations
    }

    pub const fn mutation_supported(&self) -> bool {
        self.mutation_supported
    }

    pub const fn transport(&self) -> &str {
        self.transport
    }

    pub const fn authentication(&self) -> &str {
        self.authentication
    }

    pub const fn max_request_bytes(&self) -> usize {
        self.max_request_bytes
    }

    pub const fn request_timeout_seconds(&self) -> u64 {
        self.request_timeout_seconds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_arguments() -> serde_json::Value {
        serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "dry_run": true,
            "confirm": false
        })
    }

    #[test]
    fn common_arguments_are_closed_and_bounded() {
        let arguments: MutationArguments = serde_json::from_value(valid_arguments()).unwrap();
        arguments.validate().unwrap();
        assert!(arguments.dry_run);
        assert!(!arguments.confirm);
        assert!(arguments.reason.is_none());
        assert!(arguments.request_key.is_none());

        let optional_nulls: MutationArguments = serde_json::from_value(serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "reason": null,
            "request_key": null
        }))
        .unwrap();
        optional_nulls.validate().unwrap();

        let execute: MutationArguments = serde_json::from_value(serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "dry_run": false,
            "confirm": true,
            "reason": "planned change",
            "request_key": "request-1234"
        }))
        .unwrap();
        execute.validate().unwrap();

        let mut cases = Vec::new();
        let mut unknown = valid_arguments();
        unknown["unknown"] = serde_json::json!(true);
        cases.push(unknown);
        for (field, value) in [
            ("reason", serde_json::json!("")),
            ("reason", serde_json::json!("four")),
            ("reason", serde_json::json!("operator\ncommand")),
            ("reason", serde_json::json!(42)),
            ("reason", serde_json::json!("`command`")),
            ("reason", serde_json::json!("x".repeat(257))),
            ("request_key", serde_json::json!("short")),
            ("request_key", serde_json::json!("bad key")),
            ("request_key", serde_json::json!("x".repeat(65))),
            ("request_key", serde_json::json!(42)),
            ("confirm", serde_json::Value::Null),
            ("confirm", serde_json::json!("yes")),
            ("dry_run", serde_json::Value::Null),
            ("dry_run", serde_json::json!("true")),
        ] {
            let mut case = valid_arguments();
            case[field] = value;
            cases.push(case);
        }
        let mut execute_without_confirmation = valid_arguments();
        execute_without_confirmation["dry_run"] = serde_json::json!(false);
        cases.push(execute_without_confirmation);

        let mut execute_without_reason = valid_arguments();
        execute_without_reason["dry_run"] = serde_json::json!(false);
        execute_without_reason["confirm"] = serde_json::json!(true);
        cases.push(execute_without_reason);

        for case in cases {
            let rejected = serde_json::from_value::<MutationArguments>(case)
                .map_err(|_| ControlError::invalid_argument())
                .and_then(|arguments| arguments.validate());
            assert!(rejected.is_err());
        }
    }

    #[test]
    fn confirmation_operator_and_reason_validation_have_stable_codes() {
        let execute_without_confirmation: MutationArguments = serde_json::from_value(serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "dry_run": false,
            "confirm": false,
            "reason": "token=must-not-be-inspected-first"
        }))
        .unwrap();
        assert_eq!(
            execute_without_confirmation.validate().unwrap_err().code(),
            crate::error::ControlErrorCode::ConfirmationRequired
        );

        let execute_without_reason: MutationArguments = serde_json::from_value(serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "dry_run": false,
            "confirm": true
        }))
        .unwrap();
        assert_eq!(
            execute_without_reason.validate().unwrap_err().code(),
            crate::error::ControlErrorCode::InvalidArgument
        );

        for (case, unsafe_reason) in [
            "token=top-secret",
            "token%3dtop-secret",
            "token%253dtop-secret",
            "token%25253dtop-secret",
            "\"token\" = top-secret",
            "[secret_key]: top-secret",
            "credentials['access_key'] = top-secret",
            "Bearer abc.def.ghi",
            "Bearer%20abc.def.ghi",
            "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJvcGVyYXRvciJ9.signature-value",
            "compact a.b._ material",
            "unsigned a.b. material",
            "token=a.b._",
            "https://control.invalid/change",
            "https%3a%2f%2fcontrol.invalid%2fchange",
            "//control.invalid/change",
            "custom:opaque-location",
            "broker.internal:10911",
            "broker.internal.",
            "broker.internal.:10911",
            "broker%2einternal%3a10911",
            "broker.internal:10911/admin",
            "10.0.0.1",
            "[fe80::1%eth0]:10911",
            "fe80::1%eth0",
            "endpoint=broker.internal:10911",
            "endpoint='10.0.0.1:10911'",
            "endpoint=[fe80::1%eth0]:10911",
            "target=[broker.internal:10911]",
            "user@broker.internal:10911",
            "ops@10.0.0.1",
            "host=(broker.internal.)",
            "target=/broker.internal/",
            "target=\\broker.internal\\",
            "|broker.internal|",
            ":broker.internal:",
            "-broker.internal-",
            "[broker.internal]/",
            "owner@broker.internal",
            "http:broker.internal",
            "{10.0.0.1}",
            "(a.b._)",
            "route,broker.internal,now",
            "route 10.0.0.1,next",
            "route#broker.internal#now",
            "route_10.0.0.1_now",
            "route..10.0.0.1..now",
            "route..broker.internal..now",
            "note..a.b.c..now",
            "approved fullwidth token＝secret",
            "approved fullwidth colon：secret",
            "approved bidi \u{202e} text",
            "approved format \u{200b} text",
            "approved separator \u{2028} text",
        ]
        .into_iter()
        .enumerate()
        {
            assert!(!valid_reason(unsafe_reason), "unsafe reason case {case} was accepted");
            let error = crate::audit::AuditContext::try_new("operator@example.test", Some(unsafe_reason)).unwrap_err();
            assert_eq!(error.code(), crate::error::ControlErrorCode::InvalidArgument);
            assert_eq!(error.to_string(), "mutation argument is invalid");
        }
        for (case, safe_reason) in [
            "approved maintenance change",
            "increase topic queue count",
            "repair consumer request mode",
            "change increase queue count",
            "CHG-1234 increase queue count",
            "ticket INC_42, increase queue count",
            "issue #42 release 1.2 approved",
        ]
        .into_iter()
        .enumerate()
        {
            assert!(valid_reason(safe_reason), "safe reason case {case} was rejected");
        }
        for valid in [
            "operator@example.test",
            "operator@sub.example.test",
            "operator@team.example.com",
            "first.middle.last@example.test",
            "operator@mail.example.co.uk",
            "123e4567-e89b-12d3-a456-426614174000",
            "svc-control_01",
            "1-service",
        ] {
            assert!(valid_operator(valid));
        }
        for (case, invalid) in [
            "",
            " operator",
            "operator ",
            "operator name",
            "operator\nadmin",
            "https://identity.invalid/operator",
            "token=top-secret",
            "token",
            "svc-secret",
            "Bearer abc.def.ghi",
            "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJvcGVyYXRvciJ9.signature-value",
            "a.b._",
            "a.b.",
            "a.b._@example.test",
            "eyJhbGciOiJSUzI1NiJ9.e30.x@example.test",
            "eyJhbGciOiJub25lIn0.e30.x@example.test",
            "eyJhbGciOiJSUzk5OSJ9.e30.x@example.test",
            "eyJ0eXAiOiJKV1QifQ.e30.x@example.test",
            "eyJhbGciOm51bGx9.e30.x@example.test",
            "10.0.0.1",
            "10.0.0.1:10911",
            "broker.internal.",
            "operator%25admin",
            "operator/path",
            "operator\u{202e}admin",
            "operator\u{2028}admin",
            "operator：admin",
            "＠operator",
            "operator@",
            "operator@10.0.0.1",
            "operator@10.0.0.1.",
            "operator@broker.internal",
            "operator@broker.internal.",
            "operator@example.123",
        ]
        .into_iter()
        .enumerate()
        {
            assert!(!valid_operator(invalid), "unsafe operator case {case} was accepted");
            let error = crate::audit::AuditContext::try_new(invalid, None).unwrap_err();
            assert_eq!(error.code(), crate::error::ControlErrorCode::PermissionDenied);
            assert_eq!(error.to_string(), "write permission is required");
        }
        assert!(!valid_operator(&"x".repeat(129)));
    }

    #[test]
    fn reason_endpoint_candidates_fail_closed_across_contexts() {
        for (endpoint_case, endpoint) in ["broker.internal", "broker.internal.", "10.0.0.1"]
            .into_iter()
            .enumerate()
        {
            for (context_case, reason) in [
                endpoint.to_owned(),
                format!("-{endpoint}-"),
                format!("#{endpoint}#"),
                format!(",,,{endpoint},,,"),
                format!("route,{endpoint},now"),
                format!("route#{endpoint}#now"),
                format!("route_{endpoint}_now"),
                format!("route-{endpoint}-now"),
                format!("route,_#{endpoint}#_,now"),
                format!("route..{endpoint}..now"),
            ]
            .into_iter()
            .enumerate()
            {
                assert!(
                    !valid_reason(&reason),
                    "endpoint case {endpoint_case}, context case {context_case} was accepted"
                );
                let error = crate::audit::AuditContext::try_new("operator@example.test", Some(&reason)).unwrap_err();
                assert_eq!(error, ControlError::invalid_argument());
                assert_eq!(error.to_string(), "mutation argument is invalid");
            }
        }

        for (case, syntax) in [
            '/', '\\', '|', ':', '=', '@', '[', ']', '{', '}', '(', ')', '\'', '"', '%', '<', '>', '`',
        ]
        .into_iter()
        .enumerate()
        {
            let reason = format!("approved{syntax}change");
            assert!(!valid_reason(&reason), "unsafe syntax case {case} was accepted");
            let error = crate::audit::AuditContext::try_new("operator@example.test", Some(&reason)).unwrap_err();
            assert_eq!(error, ControlError::invalid_argument());
            assert_eq!(error.to_string(), "mutation argument is invalid");
        }
    }

    #[test]
    fn cluster_aliases_and_operation_ids_are_closed() {
        for valid in ["cluster-a", "cluster_A", "A1"] {
            assert!(ClusterName::try_new(valid).is_ok());
        }
        for invalid in ["", "cluster.a", "10.0.0.1", "host:9876", "token=secret"] {
            assert!(ClusterName::try_new(invalid).is_err());
        }
        assert!(ClusterName::try_new("x".repeat(65)).is_err());
        for operation in [
            "topic_upsert",
            "consumer_group_upsert",
            "consumer_offset_reset",
            "broker_config_patch",
            "consumer_request_mode",
        ] {
            assert!(ControlOperation::from_str(operation).is_ok());
        }
        for rejected in [
            "skip_accumulated_messages",
            "resend_dead_letter_message",
            "free_form_rpc",
        ] {
            assert!(ControlOperation::from_str(rejected).is_err());
        }
    }
}
