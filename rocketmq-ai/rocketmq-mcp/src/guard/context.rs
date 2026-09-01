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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VisibilityClass {
    #[default]
    Standard,
    Sensitive,
}

impl VisibilityClass {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Sensitive => "sensitive",
        }
    }

    fn from_scopes(scopes: &BTreeSet<String>) -> Self {
        if scopes.contains("rocketmq:diagnose") || scopes.contains("rocketmq:plan") {
            Self::Sensitive
        } else {
            Self::Standard
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub id: String,
    pub tenant: Option<String>,
    pub roles: BTreeSet<String>,
    pub scopes: BTreeSet<String>,
    pub allowed_clusters: Option<BTreeSet<String>>,
}

impl Principal {
    pub fn local(profile: &str) -> Self {
        let mut scopes = BTreeSet::from(["rocketmq:read".to_string()]);
        if profile.eq_ignore_ascii_case("diagnose")
            || profile.eq_ignore_ascii_case("diagnostic")
            || profile.eq_ignore_ascii_case("operator")
        {
            scopes.insert("rocketmq:diagnose".to_string());
        }
        if profile.eq_ignore_ascii_case("plan") || profile.eq_ignore_ascii_case("operator") {
            scopes.insert("rocketmq:plan".to_string());
        }
        Self {
            id: "local-stdio".to_string(),
            tenant: None,
            roles: [profile.to_string()].into_iter().collect(),
            scopes,
            allowed_clusters: None,
        }
    }

    pub(crate) fn visibility_class(&self) -> VisibilityClass {
        VisibilityClass::from_scopes(&self.scopes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContext {
    pub principal: Principal,
    pub client: Option<String>,
}

impl RequestContext {
    pub fn local(profile: &str) -> Self {
        Self {
            principal: Principal::local(profile),
            client: Some("stdio".to_string()),
        }
    }

    pub(crate) fn visibility_class(&self) -> VisibilityClass {
        self.principal.visibility_class()
    }

    pub(crate) fn canonical_auth_claims(&self) -> Vec<u8> {
        let mut claims = b"rocketmq-mcp.discovery-auth.v1".to_vec();
        push_tlv(&mut claims, 0x01, self.principal.id.as_bytes());
        push_optional_tlv(&mut claims, 0x02, self.principal.tenant.as_deref());
        push_collection_tlv(&mut claims, 0x03, &self.principal.roles);
        push_collection_tlv(&mut claims, 0x04, &self.principal.scopes);
        push_optional_collection_tlv(&mut claims, 0x05, self.principal.allowed_clusters.as_ref());
        push_tlv(&mut claims, 0x06, self.visibility_class().as_str().as_bytes());
        claims
    }
}

fn push_tlv(output: &mut Vec<u8>, tag: u8, value: &[u8]) {
    output.push(tag);
    output.extend_from_slice(&(value.len() as u64).to_be_bytes());
    output.extend_from_slice(value);
}

fn push_optional_tlv(output: &mut Vec<u8>, tag: u8, value: Option<&str>) {
    let mut encoded = Vec::new();
    match value {
        Some(value) => {
            encoded.push(1);
            encoded.extend_from_slice(&(value.len() as u64).to_be_bytes());
            encoded.extend_from_slice(value.as_bytes());
        }
        None => encoded.push(0),
    }
    push_tlv(output, tag, &encoded);
}

fn push_collection_tlv(output: &mut Vec<u8>, tag: u8, values: &BTreeSet<String>) {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(values.len() as u64).to_be_bytes());
    for value in values {
        encoded.extend_from_slice(&(value.len() as u64).to_be_bytes());
        encoded.extend_from_slice(value.as_bytes());
    }
    push_tlv(output, tag, &encoded);
}

fn push_optional_collection_tlv(output: &mut Vec<u8>, tag: u8, values: Option<&BTreeSet<String>>) {
    let mut encoded = Vec::new();
    match values {
        Some(values) => {
            encoded.push(1);
            encoded.extend_from_slice(&(values.len() as u64).to_be_bytes());
            for value in values {
                encoded.extend_from_slice(&(value.len() as u64).to_be_bytes());
                encoded.extend_from_slice(value.as_bytes());
            }
        }
        None => encoded.push(0),
    }
    push_tlv(output, tag, &encoded);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_scope_mapping_is_closed_and_least_privilege() {
        for (scopes, expected) in [
            (&[][..], VisibilityClass::Standard),
            (&["rocketmq:read"][..], VisibilityClass::Standard),
            (&["unrelated:scope"][..], VisibilityClass::Standard),
            (&["rocketmq:diagnose"][..], VisibilityClass::Sensitive),
            (&["rocketmq:plan"][..], VisibilityClass::Sensitive),
            (
                &["rocketmq:read", "rocketmq:diagnose", "rocketmq:plan"][..],
                VisibilityClass::Sensitive,
            ),
        ] {
            let principal = Principal {
                id: "principal-not-retained-by-query-state".to_string(),
                tenant: None,
                roles: BTreeSet::new(),
                scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
                allowed_clusters: None,
            };
            assert_eq!(principal.visibility_class(), expected, "scopes={scopes:?}");
        }
    }

    #[test]
    fn local_profile_mapping_uses_the_same_scope_rule() {
        for (profile, expected) in [
            ("read_only", VisibilityClass::Standard),
            ("readonly", VisibilityClass::Standard),
            ("read-only", VisibilityClass::Standard),
            ("diagnose", VisibilityClass::Sensitive),
            ("diagnostic", VisibilityClass::Sensitive),
            ("operator", VisibilityClass::Sensitive),
        ] {
            let context = RequestContext::local(profile);
            assert_eq!(context.visibility_class(), expected, "profile={profile}");
        }
    }

    #[test]
    fn visibility_names_are_stable_and_non_sensitive() {
        assert_eq!(VisibilityClass::Standard.as_str(), "standard");
        assert_eq!(VisibilityClass::Sensitive.as_str(), "sensitive");
    }

    #[test]
    fn discovery_auth_claims_are_canonical_and_collision_resistant_by_construction() {
        fn context(tenant: Option<&str>, roles: &[&str], scopes: &[&str], clusters: Option<&[&str]>) -> RequestContext {
            RequestContext {
                principal: Principal {
                    id: "principal".to_string(),
                    tenant: tenant.map(ToString::to_string),
                    roles: roles.iter().map(|value| (*value).to_string()).collect(),
                    scopes: scopes.iter().map(|value| (*value).to_string()).collect(),
                    allowed_clusters: clusters.map(|values| values.iter().map(|value| (*value).to_string()).collect()),
                },
                client: None,
            }
        }

        let baseline = context(None, &["a", "bc"], &["rocketmq:read"], None);
        let cases = [
            context(None, &["ab", "c"], &["rocketmq:read"], None),
            context(None, &["rocketmq:read"], &["a", "bc"], None),
            context(Some(""), &["a", "bc"], &["rocketmq:read"], None),
            context(None, &["a", "bc"], &["rocketmq:read"], Some(&[])),
            context(None, &["a", "bc"], &["rocketmq:diagnose"], None),
        ];
        for candidate in cases {
            assert_ne!(baseline.canonical_auth_claims(), candidate.canonical_auth_claims());
        }

        let sorted = context(None, &["bc", "a"], &["rocketmq:read"], None);
        assert_eq!(baseline.canonical_auth_claims(), sorted.canonical_auth_claims());
    }
}
