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
}
