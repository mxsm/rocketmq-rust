use std::collections::HashMap;

use crate::authorization::model::environment::source_ip_matches;
use crate::migration::alc::acl_config::AclConfig;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WhiteList {
    global_patterns: Vec<String>,
    account_patterns: HashMap<String, Vec<String>>,
}

impl WhiteList {
    pub fn from_acl_config(acl_config: &AclConfig) -> Self {
        let global_patterns = acl_config
            .global_white_addrs()
            .unwrap_or_default()
            .iter()
            .flat_map(|value| normalize_patterns(value.as_str()))
            .collect();

        let account_patterns = acl_config
            .plain_access_configs()
            .unwrap_or_default()
            .iter()
            .filter_map(|account| {
                let access_key = account.access_key()?.as_str().trim();
                let white_remote_address = account.white_remote_address()?.as_str();
                if access_key.is_empty() {
                    return None;
                }

                let patterns = normalize_patterns(white_remote_address);
                if patterns.is_empty() {
                    return None;
                }
                Some((access_key.to_owned(), patterns))
            })
            .collect();

        Self {
            global_patterns,
            account_patterns,
        }
    }

    pub fn from_global_patterns<I, S>(patterns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            global_patterns: patterns
                .into_iter()
                .flat_map(|pattern| normalize_patterns(pattern.as_ref()))
                .collect(),
            account_patterns: HashMap::new(),
        }
    }

    pub fn with_global_patterns<I, S>(&self, patterns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            global_patterns: patterns
                .into_iter()
                .flat_map(|pattern| normalize_patterns(pattern.as_ref()))
                .collect(),
            account_patterns: self.account_patterns.clone(),
        }
    }

    pub fn matches(&self, access_key: Option<&str>, source_ip: Option<&str>) -> bool {
        let Some(source_ip) = valid_source_ip(source_ip) else {
            return false;
        };

        if self
            .global_patterns
            .iter()
            .any(|pattern| source_ip_matches(pattern, source_ip))
        {
            return true;
        }

        let Some(access_key) = access_key.map(str::trim).filter(|value| !value.is_empty()) else {
            return false;
        };

        self.account_patterns
            .get(access_key)
            .is_some_and(|patterns| patterns.iter().any(|pattern| source_ip_matches(pattern, source_ip)))
    }
}

fn normalize_patterns(value: &str) -> Vec<String> {
    value
        .split(';')
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
        .flat_map(expand_brace_pattern)
        .collect()
}

fn expand_brace_pattern(pattern: &str) -> Vec<String> {
    let Some(open) = pattern.find('{') else {
        return vec![pattern.to_owned()];
    };
    let Some(close_offset) = pattern[open + 1..].find('}') else {
        return vec![pattern.to_owned()];
    };
    let close = open + 1 + close_offset;
    let prefix = &pattern[..open];
    let suffix = &pattern[close + 1..];
    let choices = &pattern[open + 1..close];
    let expanded: Vec<String> = choices
        .split(',')
        .map(str::trim)
        .filter(|choice| !choice.is_empty())
        .map(|choice| format!("{prefix}{choice}{suffix}"))
        .collect();

    if expanded.is_empty() {
        vec![pattern.to_owned()]
    } else {
        expanded
    }
}

fn valid_source_ip(source_ip: Option<&str>) -> Option<&str> {
    source_ip
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("unknown"))
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;
    use crate::migration::alc::acl_config::AclConfig;
    use crate::migration::alc::plain_access_config::PlainAccessConfig;

    #[test]
    fn white_list_matches_global_and_account_patterns() {
        let mut account = PlainAccessConfig::default();
        account.set_access_key(CheetahString::from_static_str("alice"));
        account.set_white_remote_address(CheetahString::from_static_str("192.168.0.*;172.16.1.1"));

        let mut config = AclConfig::new();
        config.set_global_white_addrs(vec![CheetahString::from_static_str("10.10.*.*")]);
        config.set_plain_access_configs(vec![account]);

        let white_list = WhiteList::from_acl_config(&config);

        assert!(white_list.matches(None, Some("10.10.1.2")));
        assert!(white_list.matches(Some("alice"), Some("192.168.0.7")));
        assert!(white_list.matches(Some("alice"), Some("172.16.1.1")));
        assert!(!white_list.matches(Some("alice"), Some("192.168.1.7")));
        assert!(!white_list.matches(Some("bob"), Some("192.168.0.7")));
        assert!(!white_list.matches(Some("alice"), None));
        assert!(!white_list.matches(Some("alice"), Some("unknown")));
    }

    #[test]
    fn white_list_expands_java_brace_address_patterns() {
        let white_list = WhiteList::from_global_patterns(vec!["1.1.1.{1,2}", "2001:db8::{1,2}"]);

        assert!(white_list.matches(None, Some("1.1.1.1")));
        assert!(white_list.matches(None, Some("1.1.1.2")));
        assert!(!white_list.matches(None, Some("1.1.1.3")));
        assert!(white_list.matches(None, Some("2001:db8::1")));
        assert!(white_list.matches(None, Some("2001:db8::2")));
        assert!(!white_list.matches(None, Some("2001:db8::3")));
    }

    #[test]
    fn replacing_global_patterns_preserves_account_patterns() {
        let mut account = PlainAccessConfig::default();
        account.set_access_key(CheetahString::from_static_str("alice"));
        account.set_white_remote_address(CheetahString::from_static_str("192.168.0.*"));

        let mut config = AclConfig::new();
        config.set_global_white_addrs(vec![CheetahString::from_static_str("10.10.*.*")]);
        config.set_plain_access_configs(vec![account]);
        let white_list = WhiteList::from_acl_config(&config);

        let white_list = white_list.with_global_patterns(vec!["172.16.*.*"]);

        assert!(!white_list.matches(None, Some("10.10.1.2")));
        assert!(white_list.matches(None, Some("172.16.1.2")));
        assert!(white_list.matches(Some("alice"), Some("192.168.0.7")));
    }
}
