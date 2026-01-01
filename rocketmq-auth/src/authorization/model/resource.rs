// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt;

use rocketmq_common::common::mix_all;
use rocketmq_common::common::resource::resource_pattern::ResourcePattern;
use rocketmq_common::common::resource::resource_type::ResourceType;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resource {
    pub resource_type: ResourceType,
    pub resource_name: Option<String>,
    pub resource_pattern: ResourcePattern,
}

impl Resource {
    pub fn of_cluster(cluster_name: &str) -> Self {
        Self::of(
            ResourceType::Cluster,
            Some(cluster_name.to_string()),
            ResourcePattern::Literal,
        )
    }

    pub fn of_topic(topic_name: &str) -> Self {
        Self::of(
            ResourceType::Topic,
            Some(topic_name.to_string()),
            ResourcePattern::Literal,
        )
    }

    pub fn of_group(mut group_name: String) -> Self {
        if NamespaceUtil::is_retry_topic(&group_name) {
            // strip retry prefix
            let prefix = mix_all::RETRY_GROUP_TOPIC_PREFIX;
            if group_name.starts_with(prefix) {
                group_name = group_name[prefix.len()..].to_string();
            }
        }
        Self::of(ResourceType::Group, Some(group_name), ResourcePattern::Literal)
    }

    pub fn of(resource_type: ResourceType, resource_name: Option<String>, resource_pattern: ResourcePattern) -> Self {
        Resource {
            resource_type,
            resource_name,
            resource_pattern,
        }
    }

    pub fn of_list<I, S>(resource_keys: I) -> Option<Vec<Self>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut out = Vec::new();
        for s in resource_keys.into_iter() {
            let sref = s.as_ref();
            if let Some(r) = Resource::of_str(sref) {
                out.push(r);
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    pub fn of_str(resource_key: &str) -> Option<Self> {
        let key = resource_key.trim();
        if key.is_empty() {
            return None;
        }
        if key == "*" {
            return Some(Self::of(ResourceType::Any, None, ResourcePattern::Any));
        }
        let (t, name) = match key.split_once(':') {
            Some((a, b)) => (a, b),
            None => (key, ""),
        };
        let resource_type = ResourceType::get_by_name(t)?;
        let mut resource_name = if name.is_empty() { None } else { Some(name.to_string()) };
        let mut resource_pattern = ResourcePattern::Literal;

        if let Some(ref rn) = resource_name {
            if rn == "*" {
                resource_name = None;
                resource_pattern = ResourcePattern::Any;
            } else if rn.ends_with('*') {
                let trimmed = rn.trim_end_matches('*').to_string();
                resource_name = Some(trimmed);
                resource_pattern = ResourcePattern::Prefixed;
            }
        }

        Some(Self::of(resource_type, resource_name, resource_pattern))
    }

    /// Serialized resource key for matching and storage.
    pub fn resource_key(&self) -> Option<String> {
        if self.resource_type == ResourceType::Any {
            return Some("*".to_string());
        }
        let type_name = self.resource_type.name();
        match self.resource_pattern {
            ResourcePattern::Any => Some(format!("{}:{}", type_name, "*")),
            ResourcePattern::Literal => Some(format!("{}:{}", type_name, self.resource_name.as_deref().unwrap_or(""))),
            ResourcePattern::Prefixed => Some(format!(
                "{}:{}*",
                type_name,
                self.resource_name.as_deref().unwrap_or("")
            )),
        }
    }

    /// Check whether `self` (the rule) matches the given `resource`.
    pub fn is_match(&self, resource: &Resource) -> bool {
        if self.resource_type == ResourceType::Any {
            return true;
        }
        if self.resource_type != resource.resource_type {
            return false;
        }
        match self.resource_pattern {
            ResourcePattern::Any => true,
            ResourcePattern::Literal => resource.resource_name.as_deref() == self.resource_name.as_deref(),
            ResourcePattern::Prefixed => match (&resource.resource_name, &self.resource_name) {
                (Some(target), Some(prefix)) => target.starts_with(prefix),
                _ => false,
            },
        }
    }

    // getters/setters
    pub fn resource_type(&self) -> ResourceType {
        self.resource_type
    }

    pub fn resource_name(&self) -> Option<&str> {
        self.resource_name.as_deref()
    }

    pub fn resource_pattern(&self) -> ResourcePattern {
        self.resource_pattern
    }

    pub fn set_resource_type(&mut self, t: ResourceType) {
        self.resource_type = t;
    }

    pub fn set_resource_name<S: Into<String>>(&mut self, name: Option<S>) {
        self.resource_name = name.map(Into::into);
    }

    pub fn set_resource_pattern(&mut self, p: ResourcePattern) {
        self.resource_pattern = p;
    }
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.resource_key() {
            Some(s) => write!(f, "{}", s),
            None => write!(f, ""),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_of_and_serialization() {
        let r = Resource::of(ResourceType::Topic, Some("foo".to_string()), ResourcePattern::Literal);
        assert_eq!(r.resource_key(), Some("Topic:foo".to_string()));

        let any = Resource::of_str("*").unwrap();
        assert_eq!(any.resource_key(), Some("*".to_string()));
    }

    #[test]
    fn test_of_parsing_patterns() {
        assert_eq!(
            Resource::of_str("Topic:*").unwrap().resource_pattern,
            ResourcePattern::Any
        );
        assert_eq!(
            Resource::of_str("Topic:pref*").unwrap().resource_pattern,
            ResourcePattern::Prefixed
        );
        assert_eq!(
            Resource::of_str("Topic:foo").unwrap().resource_pattern,
            ResourcePattern::Literal
        );
    }

    #[test]
    fn test_group_retry_strip() {
        let g = format!("{}{}", mix_all::RETRY_GROUP_TOPIC_PREFIX, "mygroup");
        let r = Resource::of_group(g);
        assert_eq!(r.resource_name(), Some("mygroup"));
    }

    #[test]
    fn test_is_match() {
        let rule_any = Resource::of(ResourceType::Any, None, ResourcePattern::Any);
        let res = Resource::of(ResourceType::Topic, Some("a".to_string()), ResourcePattern::Literal);
        assert!(rule_any.is_match(&res));

        let rule_pref = Resource::of(ResourceType::Topic, Some("pre".to_string()), ResourcePattern::Prefixed);
        let res2 = Resource::of(
            ResourceType::Topic,
            Some("prefix_val".to_string()),
            ResourcePattern::Literal,
        );
        assert!(rule_pref.is_match(&res2));

        let rule_lit = Resource::of(ResourceType::Topic, Some("t1".to_string()), ResourcePattern::Literal);
        assert!(rule_lit.is_match(&Resource::of(
            ResourceType::Topic,
            Some("t1".to_string()),
            ResourcePattern::Literal
        )));
        assert!(!rule_lit.is_match(&Resource::of(
            ResourceType::Topic,
            Some("t2".to_string()),
            ResourcePattern::Literal
        )));
    }

    #[test]
    fn test_of_list() {
        let keys = vec!["Topic:foo".to_string(), "Topic:bar".to_string()];
        let v = Resource::of_list(keys).unwrap();
        assert_eq!(v.len(), 2);

        let empty: Vec<String> = vec![];
        assert!(Resource::of_list(empty).is_none());
    }
}
