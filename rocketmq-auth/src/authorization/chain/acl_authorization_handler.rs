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

//! ACL-based authorization handler implementation.
//!
//! This module implements Access Control List (ACL) authorization as part of the
//! authorization chain. It evaluates incoming requests against stored ACL policies
//! and makes allow/deny decisions based on:
//! - Resource matching (topic, group, cluster)
//! - Action matching (PUB, SUB, CREATE, etc.)
//! - Environment matching (source IP, etc.)
//! - Policy priority (CUSTOM > DEFAULT, DENY > ALLOW, LITERAL > PREFIXED)

use std::cmp::Ordering;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rocketmq_common::common::resource::resource_pattern::ResourcePattern;
use rocketmq_common::common::resource::resource_type::ResourceType;
use rocketmq_error::RocketMQError;

use super::handler::AuthorizationHandler;
use crate::authentication::model::user::User;
use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::enums::decision::Decision;
use crate::authorization::enums::policy_type::PolicyType;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::authorization::model::acl::Acl;
use crate::authorization::model::environment::Environment;
use crate::authorization::model::policy::Policy;
use crate::authorization::model::policy_entry::PolicyEntry;

/// ACL Authorization Handler.
///
/// This handler performs ACL-based authorization by:
/// 1. Loading the subject's ACL from the metadata provider
/// 2. Matching policy entries against the request context
/// 3. Evaluating the decision (ALLOW/DENY) based on priority
///
/// # Priority Rules
///
/// Policies are evaluated in this priority order:
/// 1. **Resource Type**: Specific types (Topic, Group) > ANY
/// 2. **Resource Pattern**: LITERAL > PREFIXED > ANY
/// 3. **Resource Name Length**: Longer prefixes > shorter (for PREFIXED pattern)
/// 4. **Decision**: DENY > ALLOW (deny takes precedence)
pub struct AclAuthorizationHandler<P: AuthorizationMetadataProvider> {
    /// Provider for fetching ACL metadata
    metadata_provider: Arc<P>,
}

impl<P: AuthorizationMetadataProvider> AclAuthorizationHandler<P> {
    /// Create a new ACL authorization handler.
    ///
    /// # Arguments
    ///
    /// * `metadata_provider` - Provider for retrieving ACL data
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use rocketmq_auth::authorization::chain::AclAuthorizationHandler;
    /// use rocketmq_auth::authorization::metadata_provider::LocalAuthorizationMetadataProvider;
    /// use std::sync::Arc;
    ///
    /// let provider = Arc::new(LocalAuthorizationMetadataProvider::new());
    /// let handler = AclAuthorizationHandler::new(provider);
    /// ```
    pub fn new(metadata_provider: Arc<P>) -> Self {
        Self { metadata_provider }
    }

    /// Match policy entries from an ACL against the authorization context.
    ///
    /// This method:
    /// 1. Tries CUSTOM policies first
    /// 2. Falls back to DEFAULT policies if no CUSTOM match
    /// 3. Sorts matched entries by priority
    /// 4. Returns the highest-priority entry
    async fn match_policy_entries(&self, context: &DefaultAuthorizationContext, acl: &Acl) -> Option<PolicyEntry> {
        let mut matched_entries = Vec::new();

        // Step 1: Try CUSTOM policies first
        if let Some(policy) = acl.get_policy(PolicyType::Custom) {
            if let Some(mut entries) = self.match_policy_entries_from_policy(context, policy) {
                matched_entries.append(&mut entries);
            }
        }

        // Step 2: If no CUSTOM matches, try DEFAULT policies
        if matched_entries.is_empty() {
            if let Some(policy) = acl.get_policy(PolicyType::Default) {
                if let Some(mut entries) = self.match_policy_entries_from_policy(context, policy) {
                    matched_entries.append(&mut entries);
                }
            }
        }

        // Step 3: No matches found
        if matched_entries.is_empty() {
            return None;
        }

        // Step 4: Sort by priority and return the first (highest priority)
        matched_entries.sort_by(|a, b| self.compare_policy_entries(a, b));

        // Return first element (highest priority)
        matched_entries.into_iter().next()
    }

    /// Match policy entries from a specific policy against the context.
    ///
    /// Filters entries that match:
    /// - Resource (type, name, pattern)
    /// - Actions (must include at least one requested action)
    /// - Environment (source IP, etc.)
    ///
    /// # Java Mapping
    ///
    /// Maps to `matchPolicyEntries(DefaultAuthorizationContext, List<PolicyEntry>)`
    fn match_policy_entries_from_policy(
        &self,
        context: &DefaultAuthorizationContext,
        policy: &Policy,
    ) -> Option<Vec<PolicyEntry>> {
        let entries = policy.entries();
        if entries.is_empty() {
            return None;
        }

        let resource_binding = context.resource();
        let resource = resource_binding.as_ref()?;
        let actions = context.actions();
        let source_ip_binding = context.source_ip();
        let source_ip = source_ip_binding.unwrap_or("");
        let environment = Environment::of(source_ip).unwrap_or_default();

        let matched: Vec<PolicyEntry> = entries
            .iter()
            .filter(|entry| entry.is_match_resource(resource))
            .filter(|entry| entry.is_match_action(actions))
            .filter(|entry| entry.is_match_environment(&environment))
            .cloned()
            .collect();

        if matched.is_empty() {
            None
        } else {
            Some(matched)
        }
    }

    /// Compare two policy entries for priority sorting.
    ///
    /// Priority rules (in order):
    /// 1. Resource Type: Specific (Topic, Group, Cluster) > ANY
    /// 2. Resource Pattern: LITERAL > PREFIXED > ANY
    /// 3. For PREFIXED: Longer prefix > Shorter prefix
    /// 4. Decision: DENY > ALLOW
    ///
    /// Returns:
    /// - `Ordering::Less` if o1 has higher priority
    /// - `Ordering::Greater` if o2 has higher priority
    /// - `Ordering::Equal` if same priority
    fn compare_policy_entries(&self, o1: &PolicyEntry, o2: &PolicyEntry) -> Ordering {
        let r1 = o1.resource();
        let r2 = o2.resource();

        // Step 1: Compare resource types
        if r1.resource_type != r2.resource_type {
            // ANY has lowest priority
            if r1.resource_type == ResourceType::Any {
                return Ordering::Greater; // o1 has lower priority
            }
            if r2.resource_type == ResourceType::Any {
                return Ordering::Less; // o1 has higher priority
            }
        }

        // Step 2: Compare resource patterns
        if r1.resource_pattern != r2.resource_pattern {
            return match (r1.resource_pattern, r2.resource_pattern) {
                (ResourcePattern::Literal, _) => Ordering::Less, // LITERAL has highest priority
                (_, ResourcePattern::Literal) => Ordering::Greater,
                (ResourcePattern::Prefixed, ResourcePattern::Any) => Ordering::Less,
                (ResourcePattern::Any, ResourcePattern::Prefixed) => Ordering::Greater,
                _ => Ordering::Equal,
            };
        }

        // Step 3: For PREFIXED pattern, compare prefix lengths (longer = higher priority)
        if r1.resource_pattern == ResourcePattern::Prefixed {
            if let (Some(name1), Some(name2)) = (&r1.resource_name, &r2.resource_name) {
                let cmp = name2.len().cmp(&name1.len()); // Reverse: longer name has higher priority
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
        }

        // Step 4: Compare decisions (DENY > ALLOW)
        let d1 = o1.decision();
        let d2 = o2.decision();
        if d1 != d2 {
            return if d1 == Decision::Deny {
                Ordering::Less // o1 (DENY) has higher priority
            } else {
                Ordering::Greater // o2 (DENY) has higher priority
            };
        }

        Ordering::Equal
    }

    /// Create an authorization error with context details.
    fn create_error(&self, context: &DefaultAuthorizationContext, detail: &str) -> RocketMQError {
        let subject_key = context.subject().as_ref().map(|s| s.subject_key()).unwrap_or("unknown");
        let resource_key = context
            .resource()
            .as_ref()
            .and_then(|r| r.resource_key())
            .unwrap_or_else(|| "unknown".to_string());
        let source_ip_binding = context.source_ip();
        let source_ip = source_ip_binding.unwrap_or("unknown");

        RocketMQError::authentication_failed(format!(
            "{} has no permission to access {} from {}, {}",
            subject_key, resource_key, source_ip, detail
        ))
    }
}

impl<P: AuthorizationMetadataProvider + 'static> AuthorizationHandler for AclAuthorizationHandler<P> {
    fn handle<'a>(
        &'a self,
        context: &'a DefaultAuthorizationContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), RocketMQError>> + Send + 'a>> {
        Box::pin(async move {
            // Step 1: Extract subject from context
            let subject_binding = context.subject();
            let subject_wrapper = subject_binding
                .as_ref()
                .ok_or_else(|| RocketMQError::authentication_failed("Subject not found in authorization context"))?;

            // Create a User subject for ACL lookup (required by metadata provider trait)
            let subject = User::of(subject_wrapper.subject_key());

            // Step 2: Fetch ACL from metadata provider
            let acl = self
                .metadata_provider
                .get_acl(&subject)
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to fetch ACL: {}", e)))?
                .ok_or_else(|| self.create_error(context, "no matched policies"))?;

            // Step 3: Match policy entries
            let matched_entry = self
                .match_policy_entries(context, &acl)
                .await
                .ok_or_else(|| self.create_error(context, "no matched policies"))?;

            // Step 4: Check decision
            if matched_entry.decision() == Decision::Deny {
                return Err(self.create_error(context, "the decision is deny"));
            }

            // Step 5: Authorization granted
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocketmq_common::common::action::Action;
    use rocketmq_common::common::resource::resource_pattern::ResourcePattern;
    use rocketmq_common::common::resource::resource_type::ResourceType;

    use super::*;
    use crate::authentication::enums::subject_type::SubjectType;
    use crate::authentication::model::user::User;
    use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
    use crate::authorization::metadata_provider::local::LocalAuthorizationMetadataProvider;
    use crate::authorization::model::policy::Policy;
    use crate::authorization::model::resource::Resource;
    use crate::config::AuthConfig;

    #[tokio::test]
    async fn test_acl_authorization_handler_allow() {
        // Create a local metadata provider
        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).unwrap();
        let provider = Arc::new(provider);

        // Create ACL for user "alice" with ALLOW on topic "test-topic"
        let user = User::of("alice");
        let resource = Resource::of_topic("test-topic");
        let policy = Policy::of(vec![resource.clone()], vec![Action::Pub], None, Decision::Allow);
        let acl = Acl::of_subject_and_policy(&user, policy);

        // Store ACL
        provider.create_acl(acl).await.unwrap();

        // Create handler
        let handler = AclAuthorizationHandler::new(provider);

        // Create authorization context
        let context = DefaultAuthorizationContext::of("alice", SubjectType::User, resource, Action::Pub, "127.0.0.1");

        // Test authorization - should succeed
        let result = handler.handle(&context).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_acl_authorization_handler_deny() {
        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).unwrap();
        let provider = Arc::new(provider);

        // Create ACL with DENY decision
        let user = User::of("bob");
        let resource = Resource::of_topic("forbidden-topic");
        let policy = Policy::of(vec![resource.clone()], vec![Action::Pub], None, Decision::Deny);
        let acl = Acl::of_subject_and_policy(&user, policy);

        provider.create_acl(acl).await.unwrap();

        let handler = AclAuthorizationHandler::new(provider);

        let context = DefaultAuthorizationContext::of("bob", SubjectType::User, resource, Action::Pub, "192.168.1.1");

        // Test authorization - should fail with DENY
        let result = handler.handle(&context).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_acl_authorization_handler_no_acl() {
        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).unwrap();
        let provider = Arc::new(provider);
        let handler = AclAuthorizationHandler::new(provider);

        let resource = Resource::of_topic("test-topic");
        let context = DefaultAuthorizationContext::of("charlie", SubjectType::User, resource, Action::Pub, "10.0.0.1");

        // No ACL exists for "charlie" - should fail
        let result = handler.handle(&context).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_acl_authorization_handler_no_matching_policy() {
        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).unwrap();
        let provider = Arc::new(provider);

        // Create ACL for topic "topic-a" but request accesses "topic-b"
        let user = User::of("dave");
        let allowed_resource = Resource::of_topic("topic-a");
        let policy = Policy::of(vec![allowed_resource], vec![Action::Pub], None, Decision::Allow);
        let acl = Acl::of_subject_and_policy(&user, policy);

        provider.create_acl(acl).await.unwrap();

        let handler = AclAuthorizationHandler::new(provider);

        let requested_resource = Resource::of_topic("topic-b");
        let context =
            DefaultAuthorizationContext::of("dave", SubjectType::User, requested_resource, Action::Pub, "172.16.0.1");

        // Should fail - no matching policy
        let result = handler.handle(&context).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_compare_policy_entries_resource_type() {
        let provider = Arc::new(LocalAuthorizationMetadataProvider::new());
        let handler = AclAuthorizationHandler::new(provider);

        let topic_entry = PolicyEntry::of(
            Resource::of(ResourceType::Topic, Some("t1".to_string()), ResourcePattern::Literal),
            vec![Action::Pub],
            None,
            Decision::Allow,
        );

        let any_entry = PolicyEntry::of(
            Resource::of(ResourceType::Any, None, ResourcePattern::Any),
            vec![Action::Pub],
            None,
            Decision::Allow,
        );

        // Topic should have higher priority than ANY
        assert_eq!(handler.compare_policy_entries(&topic_entry, &any_entry), Ordering::Less);
        assert_eq!(
            handler.compare_policy_entries(&any_entry, &topic_entry),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_policy_entries_resource_pattern() {
        let provider = Arc::new(LocalAuthorizationMetadataProvider::new());
        let handler = AclAuthorizationHandler::new(provider);

        let literal = PolicyEntry::of(
            Resource::of(ResourceType::Topic, Some("test".to_string()), ResourcePattern::Literal),
            vec![Action::Pub],
            None,
            Decision::Allow,
        );

        let prefixed = PolicyEntry::of(
            Resource::of(ResourceType::Topic, Some("test".to_string()), ResourcePattern::Prefixed),
            vec![Action::Pub],
            None,
            Decision::Allow,
        );

        // LITERAL > PREFIXED
        assert_eq!(handler.compare_policy_entries(&literal, &prefixed), Ordering::Less);
    }

    #[test]
    fn test_compare_policy_entries_decision() {
        let provider = Arc::new(LocalAuthorizationMetadataProvider::new());
        let handler = AclAuthorizationHandler::new(provider);

        let deny = PolicyEntry::of(
            Resource::of(ResourceType::Topic, Some("t1".to_string()), ResourcePattern::Literal),
            vec![Action::Pub],
            None,
            Decision::Deny,
        );

        let allow = PolicyEntry::of(
            Resource::of(ResourceType::Topic, Some("t1".to_string()), ResourcePattern::Literal),
            vec![Action::Pub],
            None,
            Decision::Allow,
        );

        // DENY > ALLOW
        assert_eq!(handler.compare_policy_entries(&deny, &allow), Ordering::Less);
    }
}
