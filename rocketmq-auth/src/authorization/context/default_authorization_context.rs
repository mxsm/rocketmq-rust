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

//! Default authorization context implementation.
//!
//! This module provides the standard authorization context used throughout RocketMQ.

use std::collections::HashMap;
use std::fmt;

use rocketmq_common::common::action::Action;

use crate::authentication::enums::subject_type::SubjectType;
use crate::authorization::model::resource::Resource;

/// A type-erased subject wrapper that implements Debug and Clone.
pub struct SubjectWrapper {
    subject_key: String,
    subject_type: SubjectType,
}

impl SubjectWrapper {
    pub fn new(subject_key: String, subject_type: SubjectType) -> Self {
        Self {
            subject_key,
            subject_type,
        }
    }

    pub fn subject_key(&self) -> &str {
        &self.subject_key
    }

    pub fn subject_type(&self) -> SubjectType {
        self.subject_type
    }
}

impl fmt::Debug for SubjectWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubjectWrapper")
            .field("subject_key", &self.subject_key)
            .field("subject_type", &self.subject_type)
            .finish()
    }
}

impl Clone for SubjectWrapper {
    fn clone(&self) -> Self {
        Self {
            subject_key: self.subject_key.clone(),
            subject_type: self.subject_type,
        }
    }
}

/// Default authorization context containing all information needed for authorization decisions.
///
/// This struct contains:
/// - Subject: who is performing the action (user, role, service account)
/// - Resource: what is being accessed (topic, group, cluster)
/// - Actions: what operations are being performed (PUB, SUB, CREATE, etc.)
/// - Source IP: where the request originates from
/// - Channel ID: unique identifier for the communication channel
/// - RPC code: the RocketMQ request code
/// - Extended info: additional context-specific metadata
#[derive(Debug, Clone, Default)]
pub struct DefaultAuthorizationContext {
    /// The subject performing the operation (user, role, etc.)
    subject: Option<SubjectWrapper>,

    /// The resource being accessed (topic, group, etc.)
    resource: Option<Resource>,

    /// List of actions being performed on the resource
    actions: Vec<Action>,

    /// Source IP address of the request
    source_ip: Option<String>,

    /// Unique identifier for the communication channel (inherited from base context)
    channel_id: Option<String>,

    /// RocketMQ RPC request code (for protocol-specific authorization)
    rpc_code: Option<String>,

    /// Extended information for custom authorization logic
    ext_info: HashMap<String, String>,
}

impl DefaultAuthorizationContext {
    /// Create a new authorization context with basic information.
    ///
    /// # Arguments
    /// * `subject_key` - The subject key performing the operation
    /// * `subject_type` - The type of the subject
    /// * `resource` - The resource being accessed
    /// * `action` - The action being performed
    /// * `source_ip` - Source IP address of the request
    pub fn of(
        subject_key: impl Into<String>,
        subject_type: SubjectType,
        resource: Resource,
        action: Action,
        source_ip: impl Into<String>,
    ) -> Self {
        Self {
            subject: Some(SubjectWrapper::new(subject_key.into(), subject_type)),
            resource: Some(resource),
            actions: vec![action],
            source_ip: Some(source_ip.into()),
            channel_id: None,
            rpc_code: None,
            ext_info: HashMap::new(),
        }
    }

    /// Create a new authorization context with multiple actions.
    ///
    /// # Arguments
    /// * `subject_key` - The subject key performing the operation
    /// * `subject_type` - The type of the subject
    /// * `resource` - The resource being accessed
    /// * `actions` - List of actions being performed
    /// * `source_ip` - Source IP address of the request
    pub fn of_multi_actions(
        subject_key: impl Into<String>,
        subject_type: SubjectType,
        resource: Resource,
        actions: Vec<Action>,
        source_ip: impl Into<String>,
    ) -> Self {
        Self {
            subject: Some(SubjectWrapper::new(subject_key.into(), subject_type)),
            resource: Some(resource),
            actions,
            source_ip: Some(source_ip.into()),
            channel_id: None,
            rpc_code: None,
            ext_info: HashMap::new(),
        }
    }

    /// Create a builder for constructing authorization contexts.
    pub fn builder() -> DefaultAuthorizationContextBuilder {
        DefaultAuthorizationContextBuilder::default()
    }

    /// Get the subject key (unique identifier for the subject).
    pub fn subject_key(&self) -> Option<&str> {
        self.subject.as_ref().map(|s| s.subject_key())
    }

    /// Get the subject type.
    pub fn subject_type(&self) -> Option<SubjectType> {
        self.subject.as_ref().map(|s| s.subject_type())
    }

    /// Get the resource key (unique identifier for the resource).
    pub fn resource_key(&self) -> Option<String> {
        self.resource.as_ref().and_then(|r| r.resource_key())
    }

    // Getters
    pub fn subject(&self) -> Option<&SubjectWrapper> {
        self.subject.as_ref()
    }

    pub fn resource(&self) -> Option<&Resource> {
        self.resource.as_ref()
    }

    pub fn actions(&self) -> &[Action] {
        &self.actions
    }

    pub fn source_ip(&self) -> Option<&str> {
        self.source_ip.as_deref()
    }

    pub fn channel_id(&self) -> Option<&str> {
        self.channel_id.as_deref()
    }

    pub fn rpc_code(&self) -> Option<&str> {
        self.rpc_code.as_deref()
    }

    pub fn ext_info(&self) -> &HashMap<String, String> {
        &self.ext_info
    }

    // Setters
    pub fn set_subject(&mut self, subject_key: impl Into<String>, subject_type: SubjectType) {
        self.subject = Some(SubjectWrapper::new(subject_key.into(), subject_type));
    }

    pub fn set_resource(&mut self, resource: Resource) {
        self.resource = Some(resource);
    }

    pub fn set_actions(&mut self, actions: Vec<Action>) {
        self.actions = actions;
    }

    pub fn set_source_ip(&mut self, source_ip: impl Into<String>) {
        self.source_ip = Some(source_ip.into());
    }

    pub fn set_channel_id(&mut self, channel_id: impl Into<String>) {
        self.channel_id = Some(channel_id.into());
    }

    pub fn set_rpc_code(&mut self, rpc_code: impl Into<String>) {
        self.rpc_code = Some(rpc_code.into());
    }

    pub fn set_ext_info(&mut self, ext_info: HashMap<String, String>) {
        self.ext_info = ext_info;
    }

    pub fn add_ext_info(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.ext_info.insert(key.into(), value.into());
    }
}

/// Builder for constructing `DefaultAuthorizationContext` instances.
#[derive(Default)]
pub struct DefaultAuthorizationContextBuilder {
    subject: Option<SubjectWrapper>,
    resource: Option<Resource>,
    actions: Vec<Action>,
    source_ip: Option<String>,
    channel_id: Option<String>,
    rpc_code: Option<String>,
    ext_info: HashMap<String, String>,
}

impl DefaultAuthorizationContextBuilder {
    pub fn subject(mut self, subject_key: impl Into<String>, subject_type: SubjectType) -> Self {
        self.subject = Some(SubjectWrapper::new(subject_key.into(), subject_type));
        self
    }

    pub fn resource(mut self, resource: Resource) -> Self {
        self.resource = Some(resource);
        self
    }

    pub fn action(mut self, action: Action) -> Self {
        self.actions.push(action);
        self
    }

    pub fn actions(mut self, actions: Vec<Action>) -> Self {
        self.actions = actions;
        self
    }

    pub fn source_ip(mut self, source_ip: impl Into<String>) -> Self {
        self.source_ip = Some(source_ip.into());
        self
    }

    pub fn channel_id(mut self, channel_id: impl Into<String>) -> Self {
        self.channel_id = Some(channel_id.into());
        self
    }

    pub fn rpc_code(mut self, rpc_code: impl Into<String>) -> Self {
        self.rpc_code = Some(rpc_code.into());
        self
    }

    pub fn ext_info(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.ext_info.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> DefaultAuthorizationContext {
        DefaultAuthorizationContext {
            subject: self.subject,
            resource: self.resource,
            actions: self.actions,
            source_ip: self.source_ip,
            channel_id: self.channel_id,
            rpc_code: self.rpc_code,
            ext_info: self.ext_info,
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::action::Action;

    use super::*;

    #[test]
    fn test_context_creation_with_of() {
        let subject_key = "user:alice";
        let subject_type = SubjectType::User;
        let resource = Resource::of_topic("test-topic");
        let context = DefaultAuthorizationContext::of(subject_key, subject_type, resource, Action::Pub, "192.168.1.1");

        assert_eq!(context.subject_key(), Some("user:alice"));
        assert_eq!(context.subject_type(), Some(SubjectType::User));
        assert_eq!(context.actions().len(), 1);
        assert_eq!(context.actions()[0], Action::Pub);
        assert_eq!(context.source_ip(), Some("192.168.1.1"));
    }

    #[test]
    fn test_context_creation_with_multi_actions() {
        let subject_key = "user:bob";
        let subject_type = SubjectType::User;
        let resource = Resource::of_topic("test-topic");
        let actions = vec![Action::Pub, Action::Sub];
        let context =
            DefaultAuthorizationContext::of_multi_actions(subject_key, subject_type, resource, actions, "10.0.0.1");

        assert_eq!(context.actions().len(), 2);
        assert!(context.actions().contains(&Action::Pub));
        assert!(context.actions().contains(&Action::Sub));
    }

    #[test]
    fn test_context_builder() {
        let subject_key = "user:charlie";
        let subject_type = SubjectType::User;
        let resource = Resource::of_group("test-group".to_string());

        let context = DefaultAuthorizationContext::builder()
            .subject(subject_key, subject_type)
            .resource(resource)
            .action(Action::Create)
            .source_ip("172.16.0.1")
            .rpc_code("310")
            .ext_info("region", "us-west")
            .build();

        assert_eq!(context.subject_key(), Some("user:charlie"));
        assert_eq!(context.rpc_code(), Some("310"));
        assert_eq!(context.ext_info().get("region"), Some(&"us-west".to_string()));
    }

    #[test]
    fn test_context_setters() {
        let mut context = DefaultAuthorizationContext::default();

        context.set_subject("user:dave", SubjectType::User);
        context.set_resource(Resource::of_topic("new-topic"));
        context.set_actions(vec![Action::Delete]);
        context.set_source_ip("203.0.113.1");
        context.set_rpc_code("500");
        context.add_ext_info("environment", "production");

        assert_eq!(context.subject_key(), Some("user:dave"));
        assert_eq!(context.actions().len(), 1);
        assert_eq!(context.source_ip(), Some("203.0.113.1"));
        assert_eq!(context.rpc_code(), Some("500"));
        assert_eq!(context.ext_info().get("environment"), Some(&"production".to_string()));
    }

    #[test]
    fn test_resource_key_generation() {
        let subject_key = "user:eve";
        let subject_type = SubjectType::User;
        let resource = Resource::of_topic("my-topic");
        let context = DefaultAuthorizationContext::of(subject_key, subject_type, resource, Action::Get, "10.10.10.10");

        let resource_key = context.resource_key();
        assert!(resource_key.is_some());
        let key = resource_key.unwrap();
        assert!(key.contains("Topic"));
        assert!(key.contains("my-topic"));
    }

    #[test]
    fn test_subject_wrapper() {
        let wrapper = SubjectWrapper::new("user:test".to_string(), SubjectType::User);
        assert_eq!(wrapper.subject_key(), "user:test");
        assert_eq!(wrapper.subject_type(), SubjectType::User);

        // Test clone
        let cloned = wrapper.clone();
        assert_eq!(cloned.subject_key(), wrapper.subject_key());
        assert_eq!(cloned.subject_type(), wrapper.subject_type());

        // Test debug
        let debug_str = format!("{:?}", wrapper);
        assert!(debug_str.contains("user:test"));
    }
}
