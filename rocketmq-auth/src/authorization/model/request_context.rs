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

use rocketmq_common::common::action::Action;

use crate::authentication::model::subject::Subject;
use crate::authorization::context::default_authorization_context::SubjectWrapper;
use crate::authorization::model::resource::Resource;

#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    subject: Option<SubjectWrapper>,
    resource: Option<Resource>,
    action: Option<Action>,
    source_ip: Option<String>,
}

impl RequestContext {
    pub fn subject(&self) -> Option<&SubjectWrapper> {
        self.subject.as_ref()
    }

    pub fn set_subject<S: Subject>(&mut self, subject: &S) {
        self.subject = Some(SubjectWrapper::new(
            subject.subject_key().to_string(),
            subject.subject_type(),
        ));
    }

    pub fn set_subject_key(
        &mut self,
        subject_key: impl Into<String>,
        subject_type: crate::authentication::enums::subject_type::SubjectType,
    ) {
        self.subject = Some(SubjectWrapper::new(subject_key.into(), subject_type));
    }

    pub fn resource(&self) -> Option<&Resource> {
        self.resource.as_ref()
    }

    pub fn set_resource(&mut self, resource: Resource) {
        self.resource = Some(resource);
    }

    pub fn action(&self) -> Option<Action> {
        self.action
    }

    pub fn set_action(&mut self, action: Action) {
        self.action = Some(action);
    }

    pub fn source_ip(&self) -> Option<&str> {
        self.source_ip.as_deref()
    }

    pub fn set_source_ip(&mut self, source_ip: impl Into<String>) {
        self.source_ip = Some(source_ip.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::model::user::User;

    #[test]
    fn request_context_stores_java_model_fields() {
        let user = User::of("alice");
        let resource = Resource::of_topic("topic-a");
        let mut context = RequestContext::default();

        context.set_subject(&user);
        context.set_resource(resource.clone());
        context.set_action(Action::Pub);
        context.set_source_ip("127.0.0.1");

        assert_eq!(
            context.subject().map(|subject| subject.subject_key()),
            Some("User:alice")
        );
        assert_eq!(context.resource(), Some(&resource));
        assert_eq!(context.action(), Some(Action::Pub));
        assert_eq!(context.source_ip(), Some("127.0.0.1"));
    }
}
