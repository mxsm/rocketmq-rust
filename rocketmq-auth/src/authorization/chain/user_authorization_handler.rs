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

use std::sync::Arc;

use rocketmq_error::AuthError;
use rocketmq_error::RocketMQError;
use rocketmq_security_api::AuthorizationDecision;
use rocketmq_security_api::AuthorizationDenial;

use crate::authentication::enums::subject_type::SubjectType;
use crate::authentication::enums::user_status::UserStatus;
use crate::authentication::enums::user_type::UserType;
use crate::authentication::model::user::User;
use crate::authentication::provider::AuthenticationMetadataProvider;
use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::provider::AuthorizationError;
use crate::authorization::provider::AuthorizationResult;

pub struct UserAuthorizationHandler<P: AuthenticationMetadataProvider> {
    authentication_metadata_provider: Arc<P>,
}

impl<P: AuthenticationMetadataProvider> UserAuthorizationHandler<P> {
    pub fn new(authentication_metadata_provider: Arc<P>) -> Self {
        Self {
            authentication_metadata_provider,
        }
    }

    pub async fn authorize_subject(
        &self,
        context: &DefaultAuthorizationContext,
    ) -> AuthorizationResult<Option<AuthorizationDecision>> {
        let subject = match context.subject() {
            Some(subject) => subject,
            None => return Ok(None),
        };

        if subject.subject_type() != SubjectType::User {
            return Ok(None);
        }

        let username = User::username_from_subject_key(subject.subject_key());
        let user = match self.authentication_metadata_provider.get_user(username).await {
            Ok(user) => user,
            Err(RocketMQError::Authentication(AuthError::UserNotFound(_))) => {
                return Ok(Some(AuthorizationDecision::Deny(AuthorizationDenial::SubjectUnknown)));
            }
            Err(source) => {
                return Err(AuthorizationError::ProviderRuntimeFailed {
                    operation: "load authorization subject",
                    source: Box::new(source),
                });
            }
        };

        if user.user_status() == Some(UserStatus::Disable) {
            return Ok(Some(AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied)));
        }

        if user.user_type() == Some(UserType::Super) {
            return Ok(Some(AuthorizationDecision::Allow));
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::provider::LocalAuthenticationMetadataProvider;
    use crate::config::AuthConfig;

    #[tokio::test]
    async fn test_super_user_bypass() {
        let mut metadata_provider = LocalAuthenticationMetadataProvider::new();
        metadata_provider.initialize(AuthConfig::default(), None).await.unwrap();

        let mut user = User::of_with_type("alice", "secret", UserType::Super);
        user.set_user_status(UserStatus::Enable);
        metadata_provider.create_user(user).await.unwrap();

        let handler = UserAuthorizationHandler::new(Arc::new(metadata_provider));
        let mut context = DefaultAuthorizationContext::default();
        context.set_subject("alice", SubjectType::User);

        let decision = handler.authorize_subject(&context).await.unwrap();
        assert_eq!(decision, Some(AuthorizationDecision::Allow));
    }

    #[tokio::test]
    async fn test_disabled_user_is_rejected() {
        let mut metadata_provider = LocalAuthenticationMetadataProvider::new();
        metadata_provider.initialize(AuthConfig::default(), None).await.unwrap();

        let mut user = User::of_with_type("alice", "secret", UserType::Normal);
        user.set_user_status(UserStatus::Disable);
        metadata_provider.create_user(user).await.unwrap();

        let handler = UserAuthorizationHandler::new(Arc::new(metadata_provider));
        let mut context = DefaultAuthorizationContext::default();
        context.set_subject("alice", SubjectType::User);

        let result = handler.authorize_subject(&context).await;
        assert_eq!(
            result.unwrap(),
            Some(AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied))
        );
    }
}
