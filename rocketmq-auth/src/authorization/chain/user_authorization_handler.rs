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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rocketmq_error::RocketMQError;

use super::handler::AuthorizationHandler;
use crate::authentication::enums::subject_type::SubjectType;
use crate::authentication::enums::user_status::UserStatus;
use crate::authentication::enums::user_type::UserType;
use crate::authentication::model::user::User;
use crate::authentication::provider::AuthenticationMetadataProvider;
use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserAuthorizationDecision {
    SuperUser,
    Continue,
}

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
    ) -> Result<UserAuthorizationDecision, RocketMQError> {
        let subject = match context.subject() {
            Some(subject) => subject,
            None => return Ok(UserAuthorizationDecision::Continue),
        };

        if subject.subject_type() != SubjectType::User {
            return Ok(UserAuthorizationDecision::Continue);
        }

        let username = User::username_from_subject_key(subject.subject_key());
        let user = self
            .authentication_metadata_provider
            .get_user(username)
            .await
            .map_err(|error| RocketMQError::authentication_failed(error.to_string()))?;

        if user.user_status() == Some(UserStatus::Disable) {
            return Err(RocketMQError::authentication_failed(format!(
                "User:{} is disabled.",
                user.username()
            )));
        }

        if user.user_type() == Some(UserType::Super) {
            return Ok(UserAuthorizationDecision::SuperUser);
        }

        Ok(UserAuthorizationDecision::Continue)
    }
}

impl<P: AuthenticationMetadataProvider + 'static> AuthorizationHandler for UserAuthorizationHandler<P> {
    fn handle<'a>(
        &'a self,
        context: &'a DefaultAuthorizationContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), RocketMQError>> + Send + 'a>> {
        Box::pin(async move {
            self.authorize_subject(context).await?;
            Ok(())
        })
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
        assert_eq!(decision, UserAuthorizationDecision::SuperUser);
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
        assert!(result.is_err());
    }
}
