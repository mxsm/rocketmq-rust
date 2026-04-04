use std::sync::Arc;

use rocketmq_auth::authentication::enums::subject_type::SubjectType;
use rocketmq_auth::authentication::enums::user_type::UserType;
use rocketmq_auth::authentication::model::subject::Subject;
use rocketmq_auth::authentication::model::user::User;
use rocketmq_auth::authentication::provider::AuthenticationMetadataProvider;
use rocketmq_auth::authentication::provider::LocalAuthenticationMetadataProvider;
use rocketmq_auth::authorization::metadata_provider::AuthorizationMetadataProvider;
use rocketmq_auth::authorization::metadata_provider::LocalAuthorizationMetadataProvider;
use rocketmq_auth::authorization::model::acl::Acl;
use rocketmq_auth::authorization::model::resource::Resource;
use rocketmq_auth::config::AuthConfig;
use rocketmq_auth::ProviderRegistry;
use rocketmq_common::common::action::Action;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::protocol::body::user_info::UserInfo;

use crate::auth::acl_converter::AclConverter;
use crate::auth::user_converter::UserConverter;

#[derive(Clone)]
pub struct AuthAdminService {
    authentication_provider: Arc<LocalAuthenticationMetadataProvider>,
    authorization_provider: Arc<LocalAuthorizationMetadataProvider>,
}

impl AuthAdminService {
    pub fn new(auth_config: AuthConfig) -> Result<Self, RocketMQError> {
        let provider_registry = ProviderRegistry::local(&auth_config)?;
        Ok(Self::with_provider_registry(provider_registry))
    }

    pub fn with_provider_registry(provider_registry: ProviderRegistry) -> Self {
        Self {
            authentication_provider: provider_registry.authentication_metadata_provider(),
            authorization_provider: provider_registry.authorization_metadata_provider(),
        }
    }

    pub fn authentication_provider(&self) -> Arc<LocalAuthenticationMetadataProvider> {
        self.authentication_provider.clone()
    }

    pub fn authorization_provider(&self) -> Arc<LocalAuthorizationMetadataProvider> {
        self.authorization_provider.clone()
    }

    pub async fn create_user(&self, user: User) -> RocketMQResult<()> {
        self.validate_username(user.username().as_str())?;

        if self
            .authentication_provider
            .get_user(user.username().as_str())
            .await
            .is_ok()
        {
            return Err(RocketMQError::IllegalArgument(format!(
                "User '{}' already exists",
                user.username()
            )));
        }

        self.authentication_provider.create_user(user).await
    }

    pub async fn update_user(&self, user: User) -> RocketMQResult<()> {
        self.validate_username(user.username().as_str())?;
        self.get_existing_user(user.username().as_str()).await?;
        self.authentication_provider.update_user(user).await
    }

    pub async fn delete_user(&self, username: &str) -> RocketMQResult<()> {
        self.validate_username(username)?;
        self.authentication_provider.delete_user(username).await?;

        let subject = SubjectRef::parse(username)?;
        self.authorization_provider
            .delete_acl(&subject)
            .await
            .map_err(map_authz_error)
    }

    pub async fn get_user(&self, username: &str) -> RocketMQResult<Option<UserInfo>> {
        self.validate_username(username)?;

        match self.authentication_provider.get_user(username).await {
            Ok(user) => Ok(Some(UserConverter::convert_user_info(&user))),
            Err(RocketMQError::Authentication(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn list_users(&self, filter: Option<&str>) -> RocketMQResult<Vec<UserInfo>> {
        let users = self.authentication_provider.list_user(filter).await?;
        Ok(users.iter().map(UserConverter::convert_user_info).collect())
    }

    pub async fn list_acls(
        &self,
        subject_filter: Option<&str>,
        resource_filter: Option<&str>,
    ) -> RocketMQResult<Vec<AclInfo>> {
        let acls = self
            .authorization_provider
            .list_acl(subject_filter, resource_filter)
            .await
            .map_err(map_authz_error)?;
        Ok(acls.iter().map(AclConverter::convert_acl).collect())
    }

    pub async fn create_acl(&self, acl: Acl) -> RocketMQResult<()> {
        self.upsert_acl(acl).await
    }

    pub async fn update_acl(&self, acl: Acl) -> RocketMQResult<()> {
        self.upsert_acl(acl).await
    }

    pub async fn get_acl(&self, subject: &str) -> RocketMQResult<Option<AclInfo>> {
        let subject = SubjectRef::parse(subject)?;
        self.ensure_subject_exists(&subject).await?;

        let acl = self
            .authorization_provider
            .get_acl(&subject)
            .await
            .map_err(map_authz_error)?;
        Ok(acl.as_ref().map(AclConverter::convert_acl))
    }

    pub async fn delete_acl(&self, subject: &str, resource: Option<&str>) -> RocketMQResult<()> {
        let subject = SubjectRef::parse(subject)?;
        let Some(resource_key) = resource.filter(|resource| !resource.trim().is_empty()) else {
            return self
                .authorization_provider
                .delete_acl(&subject)
                .await
                .map_err(map_authz_error);
        };

        let resource = Resource::of_str(resource_key)
            .ok_or_else(|| RocketMQError::illegal_argument(format!("Invalid resource '{resource_key}'")))?;

        let Some(mut acl) = self
            .authorization_provider
            .get_acl(&subject)
            .await
            .map_err(map_authz_error)?
        else {
            return Ok(());
        };

        let mut retained_policies = Vec::new();
        for mut policy in acl.policies().clone() {
            policy.delete_entry(&resource);
            if !policy.entries().is_empty() {
                retained_policies.push(policy);
            }
        }

        if retained_policies.is_empty() {
            self.authorization_provider
                .delete_acl(&subject)
                .await
                .map_err(map_authz_error)?;
        } else {
            acl.set_policies(retained_policies);
            self.authorization_provider
                .update_acl(acl)
                .await
                .map_err(map_authz_error)?;
        }

        Ok(())
    }

    pub async fn is_super_user(&self, username: &str) -> RocketMQResult<bool> {
        let Some(user_info) = self.get_user(username).await? else {
            return Ok(false);
        };
        Ok(user_info
            .user_type
            .as_deref()
            .and_then(UserType::get_by_name)
            .is_some_and(|user_type| user_type == UserType::Super))
    }

    async fn get_existing_user(&self, username: &str) -> RocketMQResult<User> {
        self.authentication_provider.get_user(username).await
    }

    fn validate_username(&self, username: &str) -> RocketMQResult<()> {
        if username.trim().is_empty() {
            return Err(RocketMQError::illegal_argument("The username is blank"));
        }
        Ok(())
    }

    async fn upsert_acl(&self, acl: Acl) -> RocketMQResult<()> {
        validate_acl(&acl)?;
        let subject = SubjectRef::parse(acl.subject_key())?;
        self.ensure_subject_exists(&subject).await?;

        let existing_acl = self
            .authorization_provider
            .get_acl(&subject)
            .await
            .map_err(map_authz_error)?;
        match existing_acl {
            Some(mut existing_acl) => {
                existing_acl.update_policies(acl.policies().clone());
                self.authorization_provider
                    .update_acl(existing_acl)
                    .await
                    .map_err(map_authz_error)
            }
            None => self
                .authorization_provider
                .create_acl(acl)
                .await
                .map_err(map_authz_error),
        }
    }

    async fn ensure_subject_exists(&self, subject: &SubjectRef) -> RocketMQResult<()> {
        match subject.subject_type() {
            SubjectType::User => {
                let username = subject.name();
                if self.authentication_provider.get_user(username).await.is_err() {
                    return Err(RocketMQError::illegal_argument(format!(
                        "The subject of {} is not exist.",
                        subject.subject_key()
                    )));
                }
                Ok(())
            }
        }
    }
}

fn map_authz_error(error: rocketmq_auth::authorization::provider::AuthorizationError) -> RocketMQError {
    RocketMQError::Internal(error.to_string())
}

#[derive(Clone)]
struct SubjectRef {
    subject_key: String,
    subject_name: String,
    subject_type: SubjectType,
}

impl SubjectRef {
    fn parse(subject: &str) -> RocketMQResult<Self> {
        let trimmed = subject.trim();
        if trimmed.is_empty() {
            return Err(RocketMQError::illegal_argument("The subject is blank"));
        }

        let (subject_type, subject_name) = match trimmed.split_once(':') {
            Some((subject_type, subject_name)) => (
                SubjectType::get_by_name(subject_type).ok_or_else(|| {
                    RocketMQError::illegal_argument(format!("Unsupported subject type '{subject_type}'"))
                })?,
                subject_name.trim(),
            ),
            None => (SubjectType::User, trimmed),
        };

        if subject_name.is_empty() {
            return Err(RocketMQError::illegal_argument("The subject name is blank"));
        }

        Ok(Self {
            subject_key: format!("{}:{}", subject_type.name(), subject_name),
            subject_name: subject_name.to_string(),
            subject_type,
        })
    }

    fn name(&self) -> &str {
        &self.subject_name
    }
}

impl Subject for SubjectRef {
    fn subject_key(&self) -> &str {
        &self.subject_key
    }

    fn subject_type(&self) -> SubjectType {
        self.subject_type
    }
}

fn validate_acl(acl: &Acl) -> RocketMQResult<()> {
    if acl.policies().is_empty() {
        return Err(RocketMQError::illegal_argument("The policies is empty."));
    }

    for policy in acl.policies() {
        if policy.entries().is_empty() {
            return Err(RocketMQError::illegal_argument("The policy entries is empty."));
        }

        for entry in policy.entries() {
            if entry.resource().resource_key().is_none() {
                return Err(RocketMQError::illegal_argument("The resource is null."));
            }
            if entry.actions().is_empty() {
                return Err(RocketMQError::illegal_argument("The actions is empty."));
            }
            if entry.actions().contains(&Action::Any) {
                return Err(RocketMQError::illegal_argument("The actions can not be Any."));
            }
            if let Some(environment) = entry.environment() {
                if environment
                    .source_ips()
                    .iter()
                    .any(|source_ip| source_ip.trim().is_empty())
                {
                    return Err(RocketMQError::illegal_argument("The source ip is empty."));
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use cheetah_string::CheetahString;
    use rocketmq_auth::authentication::enums::user_status::UserStatus;
    use rocketmq_auth::authorization::enums::decision::Decision;
    use rocketmq_auth::authorization::model::acl::Acl;
    use rocketmq_auth::authorization::model::policy::Policy;
    use rocketmq_common::common::action::Action;

    use super::*;

    fn test_auth_config() -> AuthConfig {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time should move forward")
            .as_millis();
        let path = std::env::temp_dir().join(format!("rocketmq-rust-auth-admin-service-{millis}.json"));
        AuthConfig {
            auth_config_path: CheetahString::from_string(path.to_string_lossy().into_owned()),
            ..AuthConfig::default()
        }
    }

    #[tokio::test]
    async fn create_get_list_update_delete_user_round_trip() {
        let service = AuthAdminService::new(test_auth_config()).unwrap();

        let mut user = User::of_with_type("alice", "secret", UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        service.create_user(user.clone()).await.unwrap();

        assert!(service.create_user(user.clone()).await.is_err());

        let fetched = service.get_user("alice").await.unwrap().unwrap();
        assert_eq!(fetched.username, Some(CheetahString::from_static_str("alice")));

        let listed = service.list_users(Some("ali")).await.unwrap();
        assert_eq!(listed.len(), 1);

        user.set_password("updated");
        service.update_user(user).await.unwrap();
        service.delete_user("alice").await.unwrap();
        assert!(service.get_user("alice").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_acl_can_remove_single_resource() {
        let service = AuthAdminService::new(test_auth_config()).unwrap();
        let subject = SubjectRef::parse("alice").unwrap();
        let first = Resource::of_topic("topic-a");
        let second = Resource::of_topic("topic-b");
        let acl = Acl::of(
            "alice",
            SubjectType::User,
            Policy::of(
                vec![first.clone(), second.clone()],
                vec![Action::Pub],
                None,
                Decision::Allow,
            ),
        );

        service
            .authorization_provider()
            .create_acl(acl)
            .await
            .map_err(map_authz_error)
            .unwrap();

        service.delete_acl("User:alice", Some("Topic:topic-a")).await.unwrap();

        let acl = service
            .authorization_provider()
            .get_acl(&subject)
            .await
            .map_err(map_authz_error)
            .unwrap()
            .unwrap();
        assert_eq!(acl.policies()[0].entries().len(), 1);
        assert_eq!(
            acl.policies()[0].entries()[0].resource().resource_key().as_deref(),
            Some("Topic:topic-b")
        );
    }

    #[tokio::test]
    async fn list_acl_reads_live_provider_state() {
        let service = AuthAdminService::new(test_auth_config()).unwrap();
        let acl = Acl::of(
            "alice",
            SubjectType::User,
            Policy::of(
                vec![Resource::of_topic("topic-a")],
                vec![Action::Pub],
                None,
                Decision::Allow,
            ),
        );
        service
            .authorization_provider()
            .create_acl(acl)
            .await
            .map_err(map_authz_error)
            .unwrap();

        let listed = service
            .list_acls(Some("User:alice"), Some("Topic:topic-a"))
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].subject, Some(CheetahString::from_static_str("User:alice")));
    }

    #[tokio::test]
    async fn create_update_and_get_acl_round_trip() {
        let service = AuthAdminService::new(test_auth_config()).unwrap();
        let mut user = User::of_with_type("alice", "secret", UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        service.create_user(user).await.unwrap();

        service
            .create_acl(Acl::of(
                "alice",
                SubjectType::User,
                Policy::of(
                    vec![Resource::of_topic("topic-a")],
                    vec![Action::Pub],
                    None,
                    Decision::Allow,
                ),
            ))
            .await
            .unwrap();
        service
            .update_acl(Acl::of(
                "alice",
                SubjectType::User,
                Policy::of(
                    vec![Resource::of_topic("topic-b")],
                    vec![Action::Sub],
                    None,
                    Decision::Allow,
                ),
            ))
            .await
            .unwrap();

        let acl = service.get_acl("User:alice").await.unwrap().unwrap();
        assert_eq!(acl.subject, Some(CheetahString::from_static_str("User:alice")));
        let policies = acl.policies.expect("policies should exist");
        assert_eq!(policies.len(), 1);
        let entries = policies[0].entries.as_ref().expect("entries should exist");
        assert_eq!(entries.len(), 2);
    }
}
