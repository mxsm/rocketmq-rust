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
use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::authentication::enums::subject_type::SubjectType;
use crate::authentication::enums::user_status::UserStatus;
use crate::authentication::enums::user_type::UserType;
use crate::authentication::model::subject::Subject;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    username: CheetahString,
    password: Option<CheetahString>,
    #[serde(rename = "userType")]
    user_type: Option<UserType>,
    #[serde(rename = "userStatus")]
    user_status: Option<UserStatus>,
}

impl User {
    pub fn of(username: impl Into<CheetahString>) -> Self {
        User {
            username: username.into(),
            password: None,
            user_type: None,
            user_status: None,
        }
    }

    pub fn of_with_password(username: impl Into<CheetahString>, password: impl Into<CheetahString>) -> Self {
        User {
            username: username.into(),
            password: Some(password.into()),
            user_type: None,
            user_status: None,
        }
    }

    pub fn of_with_type(
        username: impl Into<CheetahString>,
        password: impl Into<CheetahString>,
        user_type: UserType,
    ) -> Self {
        User {
            username: username.into(),
            password: Some(password.into()),
            user_type: Some(user_type),
            user_status: None,
        }
    }
}

impl User {
    pub fn username(&self) -> &CheetahString {
        &self.username
    }

    pub fn password(&self) -> Option<&CheetahString> {
        self.password.as_ref()
    }

    pub fn user_type(&self) -> Option<UserType> {
        self.user_type
    }

    pub fn user_status(&self) -> Option<UserStatus> {
        self.user_status
    }

    pub fn set_user_status(&mut self, status: UserStatus) {
        self.user_status = Some(status);
    }

    pub fn set_password(&mut self, password: impl Into<CheetahString>) {
        self.password = Some(password.into());
    }

    pub fn set_user_type(&mut self, user_type: UserType) {
        self.user_type = Some(user_type);
    }
}

impl Subject for User {
    fn subject_key(&self) -> &str {
        self.username.as_str()
    }

    fn subject_type(&self) -> SubjectType {
        SubjectType::User
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_of() {
        let username = "test_user";
        let user = User::of(username);
        assert_eq!(user.username(), &CheetahString::from(username));
        assert!(user.password().is_none());
        assert!(user.user_type().is_none());
        assert!(user.user_status().is_none());
    }

    #[test]
    fn test_user_of_with_password() {
        let username = "test_user";
        let password = "test_password";
        let user = User::of_with_password(username, password);
        assert_eq!(user.username(), &CheetahString::from(username));
        assert_eq!(user.password(), Some(&CheetahString::from(password)));
        assert!(user.user_type().is_none());
        assert!(user.user_status().is_none());
    }

    #[test]
    fn test_user_of_with_type() {
        let username = "test_user";
        let password = "test_password";
        let user_type = UserType::Normal;
        let user = User::of_with_type(username, password, user_type);
        assert_eq!(user.username(), &CheetahString::from(username));
        assert_eq!(user.password(), Some(&CheetahString::from(password)));
        assert_eq!(user.user_type(), Some(user_type));
        assert!(user.user_status().is_none());
    }

    #[test]
    fn test_user_set_user_status() {
        let mut user = User::of("test_user");
        user.set_user_status(UserStatus::Enable);
        assert_eq!(user.user_status(), Some(UserStatus::Enable));
    }

    #[test]
    fn test_user_subject_trait() {
        let username = "test_user";
        let user = User::of(username);
        assert_eq!(user.subject_key(), username);
        assert_eq!(user.subject_type(), SubjectType::User);
    }
}
