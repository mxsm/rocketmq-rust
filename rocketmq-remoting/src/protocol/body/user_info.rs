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

use std::fmt::Display;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    pub username: Option<CheetahString>,
    pub password: Option<CheetahString>,
    pub user_type: Option<CheetahString>,
    pub user_status: Option<CheetahString>,
}

impl Display for UserInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UserInfo [username={}, password={}, user_type={}, user_status={}]",
            self.username.as_ref().unwrap_or(&CheetahString::new()),
            self.password.as_ref().unwrap_or(&CheetahString::new()),
            self.user_type.as_ref().unwrap_or(&CheetahString::new()),
            self.user_status.as_ref().unwrap_or(&CheetahString::new())
        )
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn user_info_default_values() {
        let user_info = UserInfo::default();
        assert!(user_info.username.is_none());
        assert!(user_info.password.is_none());
        assert!(user_info.user_type.is_none());
        assert!(user_info.user_status.is_none());
    }

    #[test]
    fn user_info_with_values() {
        let user_info = UserInfo {
            username: Some(CheetahString::from("user")),
            password: Some(CheetahString::from("pass")),
            user_type: Some(CheetahString::from("admin")),
            user_status: Some(CheetahString::from("active")),
        };
        assert_eq!(user_info.username, Some(CheetahString::from("user")));
        assert_eq!(user_info.password, Some(CheetahString::from("pass")));
        assert_eq!(user_info.user_type, Some(CheetahString::from("admin")));
        assert_eq!(user_info.user_status, Some(CheetahString::from("active")));
    }

    #[test]
    fn serialize_user_info() {
        let user_info = UserInfo {
            username: Some(CheetahString::from("user")),
            password: Some(CheetahString::from("pass")),
            user_type: Some(CheetahString::from("admin")),
            user_status: Some(CheetahString::from("active")),
        };
        let serialized = serde_json::to_string(&user_info).unwrap();
        assert!(serialized.contains("\"username\":\"user\""));
        assert!(serialized.contains("\"password\":\"pass\""));
        assert!(serialized.contains("\"userType\":\"admin\""));
        assert!(serialized.contains("\"userStatus\":\"active\""));
    }

    #[test]
    fn deserialize_user_info() {
        let json = r#"{
            "username": "user",
            "password": "pass",
            "userType": "admin",
            "userStatus": "active"
        }"#;
        let deserialized: UserInfo = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.username, Some(CheetahString::from("user")));
        assert_eq!(deserialized.password, Some(CheetahString::from("pass")));
        assert_eq!(deserialized.user_type, Some(CheetahString::from("admin")));
        assert_eq!(deserialized.user_status, Some(CheetahString::from("active")));
    }

    #[test]
    fn deserialize_user_info_missing_optional_fields() {
        let json = r#"{}"#;
        let deserialized: UserInfo = serde_json::from_str(json).unwrap();
        assert!(deserialized.username.is_none());
        assert!(deserialized.password.is_none());
        assert!(deserialized.user_type.is_none());
        assert!(deserialized.user_status.is_none());
    }

    #[test]
    fn display_user_info() {
        let user_info = UserInfo {
            username: Some(CheetahString::from("user")),
            password: Some(CheetahString::from("pass")),
            user_type: Some(CheetahString::from("admin")),
            user_status: Some(CheetahString::from("active")),
        };
        let display = format!("{}", user_info);
        assert_eq!(
            display,
            "UserInfo [username=user, password=pass, user_type=admin, user_status=active]"
        );
    }
}
