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

use rocketmq_auth::authentication::enums::user_status::UserStatus;
use rocketmq_auth::authentication::enums::user_type::UserType;
use rocketmq_auth::authentication::model::user::User;
use rocketmq_remoting::protocol::body::user_info::UserInfo;

pub struct UserConverter {}

impl UserConverter {
    pub fn convert_user(user_info: &UserInfo) -> User {
        let mut user = User::of(user_info.username.clone().unwrap_or_default());

        if let Some(password) = &user_info.password {
            if !password.is_empty() {
                user.set_password(password.clone());
            }
        }

        if let Some(user_type_name) = &user_info.user_type {
            if !user_type_name.is_empty() {
                if let Some(user_type) = UserType::get_by_name(user_type_name) {
                    user.set_user_type(user_type);
                }
            }
        }

        if let Some(user_status_name) = &user_info.user_status {
            if !user_status_name.is_empty() {
                if let Some(user_status) = UserStatus::get_by_name(user_status_name) {
                    user.set_user_status(user_status);
                }
            }
        }

        user
    }
}
