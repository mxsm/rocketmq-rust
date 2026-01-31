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
