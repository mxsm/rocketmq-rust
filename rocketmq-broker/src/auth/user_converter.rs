use cheetah_string::CheetahString;
use rocketmq_auth::authentication::enums::user_status::UserStatus;
use rocketmq_auth::authentication::enums::user_type::UserType;
use rocketmq_auth::authentication::model::user::User;
use rocketmq_remoting::protocol::body::user_info::UserInfo;

pub struct UserConverter {}

impl UserConverter {
    pub fn convert_user(user_info: &UserInfo) -> User {
        let mut user = User::of(user_info.username.clone().unwrap_or_default());

        if let Some(password) = &user_info.password {
            user.set_password(password.clone());
        }

        if let Some(user_type) = &user_info.user_type {
            if let Some(ut) = UserType::get_by_name(user_type) {
                user.set_user_type(ut);
            }
        }

        if let Some(user_status) = &user_info.user_status {
            if let Some(us) = UserStatus::get_by_name(user_status) {
                user.set_user_status(us);
            }
        }

        user
    }
}
