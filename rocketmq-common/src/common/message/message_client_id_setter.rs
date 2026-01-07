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

use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::LazyLock;

use bytes::BufMut;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use chrono::Datelike;
use chrono::Months;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use parking_lot::Mutex;

use crate::common::hasher::string_hasher::JavaStringHasher;
use crate::common::message::message_single::Message;
use crate::common::message::MessageConst;
use crate::common::message::MessageTrait;
use crate::utils::util_all;
use crate::TimeUtils::get_current_millis;
use crate::UtilAll::bytes_to_string;
use crate::UtilAll::write_int;
use crate::UtilAll::write_short;

static COUNTER: LazyLock<AtomicI32> = LazyLock::new(|| AtomicI32::new(0));
static START_TIME: LazyLock<Mutex<i64>> = LazyLock::new(|| Mutex::new(0));
static NEXT_START_TIME: LazyLock<Mutex<i64>> = LazyLock::new(|| Mutex::new(0));
static LEN: LazyLock<usize> = LazyLock::new(|| {
    let ip = util_all::get_ip().unwrap_or_else(|_| create_fake_ip());
    ip.len() + 2 + 4 + 4 + 2
});
static FIX_STRING: LazyLock<Vec<char>> = LazyLock::new(|| {
    let ip = util_all::get_ip().unwrap_or_else(|_| create_fake_ip());
    let pid = std::process::id() as i16;
    let class_loader_hash = JavaStringHasher::hash_str("MessageClientIDSetter");
    let mut bytes = BytesMut::with_capacity(ip.len() + 2 + 4);
    bytes.put(ip.as_slice());
    bytes.put_i16(pid);
    bytes.put_i32(class_loader_hash);
    let data = bytes_to_string(bytes.freeze().as_ref()).chars().collect::<Vec<char>>();
    data
});

pub fn create_fake_ip() -> Vec<u8> {
    get_current_millis().to_be_bytes()[4..].to_vec()
}

pub struct MessageClientIDSetter;

impl MessageClientIDSetter {
    #[inline]
    pub fn get_uniq_id<T>(message: &T) -> Option<CheetahString>
    where
        T: MessageTrait,
    {
        message.get_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
        ))
    }

    fn set_start_time(millis: i64) {
        let dt = Utc.timestamp_millis_opt(millis).unwrap();
        let cal = Utc.with_ymd_and_hms(dt.year(), dt.month(), 1, 0, 0, 0).unwrap();

        *START_TIME.lock() = cal.timestamp_millis();

        *NEXT_START_TIME.lock() = cal.checked_add_months(Months::new(1)).unwrap().timestamp_millis();
    }

    pub fn create_uniq_id() -> String {
        let mut sb = vec!['\0'; *LEN * 2];
        sb[..FIX_STRING.len()].copy_from_slice(&FIX_STRING);

        let current = get_current_millis() as i64;
        if current >= *NEXT_START_TIME.lock() {
            Self::set_start_time(current);
        }

        let mut diff = (current - *START_TIME.lock()) as i32;
        if diff < 0 && diff > -1_000_000 {
            // May cause by NTP
            diff = 0;
        }

        let pos = FIX_STRING.len();
        write_int(&mut sb, pos, diff);

        let counter_val = COUNTER.fetch_add(1, Ordering::SeqCst) as i16;
        write_short(&mut sb, pos + 8, counter_val);
        sb.into_iter().collect()
    }

    pub fn set_uniq_id<T>(message: &mut T)
    where
        T: MessageTrait,
    {
        if message
            .get_property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            ))
            .is_none()
        {
            let uniq_id = Self::create_uniq_id();
            message.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                CheetahString::from_string(uniq_id),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::net::Ipv4Addr;

    use chrono::Utc;

    use super::*;

    #[test]
    fn unique_id_is_generated_correctly() {
        let message = Message::default();
        let unique_id = MessageClientIDSetter::create_uniq_id();
        assert!(!unique_id.is_empty());
    }

    #[test]
    fn unique_id_counter_increments_properly() {
        let first_id = MessageClientIDSetter::create_uniq_id();
        let second_id = MessageClientIDSetter::create_uniq_id();
        let first_counter = &first_id[FIX_STRING.len() + 8..];
        let second_counter = &second_id[FIX_STRING.len() + 8..];
        assert_ne!(first_counter, second_counter);
    }

    #[test]
    fn get_uniq_id_returns_none_when_not_set() {
        let message = Message::default();
        assert_eq!(MessageClientIDSetter::get_uniq_id(&message), None);
    }

    #[test]
    fn create_fake_ip_generates_valid_ip() {
        let fake_ip = create_fake_ip();
        assert_eq!(fake_ip.len(), 4);
        assert!(fake_ip.iter().all(|&byte| true));
    }

    #[test]
    fn start_time_is_set_correctly_for_new_month() {
        let millis = Utc::now().timestamp_millis();
        MessageClientIDSetter::set_start_time(millis);
        let locked_start_time = *START_TIME.lock();
        assert!(locked_start_time <= millis);
    }

    #[test]
    fn next_start_time_is_set_correctly_for_new_month() {
        let millis = Utc::now().timestamp_millis();
        MessageClientIDSetter::set_start_time(millis);
        let locked_next_start_time = *NEXT_START_TIME.lock();
        assert!(locked_next_start_time > millis);
    }
}
