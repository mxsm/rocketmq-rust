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
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::LazyLock;

use bytes::BufMut;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use chrono::DateTime;
use chrono::Datelike;
use chrono::Local;
use chrono::LocalResult;
use chrono::Months;
use chrono::TimeZone;
use chrono::Timelike;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use crate::common::hasher::string_hasher::JavaStringHasher;
use crate::common::message::message_single::Message;
use crate::common::message::MessageConst;
use crate::common::message::MessageTrait;
use crate::utils::util_all;
use crate::TimeUtils::current_millis;
use crate::UtilAll::bytes_to_string;
use crate::UtilAll::write_int;
use crate::UtilAll::write_short;

static COUNTER: LazyLock<AtomicI32> = LazyLock::new(|| AtomicI32::new(0));
static TIME_WINDOW: LazyLock<TimeWindow> = LazyLock::new(|| {
    let (start_time, next_start_time) = compute_time_window(current_millis() as i64);
    TimeWindow {
        start_time: AtomicI64::new(start_time),
        next_start_time: AtomicI64::new(next_start_time),
    }
});
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

struct TimeWindow {
    start_time: AtomicI64,
    next_start_time: AtomicI64,
}

pub fn create_fake_ip() -> Vec<u8> {
    current_millis().to_be_bytes()[4..].to_vec()
}

fn local_timestamp_millis_or_epoch(millis: i64) -> DateTime<Local> {
    Local
        .timestamp_millis_opt(millis)
        .single()
        .or_else(|| Local.timestamp_millis_opt(0).single())
        .unwrap_or_else(Local::now)
}

fn local_month_start(dt: DateTime<Local>) -> DateTime<Local> {
    match Local.with_ymd_and_hms(dt.year(), dt.month(), 1, 0, 0, 0) {
        LocalResult::Single(value) => value,
        LocalResult::Ambiguous(earliest, _) => earliest,
        LocalResult::None => dt,
    }
}

fn compute_time_window(millis: i64) -> (i64, i64) {
    let start = local_month_start(local_timestamp_millis_or_epoch(millis));
    let next = start.checked_add_months(Months::new(1)).unwrap_or(start);
    (start.timestamp_millis(), next.timestamp_millis())
}

fn decode_uniq_id_bytes(msg_id: &str) -> RocketMQResult<Vec<u8>> {
    let msg_id = msg_id.trim();
    if msg_id.len() != 32 && msg_id.len() != 56 {
        return Err(RocketMQError::illegal_argument(format!(
            "Invalid uniq id length: {}. Expected 32 characters (IPv4) or 56 characters (IPv6)",
            msg_id.len()
        )));
    }
    if !msg_id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(RocketMQError::illegal_argument(
            "Invalid uniq id: expected hexadecimal characters",
        ));
    }
    util_all::string_to_bytes(msg_id).ok_or_else(|| RocketMQError::illegal_argument("Invalid uniq id hex"))
}

pub struct MessageClientIDSetter;

impl MessageClientIDSetter {
    #[inline]
    pub fn get_uniq_id<T>(message: &T) -> Option<CheetahString>
    where
        T: MessageTrait,
    {
        message.property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
        ))
    }

    fn set_start_time(millis: i64) {
        let (start_time, next_start_time) = compute_time_window(millis);
        TIME_WINDOW.start_time.store(start_time, Ordering::Release);
        TIME_WINDOW.next_start_time.store(next_start_time, Ordering::Release);
    }

    pub fn create_uniq_id() -> String {
        let mut sb = vec!['\0'; *LEN * 2];
        sb[..FIX_STRING.len()].copy_from_slice(&FIX_STRING);

        let current = current_millis() as i64;
        if current >= TIME_WINDOW.next_start_time.load(Ordering::Acquire) {
            Self::set_start_time(current);
        }

        let mut diff = (current - TIME_WINDOW.start_time.load(Ordering::Acquire)) as i32;
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

    pub fn get_nearly_time_from_id(msg_id: &str) -> RocketMQResult<DateTime<Local>> {
        let bytes = decode_uniq_id_bytes(msg_id)?;
        let ip_length = if bytes.len() == 28 { 16 } else { 4 };
        let diff_start = ip_length + 2 + 4;
        let span_ms = u32::from_be_bytes([
            bytes[diff_start],
            bytes[diff_start + 1],
            bytes[diff_start + 2],
            bytes[diff_start + 3],
        ]) as i64;
        let now = Local::now();
        let mut month_start = local_month_start(now).timestamp_millis();
        if month_start.saturating_add(span_ms) >= now.timestamp_millis() {
            month_start = local_month_start(
                now.checked_sub_months(Months::new(1))
                    .unwrap_or_else(|| local_timestamp_millis_or_epoch(0)),
            )
            .timestamp_millis();
        }
        let timestamp = month_start.saturating_add(span_ms);
        Local
            .timestamp_millis_opt(timestamp)
            .single()
            .ok_or_else(|| RocketMQError::illegal_argument("Invalid uniq id timestamp"))
    }

    pub fn get_ip_from_id(msg_id: &str) -> RocketMQResult<Vec<u8>> {
        let bytes = decode_uniq_id_bytes(msg_id)?;
        let ip_length = if bytes.len() == 28 { 16 } else { 4 };
        Ok(bytes[..ip_length].to_vec())
    }

    pub fn get_ip_str_from_id(msg_id: &str) -> RocketMQResult<String> {
        let ip_bytes = Self::get_ip_from_id(msg_id)?;
        match ip_bytes.len() {
            4 => Ok(std::net::Ipv4Addr::new(ip_bytes[0], ip_bytes[1], ip_bytes[2], ip_bytes[3]).to_string()),
            16 => {
                let mut segments = [0u8; 16];
                segments.copy_from_slice(&ip_bytes);
                Ok(std::net::Ipv6Addr::from(segments).to_string())
            }
            len => Err(RocketMQError::illegal_argument(format!(
                "Invalid uniq id IP length: {len}"
            ))),
        }
    }

    pub fn get_pid_from_id(msg_id: &str) -> RocketMQResult<i32> {
        let bytes = decode_uniq_id_bytes(msg_id)?;
        let pid_start = bytes.len() - 2 - 4 - 4 - 2;
        let pid = u16::from_be_bytes([bytes[pid_start], bytes[pid_start + 1]]);
        Ok(i32::from(pid))
    }

    pub fn set_uniq_id<T>(message: &mut T)
    where
        T: MessageTrait,
    {
        if message
            .property(&CheetahString::from_static_str(
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
    use chrono::Local;

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
        let millis = Local::now().timestamp_millis();
        MessageClientIDSetter::set_start_time(millis);
        let locked_start_time = TIME_WINDOW.start_time.load(Ordering::Acquire);
        assert!(locked_start_time <= millis);
    }

    #[test]
    fn next_start_time_is_set_correctly_for_new_month() {
        let millis = Local::now().timestamp_millis();
        MessageClientIDSetter::set_start_time(millis);
        let locked_next_start_time = TIME_WINDOW.next_start_time.load(Ordering::Acquire);
        assert!(locked_next_start_time > millis);
    }

    #[test]
    fn nearly_time_from_id_is_close_to_now() {
        let before = Local::now().timestamp_millis();
        let unique_id = MessageClientIDSetter::create_uniq_id();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let nearly_time = MessageClientIDSetter::get_nearly_time_from_id(&unique_id)
            .expect("generated uniq id should decode time")
            .timestamp_millis();
        let after = Local::now().timestamp_millis();

        assert!(nearly_time >= before - 1_000);
        assert!(nearly_time <= after + 1_000);
    }

    #[test]
    fn ip_and_pid_decode_from_generated_id() {
        let unique_id = MessageClientIDSetter::create_uniq_id();

        let ip = MessageClientIDSetter::get_ip_from_id(&unique_id).expect("generated uniq id should decode IP");
        assert!(ip.len() == 4 || ip.len() == 16);
        let ip_str =
            MessageClientIDSetter::get_ip_str_from_id(&unique_id).expect("generated uniq id should decode IP string");
        assert!(!ip_str.is_empty());
        let pid = MessageClientIDSetter::get_pid_from_id(&unique_id).expect("generated uniq id should decode PID");
        assert_eq!(pid, i32::from(std::process::id() as u16));
    }

    #[test]
    fn malformed_id_decoders_return_error() {
        assert!(MessageClientIDSetter::get_nearly_time_from_id("bad-id").is_err());
        assert!(MessageClientIDSetter::get_ip_from_id("bad-id").is_err());
        assert!(MessageClientIDSetter::get_ip_str_from_id("bad-id").is_err());
        assert!(MessageClientIDSetter::get_pid_from_id("bad-id").is_err());
    }
}
