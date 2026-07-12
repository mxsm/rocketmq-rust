// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;

use bytes::Buf;
use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use crate::common::sys_flag::message_sys_flag::MessageSysFlag;

pub const MESSAGE_MAGIC_CODE: i32 = -626_843_481;
pub const MESSAGE_MAGIC_CODE_V2: i32 = -626_843_477;
pub const NAME_VALUE_SEPARATOR: u8 = 1;
pub const PROPERTY_SEPARATOR: u8 = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedMessageFrame {
    pub store_size: i32,
    pub magic_code: i32,
    pub body_crc: u32,
    pub queue_id: i32,
    pub flag: i32,
    pub queue_offset: i64,
    pub commit_log_offset: i64,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub born_host: SocketAddr,
    pub store_timestamp: i64,
    pub store_host: SocketAddr,
    pub reconsume_times: i32,
    pub prepared_transaction_offset: i64,
    pub body: Bytes,
    pub topic: CheetahString,
    pub properties_string: CheetahString,
    pub properties: HashMap<CheetahString, CheetahString>,
}

pub fn decode_message_frame(input: &mut Bytes) -> RocketMQResult<DecodedMessageFrame> {
    let initial_len = input.remaining();
    let store_size = read_i32(input)?;
    if store_size <= 0 || store_size as usize > initial_len {
        return Err(invalid("invalid total message size"));
    }
    let magic_code = read_i32(input)?;
    if magic_code != MESSAGE_MAGIC_CODE && magic_code != MESSAGE_MAGIC_CODE_V2 {
        return Err(invalid("unknown message magic code"));
    }
    let body_crc = read_u32(input)?;
    let queue_id = read_i32(input)?;
    let flag = read_i32(input)?;
    let queue_offset = read_i64(input)?;
    let commit_log_offset = read_i64(input)?;
    let sys_flag = read_i32(input)?;
    let born_timestamp = read_i64(input)?;
    let born_host = read_host(input, sys_flag & MessageSysFlag::BORNHOST_V6_FLAG != 0)?;
    let store_timestamp = read_i64(input)?;
    let store_host = read_host(input, sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG != 0)?;
    let reconsume_times = read_i32(input)?;
    let prepared_transaction_offset = read_i64(input)?;
    let body = read_sized_i32(input)?;
    let topic_len = if magic_code == MESSAGE_MAGIC_CODE_V2 {
        let value = read_i16(input)?;
        usize::try_from(value).map_err(|_| invalid("negative topic length"))?
    } else {
        usize::from(read_u8(input)?)
    };
    let topic_bytes = take(input, topic_len)?;
    let topic = std::str::from_utf8(&topic_bytes)
        .map(CheetahString::from_slice)
        .map_err(|_| invalid("topic is not valid UTF-8"))?;
    let property_len = usize::try_from(read_i16(input)?).map_err(|_| invalid("negative properties length"))?;
    let property_bytes = take(input, property_len)?;
    let properties_string = CheetahString::from(String::from_utf8_lossy(&property_bytes).as_ref());
    let properties = decode_properties(&property_bytes);

    Ok(DecodedMessageFrame {
        store_size,
        magic_code,
        body_crc,
        queue_id,
        flag,
        queue_offset,
        commit_log_offset,
        sys_flag,
        born_timestamp,
        born_host,
        store_timestamp,
        store_host,
        reconsume_times,
        prepared_transaction_offset,
        body,
        topic,
        properties_string,
        properties,
    })
}

pub fn decode_properties(bytes: &[u8]) -> HashMap<CheetahString, CheetahString> {
    bytes
        .split(|byte| *byte == PROPERTY_SEPARATOR)
        .filter_map(|entry| {
            let separator = entry.iter().position(|byte| *byte == NAME_VALUE_SEPARATOR)?;
            if separator == 0 || separator + 1 >= entry.len() {
                return None;
            }
            let key = String::from_utf8_lossy(&entry[..separator]);
            let value = String::from_utf8_lossy(&entry[separator + 1..]);
            Some((CheetahString::from(key.as_ref()), CheetahString::from(value.as_ref())))
        })
        .collect()
}

fn invalid(reason: &'static str) -> RocketMQError {
    RocketMQError::request_body_invalid("decode_message_frame", reason)
}

fn ensure(input: &Bytes, len: usize) -> RocketMQResult<()> {
    if input.remaining() < len {
        Err(invalid("truncated message frame"))
    } else {
        Ok(())
    }
}

fn read_u8(input: &mut Bytes) -> RocketMQResult<u8> {
    ensure(input, 1)?;
    Ok(input.get_u8())
}
fn read_i16(input: &mut Bytes) -> RocketMQResult<i16> {
    ensure(input, 2)?;
    Ok(input.get_i16())
}
fn read_i32(input: &mut Bytes) -> RocketMQResult<i32> {
    ensure(input, 4)?;
    Ok(input.get_i32())
}
fn read_u32(input: &mut Bytes) -> RocketMQResult<u32> {
    ensure(input, 4)?;
    Ok(input.get_u32())
}
fn read_i64(input: &mut Bytes) -> RocketMQResult<i64> {
    ensure(input, 8)?;
    Ok(input.get_i64())
}
fn take(input: &mut Bytes, len: usize) -> RocketMQResult<Bytes> {
    ensure(input, len)?;
    Ok(input.split_to(len))
}
fn read_sized_i32(input: &mut Bytes) -> RocketMQResult<Bytes> {
    let len = usize::try_from(read_i32(input)?).map_err(|_| invalid("negative body length"))?;
    take(input, len)
}

fn read_host(input: &mut Bytes, ipv6: bool) -> RocketMQResult<SocketAddr> {
    if ipv6 {
        let bytes = take(input, 16)?;
        let mut address = [0_u8; 16];
        address.copy_from_slice(&bytes);
        let port = read_i32(input)? as u16;
        Ok(SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(address), port, 0, 0)))
    } else {
        let bytes = take(input, 4)?;
        let mut address = [0_u8; 4];
        address.copy_from_slice(&bytes);
        let port = read_i32(input)? as u16;
        Ok(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(address), port)))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use bytes::BytesMut;

    use super::*;

    fn golden_frame() -> Bytes {
        let mut frame = BytesMut::new();
        frame.put_i32(0);
        frame.put_i32(MESSAGE_MAGIC_CODE);
        frame.put_u32(7);
        frame.put_i32(3);
        frame.put_i32(9);
        frame.put_i64(11);
        frame.put_i64(13);
        frame.put_i32(0);
        frame.put_i64(17);
        frame.put_slice(&[127, 0, 0, 1]);
        frame.put_i32(10911);
        frame.put_i64(19);
        frame.put_slice(&[127, 0, 0, 2]);
        frame.put_i32(10912);
        frame.put_i32(2);
        frame.put_i64(23);
        frame.put_i32(4);
        frame.put_slice(b"body");
        frame.put_u8(5);
        frame.put_slice(b"topic");
        frame.put_i16(8);
        frame.put_slice(b"k\x01value\x02");
        let len = frame.len() as i32;
        frame[..4].copy_from_slice(&len.to_be_bytes());
        frame.freeze()
    }

    #[test]
    fn decodes_frozen_v1_frame() {
        let decoded = decode_message_frame(&mut golden_frame()).unwrap();
        assert_eq!(decoded.topic, "topic");
        assert_eq!(decoded.body, Bytes::from_static(b"body"));
        assert_eq!(decoded.properties.get("k").map(CheetahString::as_str), Some("value"));
        assert_eq!(decoded.born_host.to_string(), "127.0.0.1:10911");
    }

    #[test]
    fn rejects_truncated_and_unknown_frames() {
        let mut truncated = golden_frame().slice(..20);
        assert!(decode_message_frame(&mut truncated).is_err());
        let mut unknown = golden_frame();
        let mut bytes = BytesMut::from(unknown.as_ref());
        bytes[4..8].copy_from_slice(&0_i32.to_be_bytes());
        unknown = bytes.freeze();
        assert!(decode_message_frame(&mut unknown).is_err());
    }
}
