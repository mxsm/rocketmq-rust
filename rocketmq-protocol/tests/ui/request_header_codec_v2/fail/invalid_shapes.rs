#![allow(deprecated)]

use rocketmq_macros::RequestHeaderCodecV2;

#[derive(RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct UnitHeader;

#[derive(RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
enum EnumHeader {
    Value,
}

#[derive(RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
union UnionHeader {
    value: i32,
}

fn main() {}
