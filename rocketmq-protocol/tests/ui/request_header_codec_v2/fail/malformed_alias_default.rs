#![allow(deprecated)]

use rocketmq_macros::RequestHeaderCodecV2;

#[derive(serde::Serialize, RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct MalformedAlias {
    #[serde(alias = 7)]
    value: String,
}

#[derive(serde::Serialize, RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct MalformedDefault {
    #[serde(default = "not a valid::path")]
    value: String,
}

fn main() {}
