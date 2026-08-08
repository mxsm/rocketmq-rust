#![allow(deprecated)]

use rocketmq_macros::RequestHeaderCodecV2;

#[derive(serde::Serialize, RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct CombinedErrors {
    #[required]
    #[serde(default)]
    first: Option<String>,
}

fn main() {}
