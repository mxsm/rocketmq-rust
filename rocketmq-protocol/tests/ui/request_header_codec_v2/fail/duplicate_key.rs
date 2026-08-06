use rocketmq_macros::RequestHeaderCodecV2;

#[derive(serde::Serialize, RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct DuplicateKey {
    #[serde(rename = "same")]
    first: String,
    #[serde(alias = "same")]
    second: String,
}

fn main() {}
