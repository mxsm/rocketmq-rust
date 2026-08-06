use rocketmq_macros::RequestHeaderCodecV2;

#[derive(serde::Serialize, RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct MalformedRename {
    #[serde(rename = 7)]
    value: String,
}

fn main() {}
