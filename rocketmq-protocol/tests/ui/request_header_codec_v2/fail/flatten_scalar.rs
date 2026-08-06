use rocketmq_macros::RequestHeaderCodecV2;

#[derive(serde::Serialize, RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct FlattenScalar {
    #[serde(flatten)]
    value: i32,
}

fn main() {}
