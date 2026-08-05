use rocketmq_macros::RequestHeaderCodecV2;

#[derive(RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct RequiredOption {
    #[required]
    value: Option<String>,
}

fn main() {}
