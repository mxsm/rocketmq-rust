use rocketmq_macros::RequestHeaderCodecV2;

#[derive(RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct GenericHeader<T>
where
    T: Default + ToString + std::str::FromStr + 'static,
{
    value: T,
}

fn main() {
    let _ = GenericHeader::<u32> { value: 7 };
}
