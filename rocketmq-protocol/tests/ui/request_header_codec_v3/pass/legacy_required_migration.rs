use rocketmq_macros::RequestHeaderCodecV3;

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::LegacyRequired", crate = "rocketmq_protocol")]
struct LegacyRequired {
    #[required]
    value: String,
}

fn main() {}
