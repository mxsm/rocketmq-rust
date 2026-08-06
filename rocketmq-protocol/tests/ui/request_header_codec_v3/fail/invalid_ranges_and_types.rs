use rocketmq_macros::RequestHeaderCodecV3;

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::Ranges", crate = "rocketmq_protocol")]
struct InvalidRanges {
    #[header(required, java_type = "long", range = "i32")]
    wrong_u32_range: u32,
    #[header(required, java_type = "long")]
    missing_u64_range: u64,
    #[header(required, java_type = "long", range = "i64")]
    signed_with_range: i64,
    #[header(required, java_type = "boolean")]
    incompatible_java_type: i32,
    #[header(required)]
    unsupported: usize,
    #[header(required, range = "u64")]
    invalid_range_name: u64,
}

fn main() {}
