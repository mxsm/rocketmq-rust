use rocketmq_macros::RequestHeaderCodecV3;

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::Tuple", crate = "rocketmq_protocol")]
struct Tuple(#[header(required)] String);

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::Unit", crate = "rocketmq_protocol")]
struct Unit;

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::Enum", crate = "rocketmq_protocol")]
enum HeaderEnum {
    Value,
}

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::Union", crate = "rocketmq_protocol")]
union HeaderUnion {
    value: i32,
}

fn main() {}
