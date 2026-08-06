use rocketmq_macros::RequestHeaderCodecV3;

#[derive(RequestHeaderCodecV3)]
#[header(crate = "rocketmq_protocol")]
struct MissingTypeId {
    #[header(required)]
    value: String,
}

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "Header", lookup = "indexed", crate = "rocketmq_protocol")]
struct InvalidContainerValues {
    #[header(required)]
    value: String,
}

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::Unknown", unknown, crate = "rocketmq_protocol")]
struct UnknownContainerOption {
    #[header(required)]
    value: String,
}

fn main() {}
