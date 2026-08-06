use rocketmq_macros::RequestHeaderCodecV3;

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::MissingPolicies", crate = "rocketmq_protocol")]
struct MissingPolicies {
    #[header(required)]
    required_option: Option<String>,
    implicit_default: bool,
    #[header(required, default, default_semantic = "literal:false")]
    conflicting: bool,
    #[header(default)]
    missing_semantic: i32,
    #[header(required, default_semantic = "literal:0")]
    unexpected_semantic: i32,
    #[header(default, default_semantic = "unstable")]
    malformed_semantic: i32,
}

fn main() {}
