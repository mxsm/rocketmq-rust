use rocketmq_macros::RequestHeaderCodecV3;

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::Attributes", crate = "rocketmq_protocol")]
struct InvalidAttributes {
    #[header(required, binary_order = 7)]
    first: String,
    #[header(required, binary_order = 7)]
    second: String,
    #[header(required, mystery = "value")]
    unknown: String,
    #[header(required = true)]
    valued_flag: String,
    #[required]
    #[header(required)]
    duplicate_required_forms: String,
}

fn main() {}
