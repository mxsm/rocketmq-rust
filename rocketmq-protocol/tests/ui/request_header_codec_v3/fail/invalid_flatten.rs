use rocketmq_macros::RequestHeaderCodecV3;

struct Nested;

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::Flatten", crate = "rocketmq_protocol")]
struct InvalidFlatten {
    #[header(flatten)]
    optional_without_presence: Option<Nested>,
    #[header(flatten, presence = "any")]
    non_optional_any: Nested,
    #[header(flatten, key = "nested", required, java_type = "Nested")]
    conflicting_options: Nested,
    #[header(presence = "always", required)]
    presence_without_flatten: String,
    #[header(flatten, presence = "sometimes")]
    invalid_presence: Option<Nested>,
    #[header(flatten)]
    scalar: i32,
}

fn main() {}
