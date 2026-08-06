use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV3;
use rocketmq_model::boundary_type::BoundaryType;

fn default_flag() -> bool {
    false
}

struct Nested<T> {
    value: T,
}

#[derive(RequestHeaderCodecV3)]
#[header(
    type_id = "fixtures::CompleteHeader",
    java_class = "org.apache.rocketmq.fixtures.CompleteHeader",
    validate = "Self::validate_header",
    lookup = "get",
    crate = "rocketmq_protocol",
    fast
)]
struct CompleteHeader<T> {
    #[header(key = "name", alias = "legacyName", alias_conflict = "prefer_canonical", required)]
    name: CheetahString,
    #[header(key = "description")]
    description: Option<String>,
    #[header(default_with = "default_flag", default_semantic = "literal:false")]
    enabled: bool,
    #[header(default, default_semantic = "literal:0")]
    attempts: i32,
    #[header(required)]
    timestamp: i64,
    #[header(required, java_type = "int", range = "i32")]
    queue_id: u32,
    #[header(required, java_type = "long", range = "i64")]
    offset: u64,
    #[header(default, default_semantic = "literal:LOWER")]
    boundary: BoundaryType,
    #[header(required)]
    generic: T,
    #[header(flatten, presence = "any")]
    nested: Option<Nested<T>>,
}

impl<T> CompleteHeader<T> {
    fn validate_header(&self) -> Result<(), rocketmq_protocol::protocol::header_codec::HeaderCodecError> {
        Ok(())
    }
}

fn main() {}
