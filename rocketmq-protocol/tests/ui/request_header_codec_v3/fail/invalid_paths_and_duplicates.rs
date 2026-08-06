use rocketmq_macros::RequestHeaderCodecV3;

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::Paths")]
#[header(type_id = "fixtures::DuplicatePaths")]
#[header(java_class = "not a java class")]
#[header(validate = "not a path!")]
#[header(crate = "not a path!")]
#[header(fast)]
#[header(fast)]
struct InvalidPathsAndDuplicates<T> {
    #[header(default_with = "not a path!", default_semantic = "dynamic:provider")]
    invalid_default_path: i32,
    #[header(required, binary_order = 65536)]
    order_overflow: i32,
    #[header(required)]
    unsupported_generic_container: Vec<T>,
}

fn main() {}
