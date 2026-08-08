#![deny(deprecated)]

use rocketmq_macros::RequestHeaderCodecV2;

#[derive(RequestHeaderCodecV2)]
struct DeprecatedHeader {
    value: Option<i32>,
}

fn main() {}
