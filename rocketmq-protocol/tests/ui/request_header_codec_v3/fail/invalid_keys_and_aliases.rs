use rocketmq_macros::RequestHeaderCodecV3;

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::Keys", crate = "rocketmq_protocol")]
struct InvalidKeys {
    #[header(key = "", required)]
    empty: String,
    #[header(key = "same", alias = "same", alias = "repeat", alias = "repeat", required)]
    duplicate_aliases: String,
    #[header(key = "same", required)]
    duplicate_canonical: String,
    #[header(key = "standalone", alias_conflict = "prefer_canonical", required)]
    missing_alias: String,
    #[header(key = "bad-policy", alias = "legacy", alias_conflict = "first", required)]
    invalid_policy: String,
}

fn main() {}
