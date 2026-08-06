use rocketmq_macros::RequestHeaderCodecV3;

#[derive(RequestHeaderCodecV3, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
#[header(type_id = "fixtures::SerdeIndependent", crate = "rocketmq_protocol")]
struct SerdeIndependent {
    #[serde(rename = "json-name")]
    #[header(key = "wireName", required)]
    name: String,
}

fn main() {}
