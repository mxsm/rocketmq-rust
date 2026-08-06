use rocketmq_macros::RequestHeaderCodecV3;
use rocketmq_protocol::protocol::header_codec::HeaderCodec;
use rocketmq_protocol::{CommandCustomHeader, FromMap, HeaderMap};

#[derive(RequestHeaderCodecV3)]
#[header(
    type_id = "fixtures::ManualLegacyShim",
    legacy_shim = "manual",
    crate = "rocketmq_protocol"
)]
struct ManualLegacyShim {
    #[header(required)]
    value: i32,
}

impl CommandCustomHeader for ManualLegacyShim {
    fn to_map(&self) -> Option<HeaderMap> {
        let mut map = HeaderMap::new();
        self.try_encode_into_map(&mut map).ok()?;
        Some(map)
    }

    fn try_encode_into_map(
        &self,
        out: &mut HeaderMap,
    ) -> Result<(), rocketmq_protocol::protocol::header_codec::HeaderCodecError> {
        let mut sink = rocketmq_protocol::protocol::header_codec::MapSink::new(out);
        <Self as HeaderCodec>::encode_into(self, &mut sink)
    }
}

impl FromMap for ManualLegacyShim {
    type Error = rocketmq_protocol::__request_header_codec::RocketMQError;
    type Target = Self;

    fn from(map: &HeaderMap) -> Result<Self::Target, Self::Error> {
        <Self as HeaderCodec>::decode_from_map(map).map_err(|error| {
            rocketmq_protocol::__request_header_codec::RocketMQError::request_header_error(error.to_string())
        })
    }
}

fn main() {}
