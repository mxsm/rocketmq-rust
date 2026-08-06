use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::header_codec::{
    HeaderCodecError, HeaderFieldContext, HeaderValue, HeaderValueKind,
};

struct ExternalValue;

impl HeaderValue for ExternalValue {
    const KIND: HeaderValueKind = HeaderValueKind::String;

    fn to_map_value(&self) -> CheetahString {
        CheetahString::new()
    }

    fn encoded_len(&self) -> usize {
        0
    }

    fn write_ascii(&self, _out: &mut BytesMut) {}

    fn decode(_raw: &str, _context: HeaderFieldContext) -> Result<Self, HeaderCodecError> {
        Ok(Self)
    }
}

fn main() {}
