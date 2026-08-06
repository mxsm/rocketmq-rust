use rocketmq_protocol::protocol::header_codec::{
    EncodeSink, HeaderCodecError, HeaderFieldContext, HeaderValue,
};

struct ExternalSink;

impl EncodeSink for ExternalSink {
    fn write<V: HeaderValue>(
        &mut self,
        _key: &'static str,
        _value: &V,
        _context: HeaderFieldContext,
    ) -> Result<(), HeaderCodecError> {
        Ok(())
    }
}

fn main() {}
