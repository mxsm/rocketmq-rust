use rocketmq_protocol::protocol::header_codec::{EncodeSink, HeaderFieldContext, HeaderValue};
use rocketmq_protocol::ProtocolContractViolation;

struct ExternalSink;

impl EncodeSink for ExternalSink {
    fn write<V: HeaderValue>(
        &mut self,
        _key: &'static str,
        _value: &V,
        _context: HeaderFieldContext,
    ) -> Result<(), ProtocolContractViolation> {
        Ok(())
    }
}

fn main() {}
