use cheetah_string::CheetahString;

use super::TraceType;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceRecord {
    pub trace_type: TraceType,
    pub fields: Vec<CheetahString>,
}

impl TraceRecord {
    pub fn new(trace_type: TraceType, fields: Vec<CheetahString>) -> Self {
        Self { trace_type, fields }
    }
}
