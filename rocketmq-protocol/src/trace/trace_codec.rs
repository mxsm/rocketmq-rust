use cheetah_string::CheetahString;

use super::trace_constants::TraceConstants;
use super::TraceRecord;
use super::TraceType;

pub fn encode_records(records: &[TraceRecord]) -> CheetahString {
    let mut output = String::new();
    for record in records {
        output.push_str(&record.trace_type.to_string());
        for field in &record.fields {
            output.push(TraceConstants::CONTENT_SPLITOR);
            output.push_str(field);
        }
        output.push(TraceConstants::FIELD_SPLITOR);
    }
    CheetahString::from_string(output)
}

pub fn decode_records(input: &str) -> Vec<TraceRecord> {
    input
        .split(TraceConstants::FIELD_SPLITOR)
        .filter(|record| !record.is_empty())
        .filter_map(|record| {
            let mut fields = record.split(TraceConstants::CONTENT_SPLITOR);
            let trace_type = TraceType::parse(fields.next()?)?;
            Some(TraceRecord::new(
                trace_type,
                fields.map(CheetahString::from_slice).collect(),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_pub_trace_round_trip_preserves_delimiters_and_order() {
        let frozen = "Pub\u{1}1710000000000\u{1}region\u{1}group\u{1}topic\u{1}msg-id\u{2}";
        let records = decode_records(frozen);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].trace_type, TraceType::Pub);
        assert_eq!(encode_records(&records), frozen);
    }

    #[test]
    fn malformed_and_unknown_records_are_skipped() {
        let records = decode_records("Unknown\u{1}x\u{2}\u{2}SubAfter\u{1}request\u{2}");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].trace_type, TraceType::SubAfter);
    }
}
