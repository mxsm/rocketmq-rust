// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
fn properties() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("RETRY_TOPIC".into(), "orders".into()),
        ("ORIGIN_MESSAGE_ID".into(), "original-id".into()),
    ])
}
fn source() -> MessageIdentity {
    MessageIdentity {
        topic: "%DLQ%group-a".into(),
        message_id: "physical-id".into(),
    }
}
#[test]
fn dlq_identity_requires_consistent_original_topic_group_and_message_evidence() {
    let props = properties();
    let target = DlqResendTarget::resolve("group-a", source(), &props).unwrap();
    assert_eq!(target.original().topic, "orders");
    assert_eq!(target.original().message_id, "original-id");
    assert_eq!(target.consumer_group(), "group-a");
    assert!(!format!("{target:?}").contains("original-id"));
    for key in ["RETRY_TOPIC", "ORIGIN_MESSAGE_ID"] {
        let mut invalid = props.clone();
        invalid.remove(key);
        assert!(DlqResendTarget::resolve("group-a", source(), &invalid).is_err());
    }
    for (key, value) in [
        ("DLQ_ORIGIN_MESSAGE_ID", "different"),
        ("ORIGIN_GROUP", "other-group"),
        ("STORE_MESSAGE_ID", "other-message"),
    ] {
        let mut invalid = props.clone();
        invalid.insert(key.into(), value.into());
        assert_eq!(
            DlqResendTarget::resolve("group-a", source(), &invalid).unwrap_err(),
            DlqIdentityError::Conflict
        );
    }
    assert!(DlqResendTarget::resolve("other-group", source(), &props).is_err());
    let mut fallback = props;
    let id = fallback.remove("ORIGIN_MESSAGE_ID").unwrap();
    fallback.insert("DLQ_ORIGIN_MESSAGE_ID".into(), id);
    assert_eq!(
        DlqResendTarget::resolve("group-a", source(), &fallback).unwrap(),
        target
    );
}
#[test]
fn dlq_preserved_producer_id_is_only_a_last_resort_in_the_verified_original_topic() {
    let mut props = properties();
    props.remove("ORIGIN_MESSAGE_ID");
    props.insert("UNIQ_KEY".into(), " producer-unique-id ".into());
    let legacy = DlqResendTarget::resolve("group-a", source(), &props).unwrap();
    assert_eq!(legacy.original().topic, "orders");
    assert_eq!(legacy.original().message_id, "producer-unique-id");
    assert_eq!(legacy.consumer_group(), "group-a");

    props.insert("UNIQ_KEY".into(), " \t ".into());
    assert_eq!(
        DlqResendTarget::resolve("group-a", source(), &props).unwrap_err(),
        DlqIdentityError::MissingId
    );
    props.insert("UNIQ_KEY".into(), "producer-unique-id".into());
    props.insert("DLQ_ORIGIN_MESSAGE_ID".into(), "physical-origin".into());
    assert_eq!(
        DlqResendTarget::resolve("group-a", source(), &props)
            .unwrap()
            .original()
            .message_id,
        "physical-origin"
    );
    props.insert("ORIGIN_MESSAGE_ID".into(), "other-explicit-origin".into());
    assert_eq!(
        DlqResendTarget::resolve("group-a", source(), &props).unwrap_err(),
        DlqIdentityError::Conflict
    );
    props.remove("DLQ_ORIGIN_MESSAGE_ID");
    assert_eq!(
        DlqResendTarget::resolve("group-a", source(), &props)
            .unwrap()
            .original()
            .message_id,
        "other-explicit-origin"
    );
    props.remove("ORIGIN_MESSAGE_ID");
    assert!(DlqResendTarget::resolve("other-group", source(), &props).is_err());
    props.insert("RETRY_TOPIC".into(), "%DLQ%other-group".into());
    assert_eq!(
        DlqResendTarget::resolve("group-a", source(), &props).unwrap_err(),
        DlqIdentityError::Conflict
    );
}
#[test]
fn dlq_query_keeps_server_pages_and_exact_id_on_the_first_page() {
    let query = DlqQuery {
        consumer_group: "group-a".into(),
        begin: 1,
        end: 2,
        message_id: None,
        page: 1,
        page_size: 20,
    };
    let next = query.with_page(2).unwrap();
    assert_eq!(next.begin, 1);
    assert_eq!(next.page, 2);
    let exact = DlqQuery {
        message_id: Some("original-id".into()),
        ..query
    };
    assert!(exact.validate().is_ok());
    assert!(exact.with_page(2).is_err());
    assert!(DlqQuery { begin: 3, ..next }.validate().is_err());
}
fn csv_row(body: String) -> DlqCsvRow {
    DlqCsvRow {
        message_id: "id,\"1\"".into(),
        topic: "主题".into(),
        keys: "=1+1".into(),
        tags: "\t@tag".into(),
        store_timestamp: 5,
        queue_id: 0,
        queue_offset: 9,
        body,
    }
}
#[test]
fn dlq_csv_preserves_full_unicode_newlines_quotes_and_neutralizes_formula_prefixes() {
    let body = "=hello,\"世界\"\r\nnext".to_string();
    let csv = DlqCsv::from_rows(vec![csv_row(body.clone())]).unwrap();
    assert_eq!(std::str::from_utf8(csv.as_bytes()).unwrap(), "messageId,topic,keys,tags,storeTimestamp,queueId,queueOffset,body\n\"id,\"\"1\"\"\",主题,'=1+1,'\t@tag,5,0,9,\"'=hello,\"\"世界\"\"\r\nnext\"\n");
    assert!(!format!("{csv:?}").contains(&body));
    for (body, escaped) in [
        ("+cmd", "'+cmd"),
        ("-2+3", "'-2+3"),
        ("@SUM(A1:A2)", "'@SUM(A1:A2)"),
        ("\r=1", "\"'\r=1\""),
        ("\n=1", "\"'\n=1\""),
    ] {
        let csv = DlqCsv::from_rows(vec![csv_row(body.into())]).unwrap();
        assert!(std::str::from_utf8(csv.as_bytes())
            .unwrap()
            .ends_with(&format!(",{escaped}\n")));
    }
    let long = "x".repeat(crate::MESSAGE_BODY_MAX_BYTES + 1);
    let csv = DlqCsv::from_rows(vec![csv_row(long.clone())]).unwrap();
    assert!(std::str::from_utf8(csv.as_bytes()).unwrap().contains(&long));
    assert_eq!(
        DlqCsv::from_rows(vec![csv_row("x".repeat(DLQ_CSV_MAX_BYTES))]).unwrap_err(),
        DlqCsvError::TooLarge
    );
}
#[test]
fn dlq_retry_excludes_success_unknown_and_unattempted() {
    let target = DlqResendTarget::resolve("group-a", source(), &properties()).unwrap();
    let entries = [
        DlqEntryState::Succeeded,
        DlqEntryState::BusinessFailure,
        DlqEntryState::CallFailed,
        DlqEntryState::Unknown,
        DlqEntryState::Unattempted,
    ]
    .into_iter()
    .map(|state| DlqEntryResult {
        target: target.clone(),
        attempt: DlqAttempt { state, receipt: None },
    })
    .collect();
    assert_eq!(
        DlqBatchResult { entries }.retryable_targets(),
        vec![target.clone(), target]
    );
}

#[test]
fn dlq_selected_sources_deduplicate_only_exact_original_tuples() {
    let first = DlqResendTarget::resolve("group-a", source(), &properties()).unwrap();
    let second_source = MessageIdentity {
        message_id: "second-dlq-source".into(),
        ..source()
    };
    let duplicate = DlqResendTarget::resolve("group-a", second_source, &properties()).unwrap();
    let third_source = MessageIdentity {
        topic: "%DLQ%group-b".into(),
        message_id: "third-dlq-source".into(),
    };
    let other_group = DlqResendTarget::resolve("group-b", third_source, &properties()).unwrap();
    let row = |target: DlqResendTarget| DlqRow {
        metadata: MessageMetadataRow {
            identity: target.source().clone(),
            tags: None,
            keys: None,
            born_host: crate::MessageTextPreview::bounded(""),
            store_host: crate::MessageTextPreview::bounded(""),
            born_timestamp: 0,
            store_timestamp: 0,
            queue_id: 0,
            queue_offset: 0,
            store_size: 0,
            reconsume_times: 0,
            sys_flag: 0,
            flag: 0,
            prepared_transaction_offset: 0,
        },
        target: Ok(target),
    };
    let rows = vec![row(first.clone()), row(duplicate), row(other_group.clone())];
    let selected = rows.iter().map(|row| row.metadata.identity.clone()).collect();
    assert_eq!(dlq_selected_targets(&rows, &selected), vec![first, other_group]);
    assert!(dlq_selected_targets(&rows, &BTreeSet::new()).is_empty());
}
