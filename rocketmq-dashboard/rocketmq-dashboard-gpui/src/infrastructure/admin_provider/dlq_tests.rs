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
use rocketmq_dashboard_common::{
    DLQ_CSV_MAX_BYTES, DlqEntryState, DlqQuery, DlqResendTarget, MessageIdentity, MessagePageExtent,
};
fn query() -> DlqQuery {
    DlqQuery {
        consumer_group: "group-a".into(),
        begin: 0,
        end: 1000,
        message_id: None,
        page: 3,
        page_size: 20,
    }
}
fn source() -> DashboardMessage {
    let mut message = raw_message("%DLQ%group-a", "dlq-source");
    message.properties.insert("RETRY_TOPIC".into(), "orders".into());
    message
        .properties
        .insert("ORIGIN_MESSAGE_ID".into(), "original-unique-id".into());
    message
}
fn target() -> DlqResendTarget {
    let source = source();
    DlqResendTarget::resolve(
        "group-a",
        MessageIdentity {
            topic: source.topic,
            message_id: source.message_id,
        },
        &source.properties,
    )
    .unwrap()
}
fn source_record() -> MessageRecord {
    let mut record = raw_record("%DLQ%group-a", "dlq-source");
    record.properties = source().properties;
    record
}
#[test]
fn dlq_inclusive_range_and_csv_keep_the_last_stored_item_at_exact_end() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    let records = [998, 999, 1000]
        .into_iter()
        .map(|timestamp| {
            let mut message = source();
            message.message_id = format!("source-{timestamp}");
            message.store_timestamp = timestamp;
            message.body = format!("full-body-{timestamp}").into_bytes();
            message
        })
        .collect::<Vec<_>>();
    *controls.message_scan_records.lock().unwrap() = Some(records);
    runtime.block_on(async {
        provider.switch(snapshot(5, ConnectionScope::NameServer)).await.unwrap();
        let equal = DlqQuery {
            begin: 1000,
            end: 1000,
            page: 1,
            page_size: 2,
            ..query()
        };
        let last = MessageIdentity {
            topic: equal.topic(),
            message_id: "source-1000".into(),
        };
        let page = provider.query_dlq(5, equal.clone()).await.unwrap();
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].metadata.identity, last);
        assert_eq!(page.extent, MessagePageExtent::End);
        let csv = provider.dlq_csv(5, equal.clone(), vec![last.clone()]).await.unwrap();
        let text = std::str::from_utf8(csv.as_bytes()).unwrap();
        assert!(text.contains("full-body-1000"));
        assert!(!text.contains("full-body-999"));

        let mut record = source_record();
        record.message_id = last.message_id.clone();
        record.store_timestamp = 1000;
        *controls.message_record.lock().unwrap() = Some(record);
        let exact = provider
            .query_dlq(
                5,
                DlqQuery {
                    message_id: Some(last.message_id.clone()),
                    ..equal.clone()
                },
            )
            .await
            .unwrap();
        assert_eq!(exact.rows[0].metadata.identity, last);

        let first = DlqQuery { begin: 0, ..equal };
        let first_page = provider.query_dlq(5, first.clone()).await.unwrap();
        assert_eq!(first_page.rows.len(), 2);
        assert_eq!(first_page.extent, MessagePageExtent::MoreMayExist);
        let second = provider.query_dlq(5, first.with_page(2).unwrap()).await.unwrap();
        assert_eq!(second.rows.len(), 1);
        assert_eq!(second.rows[0].metadata.identity, last);
        assert_eq!(second.extent, MessagePageExtent::End);
        let calls = controls.message_queries.lock().unwrap();
        assert!(
            calls
                .iter()
                .all(|request| request.topic.as_deref() == Some("%DLQ%group-a"))
        );
        assert_eq!(calls.last().unwrap().page_num, Some(2));
        assert_eq!(calls.last().unwrap().end, Some(1000));
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
#[test]
fn dlq_provider_preserves_real_paging_discards_payload_and_rejects_conflicting_originals() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    runtime.block_on(async {
        provider.switch(snapshot(5, ConnectionScope::NameServer)).await.unwrap();
        let mut invalid = source();
        invalid.message_id = "unsafe-source".into();
        invalid.properties.remove("RETRY_TOPIC");
        let mut conflict = source();
        conflict.message_id = "conflict-source".into();
        conflict
            .properties
            .insert("DLQ_ORIGIN_MESSAGE_ID".into(), "different".into());
        *controls.message_results.lock().unwrap() = Some(DashboardMessageList {
            items: vec![source(), invalid, conflict],
            total: 9999,
        });
        let page = provider.query_dlq(5, query()).await.unwrap();
        assert_eq!(page.rows.len(), 3);
        assert_eq!(page.extent, MessagePageExtent::End);
        assert!(page.rows[0].target.is_ok() && page.rows[1].target.is_err() && page.rows[2].target.is_err());
        let debug = format!("{page:?}");
        assert!(
            !debug.contains("private-body") && !debug.contains("private-property") && !debug.contains("private-value")
        );
        {
            let calls = controls.message_queries.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].topic.as_deref(), Some("%DLQ%group-a"));
            assert_eq!(calls[0].page_num, Some(3));
            assert_eq!(calls[0].page_size, Some(20));
            assert_eq!(calls[0].begin, Some(0));
            assert_eq!(calls[0].end, Some(1000));
        }
        *controls.message_record.lock().unwrap() = Some(source_record());
        let exact = DlqQuery {
            message_id: Some("producer-id".into()),
            page: 1,
            ..query()
        };
        assert_eq!(provider.query_dlq(5, exact).await.unwrap().rows.len(), 1);
        assert_eq!(
            controls.message_lookups.lock().unwrap().last().unwrap().topic,
            "%DLQ%group-a"
        );
        *controls.message_results.lock().unwrap() = Some(DashboardMessageList {
            items: vec![raw_message("other-topic", "id")],
            total: 1,
        });
        assert!(provider.query_dlq(5, query()).await.is_err());
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
#[test]
fn dlq_provider_revalidates_source_and_canonical_original_before_any_scoped_consume() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    runtime.block_on(async {
        provider.switch(snapshot(5, ConnectionScope::NameServer)).await.unwrap();
        for (code, state) in [
            (Some("CR_SUCCESS"), DlqEntryState::Succeeded),
            (Some("CR_LATER"), DlqEntryState::BusinessFailure),
            (None, DlqEntryState::Unknown),
        ] {
            controls
                .message_records
                .lock()
                .unwrap()
                .extend([source_record(), raw_record("orders", "canonical-physical-original")]);
            *controls.message_consume_result.lock().unwrap() = code.map(|code| DirectConsumeResult {
                success: code == "CR_SUCCESS",
                consume_result: Some(code.into()),
                remark: Some("private-consumer-remark".into()),
            });
            let result = provider.consume_dlq(5, target()).await.unwrap();
            assert_eq!(result.state, state);
            assert!(!format!("{result:?}").contains("private-consumer-remark"));
        }
        {
            let consumed = controls.message_consumes.lock().unwrap();
            assert_eq!(consumed.len(), 3);
            assert!(consumed.iter().all(|request| request.topic == "orders"
                && request.message_id == "canonical-physical-original"
                && request.consumer_group == "group-a"));
        }
        let mut invalid = source_record();
        invalid.properties.remove("RETRY_TOPIC");
        controls.message_records.lock().unwrap().push_back(invalid);
        assert_eq!(
            provider.consume_dlq(5, target()).await.unwrap().state,
            DlqEntryState::CallFailed
        );
        assert_eq!(controls.message_consumes.lock().unwrap().len(), 3);
        controls.message_records.lock().unwrap().extend([
            source_record(),
            raw_record("wrong-topic", "canonical-physical-original"),
        ]);
        assert_eq!(
            provider.consume_dlq(5, target()).await.unwrap().state,
            DlqEntryState::CallFailed
        );
        assert_eq!(controls.message_consumes.lock().unwrap().len(), 3);
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
#[test]
fn dlq_legacy_unique_id_recovery_rechecks_source_and_resolves_only_in_original_topic() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    let mut legacy = source();
    legacy.properties.remove("ORIGIN_MESSAGE_ID");
    legacy
        .properties
        .insert("UNIQ_KEY".into(), "producer-unique-original".into());
    let mut legacy_record = source_record();
    legacy_record.properties = legacy.properties.clone();
    runtime.block_on(async {
        provider.switch(snapshot(5, ConnectionScope::NameServer)).await.unwrap();
        *controls.message_results.lock().unwrap() = Some(DashboardMessageList {
            items: vec![legacy.clone()],
            total: 1,
        });
        let page = provider.query_dlq(5, query()).await.unwrap();
        let target = page.rows[0].target.clone().unwrap();
        assert_eq!(target.original().topic, "orders");
        assert_eq!(target.original().message_id, "producer-unique-original");
        controls.message_records.lock().unwrap().extend([
            legacy_record.clone(),
            raw_record("orders", "canonical-physical-original"),
        ]);
        *controls.message_consume_result.lock().unwrap() = Some(DirectConsumeResult {
            success: true,
            consume_result: Some("CR_SUCCESS".into()),
            remark: None,
        });
        assert_eq!(
            provider.consume_dlq(5, target.clone()).await.unwrap().state,
            DlqEntryState::Succeeded
        );
        {
            let lookups = controls.message_lookups.lock().unwrap();
            assert_eq!(lookups[0].topic, "%DLQ%group-a");
            assert_eq!(lookups[0].message_id, "dlq-source");
            assert_eq!(lookups[1].topic, "orders");
            assert_eq!(lookups[1].message_id, "producer-unique-original");
            let consumed = controls.message_consumes.lock().unwrap();
            assert_eq!(consumed.len(), 1);
            assert_eq!(consumed[0].topic, "orders");
            assert_eq!(consumed[0].message_id, "canonical-physical-original");
            assert_eq!(consumed[0].consumer_group, "group-a");
            assert!(consumed[0].client_id.is_none());
        }
        controls.message_records.lock().unwrap().extend([
            legacy_record.clone(),
            raw_record("other-topic", "canonical-physical-original"),
        ]);
        assert_eq!(
            provider.consume_dlq(5, target.clone()).await.unwrap().state,
            DlqEntryState::CallFailed
        );
        // A frozen selection never substitutes new metadata discovered just before dispatch.
        let mut changed = legacy_record.clone();
        changed
            .properties
            .insert("UNIQ_KEY".into(), "different-original".into());
        let mut blank = legacy_record.clone();
        blank.properties.insert("UNIQ_KEY".into(), "  ".into());
        let mut conflict = legacy_record;
        conflict
            .properties
            .insert("ORIGIN_MESSAGE_ID".into(), "origin-a".into());
        conflict
            .properties
            .insert("DLQ_ORIGIN_MESSAGE_ID".into(), "origin-b".into());
        for record in [changed, blank, conflict] {
            controls.message_records.lock().unwrap().push_back(record);
            assert_eq!(
                provider.consume_dlq(5, target.clone()).await.unwrap().state,
                DlqEntryState::CallFailed
            );
        }
        assert_eq!(controls.message_consumes.lock().unwrap().len(), 1);
        legacy.properties.insert("UNIQ_KEY".into(), "  ".into());
        *controls.message_results.lock().unwrap() = Some(DashboardMessageList {
            items: vec![legacy],
            total: 1,
        });
        assert!(provider.query_dlq(5, query()).await.unwrap().rows[0].target.is_err());
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
#[test]
fn dlq_csv_uses_exact_applied_page_full_values_and_explicit_bounds() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    runtime.block_on(async {
        provider.switch(snapshot(5, ConnectionScope::NameServer)).await.unwrap();
        let body = "完整Body".repeat(50_000);
        let key = "full-key".repeat(1024);
        let mut message = source();
        message.body = body.as_bytes().to_vec();
        message.keys = Some(key.clone());
        *controls.message_results.lock().unwrap() = Some(DashboardMessageList {
            items: vec![message.clone()],
            total: 999,
        });
        let page = provider.query_dlq(5, query()).await.unwrap();
        assert!(page.rows[0].metadata.keys.as_ref().unwrap().truncated);
        let expected = page
            .rows
            .iter()
            .map(|row| row.metadata.identity.clone())
            .collect::<Vec<_>>();
        let csv = provider.dlq_csv(5, query(), expected.clone()).await.unwrap();
        let text = std::str::from_utf8(csv.as_bytes()).unwrap();
        assert!(text.contains(&body) && text.contains(&key));
        assert!(
            provider
                .dlq_csv(5, query(), Vec::new())
                .await
                .unwrap_err()
                .summary()
                .contains("changed")
        );
        message.body = vec![b'x'; DLQ_CSV_MAX_BYTES + 1];
        *controls.message_results.lock().unwrap() = Some(DashboardMessageList {
            items: vec![message.clone()],
            total: 1,
        });
        assert!(
            provider
                .dlq_csv(5, query(), expected.clone())
                .await
                .unwrap_err()
                .summary()
                .contains("Reduce")
        );
        message.body = vec![255];
        *controls.message_results.lock().unwrap() = Some(DashboardMessageList {
            items: vec![message],
            total: 1,
        });
        assert!(
            provider
                .dlq_csv(5, query(), expected)
                .await
                .unwrap_err()
                .summary()
                .contains("non-UTF-8")
        );
        let calls = controls.message_queries.lock().unwrap();
        assert_eq!(calls.len(), 5);
        assert!(
            calls
                .iter()
                .all(|request| request.page_num == Some(3) && request.page_size == Some(20))
        );
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
