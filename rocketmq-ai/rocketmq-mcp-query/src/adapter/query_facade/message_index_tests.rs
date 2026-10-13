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

use super::super::observation_fixture::config;
use super::super::observation_fixture::StubFactory;
use super::*;
use crate::adapter::admin_session::SessionMessageKeyMatch;
use crate::adapter::admin_session::SessionMessageMetadata;
use crate::adapter::admin_session::SessionMessageTraceRecord;
use crate::tools::message_tools::MessageTraceStep;

const KEY: &[u8] = b"message-index-test-key";
const UNIQUE_ID: &str = "7F0000010000B4AAC21828A0B9B50000";
const OFFSET_ID: &str = "0A00000100002A9F0000000000001234";
const CLIENT_HOST: &str = "10.0.0.7";
const BUSINESS_KEY: &str = "order-42";

fn aliases() -> IdentifierAliaser {
    IdentifierAliaser::with_key(KEY).unwrap()
}

fn facade(factory: &StubFactory, visibility: VisibilityClass) -> QueryFacade<StubFactory> {
    QueryFacade::with_factory(config(), factory.clone())
        .with_identifier_aliases(aliases())
        .with_visibility_class(visibility)
}

fn record(step: MessageTraceStep, timestamp: i64) -> SessionMessageTraceRecord {
    SessionMessageTraceRecord {
        step,
        timestamp,
        group: Some("producers".to_string()),
        cost_time_ms: 12,
        success: true,
        topic: Some("orders".to_string()),
        offset_message_id: (step == MessageTraceStep::Publish).then(|| OFFSET_ID.to_string()),
        tags: Some("TagA".to_string()),
        keys: vec![BUSINESS_KEY.to_string()],
        store_broker_name: Some("broker-a".to_string()),
        client_host: Some(CLIENT_HOST.to_string()),
        retry_times: 0,
        body_length: 128,
        from_transaction_check: false,
    }
}

fn trace(unanswered: &[&str], truncated: bool) -> SessionMessageTrace {
    SessionMessageTrace {
        trace_topic: "RMQ_SYS_TRACE_TOPIC".to_string(),
        trace_topic_found: true,
        records: vec![
            record(MessageTraceStep::Publish, 1_700_000_000_000),
            record(MessageTraceStep::ConsumeStart, 0),
        ],
        truncated,
        queried_broker_count: 2,
        unanswered_brokers: unanswered.iter().map(|name| (*name).to_string()).collect(),
    }
}

fn trace_args() -> QueryMessageTraceArgs {
    QueryMessageTraceArgs {
        cluster: "local-dev".to_string(),
        message_id: format!(" {UNIQUE_ID} "),
        trace_topic: None,
    }
}

#[tokio::test]
async fn a_trace_names_the_message_by_pseudonyms_and_details_only_for_a_caller_that_may_diagnose() {
    let factory = StubFactory {
        message_trace: Some(trace(&[], false)),
        ..Default::default()
    };

    let detailed = facade(&factory, VisibilityClass::Sensitive)
        .message_trace(trace_args())
        .await
        .unwrap();
    let data = &detailed.data;
    assert_eq!(
        data.unique_message_alias,
        aliases().unique_message_alias(UNIQUE_ID).unwrap()
    );
    assert_eq!(data.message_alias, Some(aliases().message_alias(OFFSET_ID).unwrap()));
    assert!(data.trace_topic_found && !data.redacted && !data.truncated);
    let published = &data.records[0];
    assert_eq!(published.step, MessageTraceStep::Publish);
    assert_eq!(published.at.as_deref(), Some("2023-11-14T22:13:20.000Z"));
    assert_eq!(published.store_broker_name.as_deref(), Some("broker-a"));
    assert_eq!(
        published.client_host_alias,
        Some(aliases().host_alias(CLIENT_HOST).unwrap())
    );
    assert_eq!(published.tags.as_deref(), Some("TagA"));
    assert_eq!(published.key_aliases, [aliases().key_alias(BUSINESS_KEY).unwrap()]);
    // A record without a time has none, instead of the year 1970.
    assert_eq!(data.records[1].at, None);
    assert!(!detailed.partial);

    let reduced = facade(&factory, VisibilityClass::Standard)
        .message_trace(trace_args())
        .await
        .unwrap();
    assert!(reduced.data.redacted);
    for record in &reduced.data.records {
        assert_eq!(
            (&record.client_host_alias, &record.tags, record.key_aliases.len()),
            (&None, &None, 0)
        );
        // The step, the time, the group and the logical Broker are not what a record hides.
        assert_eq!(record.store_broker_name.as_deref(), Some("broker-a"));
        assert_eq!(record.group.as_deref(), Some("producers"));
    }

    // Neither answer holds an identifier, a key or a host as the session read it.
    for answer in [&detailed.data, &reduced.data] {
        let published = serde_json::to_string(answer).unwrap();
        for private in [UNIQUE_ID, OFFSET_ID, CLIENT_HOST, BUSINESS_KEY] {
            assert!(!published.contains(private), "the answer holds {private}");
        }
    }
    assert_eq!(
        *factory.asked_messages.lock().unwrap(),
        [UNIQUE_ID.to_string(), UNIQUE_ID.to_string()]
    );
}

#[tokio::test]
async fn a_trace_is_kept_per_visibility_class_and_says_what_it_misses() {
    let factory = StubFactory {
        message_trace: Some(trace(&["broker-b"], true)),
        ..Default::default()
    };
    let detailed = facade(&factory, VisibilityClass::Sensitive);

    let first = detailed.message_trace(trace_args()).await.unwrap();
    let second = detailed.message_trace(trace_args()).await.unwrap();
    assert_eq!(
        (first.cache_status, second.cache_status),
        (CacheStatus::Miss, CacheStatus::Hit)
    );
    assert_eq!(factory.used(), (1, 1, 1));
    assert!(first.partial && first.data.truncated);
    assert_eq!(
        first.warnings,
        ["message_index_brokers_unanswered", "message_trace_records_truncated"]
    );
    assert_eq!(first.data.unanswered_brokers, ["broker-b"]);
    assert!(first.source_failures.is_empty());

    // A cluster without a trace Topic is an answer too, and a blank or split identifier is not.
    let untraced = StubFactory {
        message_trace: Some(SessionMessageTrace {
            trace_topic: "RMQ_SYS_TRACE_TOPIC".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let facade = facade(&untraced, VisibilityClass::Standard);
    let result = facade.message_trace(trace_args()).await.unwrap();
    assert!(!result.data.trace_topic_found && result.data.records.is_empty() && !result.partial);
    for message_id in [" ", "two ids", "id\u{0}"] {
        let mut rejected = trace_args();
        rejected.message_id = message_id.to_string();
        assert!(facade.message_trace(rejected).await.is_err(), "{message_id:?}");
    }
    assert_eq!(untraced.used(), (1, 1, 1));
}

fn found(queue_offset: i64) -> SessionMessageKeyMatch {
    SessionMessageKeyMatch {
        broker_name: "broker-a".to_string(),
        metadata: SessionMessageMetadata {
            message_id: OFFSET_ID.to_string(),
            unique_message_id: Some(UNIQUE_ID.to_string()),
            topic: "orders".to_string(),
            born_timestamp: 1_700_000_000_000,
            store_timestamp: 1_700_000_000_050,
            queue_id: 3,
            queue_offset,
            store_size: 128,
            reconsume_times: 0,
            sys_flag: 0,
            flag: 0,
            prepared_transaction_offset: 0,
        },
    }
}

fn search_args() -> FindMessagesByKeyArgs {
    FindMessagesByKeyArgs {
        cluster: "local-dev".to_string(),
        topic: "orders".to_string(),
        key: BUSINESS_KEY.to_string(),
        end_at: Some("2023-11-14T22:13:21Z".to_string()),
        window_minutes: Some(5),
    }
}

#[tokio::test]
async fn a_key_search_covers_the_asked_window_and_answers_with_pseudonyms() {
    let factory = StubFactory {
        message_key_matches: Some(SessionMessageKeyMatches {
            matches: vec![found(42)],
            truncated: false,
            queried_broker_count: 2,
            unanswered_brokers: Vec::new(),
        }),
        ..Default::default()
    };
    let facade = facade(&factory, VisibilityClass::Standard);

    let first = facade.find_messages_by_key(search_args()).await.unwrap();
    let second = facade.find_messages_by_key(search_args()).await.unwrap();

    let data = &first.data;
    assert_eq!((data.cluster.as_str(), data.topic.as_str()), ("local-dev", "orders"));
    assert_eq!(data.window_end_at, "2023-11-14T22:13:21.000Z");
    assert_eq!(data.window_begin_at, "2023-11-14T22:08:21.000Z");
    let message = &data.messages[0];
    assert_eq!(message.message_alias, aliases().message_alias(OFFSET_ID).unwrap());
    assert_eq!(
        message.unique_message_alias,
        Some(aliases().unique_message_alias(UNIQUE_ID).unwrap())
    );
    assert_eq!(
        (message.broker_name.as_str(), message.queue_id, message.queue_offset),
        ("broker-a", 3, 42)
    );
    assert_eq!(message.stored_at.as_deref(), Some("2023-11-14T22:13:20.050Z"));
    assert!(!first.partial && !data.truncated);
    // The answer names no identifier and does not repeat the key.
    let published = serde_json::to_string(data).unwrap();
    for private in [UNIQUE_ID, OFFSET_ID, BUSINESS_KEY] {
        assert!(!published.contains(private), "the answer holds {private}");
    }

    // Every search is read: an old answer would hide the newest matches.
    assert_eq!(
        (first.cache_status, second.cache_status),
        (CacheStatus::Bypass, CacheStatus::Bypass)
    );
    assert_eq!(factory.used(), (2, 2, 2));
    let end = 1_700_000_001_000;
    assert_eq!(
        factory.asked_searches.lock().unwrap()[0],
        ("orders".to_string(), BUSINESS_KEY.to_string(), end - 5 * 60_000, end)
    );
}

#[tokio::test]
async fn a_key_search_says_what_it_misses_and_refuses_what_is_no_bounded_search() {
    let factory = StubFactory {
        message_key_matches: Some(SessionMessageKeyMatches {
            matches: vec![found(1)],
            truncated: true,
            queried_broker_count: 2,
            unanswered_brokers: vec!["broker-b".to_string()],
        }),
        ..Default::default()
    };
    let facade = facade(&factory, VisibilityClass::Standard);

    let result = facade.find_messages_by_key(search_args()).await.unwrap();
    assert!(result.partial && result.data.truncated);
    assert_eq!(
        result.warnings,
        ["message_index_brokers_unanswered", "message_key_matches_truncated"]
    );
    assert_eq!(result.data.unanswered_brokers, ["broker-b"]);
    assert_eq!(factory.used(), (1, 1, 1));

    let change = |edit: fn(&mut FindMessagesByKeyArgs)| {
        let mut args = search_args();
        edit(&mut args);
        args
    };
    for (name, args) in [
        ("a key with a space", change(|args| args.key = "order 42".to_string())),
        ("a blank key", change(|args| args.key = " ".to_string())),
        ("a key that is too long", change(|args| args.key = "k".repeat(256))),
        ("no window", change(|args| args.window_minutes = Some(0))),
        ("more than a day", change(|args| args.window_minutes = Some(1_441))),
        (
            "a time that is no timestamp",
            change(|args| args.end_at = Some("yesterday".to_string())),
        ),
        (
            "a window before the epoch",
            change(|args| args.end_at = Some("1970-01-01T00:01:00Z".to_string())),
        ),
        ("a blank Topic", change(|args| args.topic = " ".to_string())),
    ] {
        assert!(facade.find_messages_by_key(args).await.is_err(), "{name}");
    }
    // Nothing of that reached a session.
    assert_eq!(factory.used(), (1, 1, 1));
}

#[test]
fn a_window_ends_now_and_is_an_hour_long_unless_it_is_asked_otherwise() {
    let now = 1_700_000_000_000;
    assert_eq!(
        SearchWindow::resolve(None, None, now).unwrap(),
        SearchWindow {
            begin: now - 60 * 60_000,
            end: now
        }
    );
    assert_eq!(
        SearchWindow::resolve(Some("2023-11-14T22:13:20+00:00"), Some(1_440), 0).unwrap(),
        SearchWindow {
            begin: now - 24 * 60 * 60_000,
            end: now
        }
    );
    // An offset is honoured: the same instant, written in another zone.
    assert_eq!(
        SearchWindow::resolve(Some("2023-11-15T06:13:20+08:00"), Some(1), 0)
            .unwrap()
            .end,
        now
    );
}
