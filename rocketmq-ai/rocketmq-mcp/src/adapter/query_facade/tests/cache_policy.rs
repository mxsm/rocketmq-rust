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

//! What the facade keeps and for how long: shared inventories, a source that is down, results
//! that are no longer current, and the lifetimes of single query families.

use super::*;
use crate::infrastructure::cache::STALE_CACHE_VALUE_WARNING;
use crate::tools::config_tools::BrokerConfigSummaryArgs;
use crate::tools::consumer_tools::ListConsumerGroupsArgs;
use crate::tools::message_tools::MessageMetadataArgs;
use crate::tools::proxy_tools::ProxyDrainStateArgs;
use crate::tools::topic_tools::ListTopicsArgs;
use crate::tools::topic_tools::QueryTopicRouteArgs;

fn topics(filter: Option<&str>, limit: u32, cursor: Option<String>) -> ListTopicsArgs {
    ListTopicsArgs {
        cluster: Some("local-dev".to_string()),
        filter: filter.map(str::to_string),
        page: PageRequest {
            limit: Some(limit),
            cursor,
        },
    }
}

fn overview() -> ClusterOverviewArgs {
    ClusterOverviewArgs {
        cluster: "local-dev".to_string(),
    }
}

fn route() -> QueryTopicRouteArgs {
    QueryTopicRouteArgs {
        cluster: "local-dev".to_string(),
        topic: "orders".to_string(),
        page: PageRequest::default(),
    }
}

fn topic_names(listed: &QueryResult<ListTopicsOutput>) -> Vec<&str> {
    listed.page.items.iter().map(|entry| entry.topic.as_str()).collect()
}

#[tokio::test(start_paused = true)]
async fn different_filters_share_one_inventory_load() {
    let factory = FakeSessionFactory {
        many_groups: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let all = facade.list_topics(topics(None, 50, None)).await.unwrap();
    assert_eq!(topic_names(&all), ["orders", "payments"]);
    assert_eq!((all.cache_status, all.freshness_ms), (CacheStatus::Miss, 0));

    // Another filter and another page size are selected from the names already read. They
    // report the age of that read, not of the selection.
    tokio::time::advance(Duration::from_secs(2)).await;
    let filtered = facade.list_topics(topics(Some("PAY"), 50, None)).await.unwrap();
    assert_eq!(topic_names(&filtered), ["payments"]);
    assert_eq!(
        (filtered.cache_status, filtered.freshness_ms),
        (CacheStatus::Hit, 2_000)
    );
    assert_eq!(filtered.observed_at, all.observed_at);
    let paged = facade.list_topics(topics(None, 1, None)).await.unwrap();
    assert_eq!(topic_names(&paged), ["orders"]);
    assert_eq!(paged.freshness_ms, 2_000);
    // So does the overview, which only counts them.
    assert_eq!(facade.cluster_overview(overview()).await.unwrap().topic_count, 2);
    assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 1);

    // A cursor stays bound to the filter and the page size of the request that returned it.
    let cursor = paged.page.next_cursor.clone();
    assert!(cursor.is_some());
    let next = facade.list_topics(topics(None, 1, cursor.clone())).await.unwrap();
    assert_eq!(topic_names(&next), ["payments"]);
    for mismatched in [topics(None, 50, cursor.clone()), topics(Some("pay"), 1, cursor)] {
        assert_eq!(
            facade.list_topics(mismatched).await.unwrap_err().code(),
            "cursor_invalid"
        );
    }
    assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 1);

    // The selections are as current as the names: all of them are read again together.
    tokio::time::advance(Duration::from_secs(3)).await;
    let reloaded = facade.list_topics(topics(Some("pay"), 50, None)).await.unwrap();
    assert_eq!((reloaded.cache_status, reloaded.freshness_ms), (CacheStatus::Miss, 0));
    assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 2);

    // The Consumer Group inventory is kept the same way, for a literal filter and an exact name.
    let group_reads = counters.consumer_group_inventory_queries.load(Ordering::SeqCst);
    let groups = |filter: &str| ListConsumerGroupsArgs {
        cluster: Some("local-dev".to_string()),
        filter: Some(filter.to_string()),
        page: PageRequest::default(),
    };
    assert_eq!(
        facade
            .list_consumer_groups(groups("group-1"))
            .await
            .unwrap()
            .page
            .total_count,
        1
    );
    assert_eq!(
        facade
            .list_consumer_groups(groups("group"))
            .await
            .unwrap()
            .page
            .total_count,
        5
    );
    let exact = facade
        .describe_consumer_group("local-dev".to_string(), "group-3".to_string())
        .await
        .unwrap();
    assert_eq!(exact.group, "group-3");
    assert_eq!(
        counters.consumer_group_inventory_queries.load(Ordering::SeqCst),
        group_reads + 1
    );
}

#[tokio::test]
async fn concurrent_requests_with_different_filters_share_one_inventory_load() {
    let gate = Arc::new(TopicInventoryGate::new(1));
    gate.arm();
    let factory = FakeSessionFactory {
        topic_inventory_gate: Some(gate.clone()),
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let mut tasks = tokio::task::JoinSet::new();
    for (filter, limit) in [(None, 50), (Some("orders"), 50), (Some("pay"), 10), (None, 1)] {
        let facade = facade.clone();
        tasks.spawn(async move {
            let listed = facade.list_topics(topics(filter, limit, None)).await.unwrap();
            (listed.page.total_count, listed.cache_status == CacheStatus::Miss)
        });
    }
    wait_for_atomic_count(&gate.entered, 1).await;
    for _ in 0..10_000 {
        if facade.cache_metrics().coalesced_waiters == 3 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(facade.cache_metrics().coalesced_waiters, 3);
    gate.release().await;

    let mut results = Vec::new();
    while let Some(result) = tasks.join_next().await {
        results.push(result.unwrap());
    }
    results.sort();
    // One request loaded the names; the other three waited for it and selected from them.
    assert_eq!(results, [(1, false), (1, false), (2, false), (2, true)]);
    assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_source_that_is_down_is_asked_once_a_second_for_each_result() {
    let factory = FakeSessionFactory::default();
    let (counters, source_down) = (factory.counters.clone(), factory.source_down.clone());
    let facade = QueryFacade::with_factory(example_config(), factory);
    let describe = || async {
        let args = DescribeBrokerArgs {
            cluster: "local-dev".to_string(),
            broker_name: "broker-a".to_string(),
        };
        let described = facade.describe_broker(args).await;
        described
            .map(|broker| broker.cache_status)
            .map_err(|error| error.code())
    };
    let refused = || counters.refused_starts.load(Ordering::SeqCst);

    source_down.store(true, Ordering::SeqCst);
    assert_eq!(describe().await, Err("source_unavailable"));
    tokio::time::advance(Duration::from_millis(999)).await;
    assert_eq!(describe().await, Err("source_unavailable"));
    assert_eq!(refused(), 1, "the kept failure answers without asking the source");

    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(describe().await, Err("source_unavailable"));
    assert_eq!(refused(), 2);

    // The source is back. The kept failure answers for the rest of its second, then the next
    // call reads the source.
    source_down.store(false, Ordering::SeqCst);
    assert_eq!(describe().await, Err("source_unavailable"));
    assert_eq!(counters.starts.load(Ordering::SeqCst), 0);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(describe().await, Ok(CacheStatus::Miss));
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(refused(), 2);
}

#[tokio::test(start_paused = true)]
async fn stale_results_answer_for_a_source_that_is_down_when_configured() {
    let mut config = example_config();
    config.cache.stale_if_error_ms = 30_000;
    let factory = FakeSessionFactory::default();
    let source_down = factory.source_down.clone();
    let facade = QueryFacade::with_factory(config, factory);

    let fresh_overview = facade.cluster_overview(overview()).await.unwrap();
    let fresh_topics = facade.list_topics(topics(None, 50, None)).await.unwrap();
    let fresh_route = facade.query_topic_route(route()).await.unwrap();
    assert!(fresh_overview.warnings.is_empty() && fresh_topics.warnings.is_empty() && fresh_route.warnings.is_empty());

    // Everything is past its lifetime, and the cluster cannot be reached.
    source_down.store(true, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(10)).await;

    let stale_overview = facade.cluster_overview(overview()).await.unwrap();
    assert_eq!(stale_overview.warnings, [STALE_CACHE_VALUE_WARNING]);
    assert_eq!(
        (stale_overview.cache_status, stale_overview.freshness_ms),
        (CacheStatus::Hit, 10_000)
    );
    assert_eq!(stale_overview.observed_at, fresh_overview.observed_at);
    assert_eq!(stale_overview.data.topic_count, 2);
    assert!(!stale_overview.partial);

    // A filter nobody asked for before is selected from the names that were kept.
    let stale_topics = facade.list_topics(topics(Some("pay"), 50, None)).await.unwrap();
    assert_eq!(topic_names(&stale_topics), ["payments"]);
    assert_eq!(stale_topics.warnings, [STALE_CACHE_VALUE_WARNING]);
    assert_eq!(
        (stale_topics.cache_status, stale_topics.freshness_ms),
        (CacheStatus::Hit, 10_000)
    );
    assert_eq!(stale_topics.observed_at, fresh_topics.observed_at);

    // A paged detail is answered by the snapshot of the same request.
    let stale_route = facade.query_topic_route(route()).await.unwrap();
    assert_eq!(stale_route.warnings, [STALE_CACHE_VALUE_WARNING]);
    assert_eq!(
        (stale_route.cache_status, stale_route.freshness_ms),
        (CacheStatus::Hit, 10_000)
    );
    assert_eq!(stale_route.data.brokers, fresh_route.data.brokers);
    assert_eq!(stale_route.data.page.items, fresh_route.data.page.items);

    // Past the window, the failure is the answer.
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(
        facade.cluster_overview(overview()).await.unwrap_err().code(),
        "source_unavailable"
    );
    assert_eq!(
        facade.list_topics(topics(None, 50, None)).await.unwrap_err().code(),
        "source_unavailable"
    );
    assert_eq!(
        facade.query_topic_route(route()).await.unwrap_err().code(),
        "source_unavailable"
    );

    // The source is back: results are current again, and unmarked.
    source_down.store(false, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(1)).await;
    let recovered = facade.cluster_overview(overview()).await.unwrap();
    assert_eq!(
        (recovered.cache_status, recovered.warnings.len()),
        (CacheStatus::Miss, 0)
    );
    let recovered = facade.list_topics(topics(None, 50, None)).await.unwrap();
    assert_eq!(
        (recovered.cache_status, recovered.warnings.len()),
        (CacheStatus::Hit, 0)
    );
}

#[tokio::test(start_paused = true)]
async fn results_that_are_no_longer_current_are_not_served_by_default() {
    let factory = FakeSessionFactory::default();
    let source_down = factory.source_down.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);
    facade.cluster_overview(overview()).await.unwrap();
    facade.list_topics(topics(None, 50, None)).await.unwrap();
    facade.query_topic_route(route()).await.unwrap();

    source_down.store(true, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(10)).await;

    assert_eq!(
        facade.cluster_overview(overview()).await.unwrap_err().code(),
        "source_unavailable"
    );
    assert_eq!(
        facade.list_topics(topics(None, 50, None)).await.unwrap_err().code(),
        "source_unavailable"
    );
    assert_eq!(
        facade.query_topic_route(route()).await.unwrap_err().code(),
        "source_unavailable"
    );
}

#[tokio::test(start_paused = true)]
async fn broker_config_message_metadata_and_proxy_state_have_lifetimes_of_their_own() {
    let mut config = example_config();
    let shared = Duration::from_millis(config.cache.broker_metrics_ttl_ms);
    config.cache.broker_config_ttl_ms = Some(60_000);
    config.cache.message_metadata_ttl_ms = Some(30_000);
    config.cache.proxy_state_ttl_ms = Some(0);
    let factory = FakeSessionFactory::default();
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(config, factory);
    let broker_config = || BrokerConfigSummaryArgs {
        cluster: "local-dev".to_string(),
        broker_name: "broker-a".to_string(),
    };
    let message = || MessageMetadataArgs {
        cluster: "local-dev".to_string(),
        message_id: "RAW-MESSAGE-ID".to_string(),
    };
    let proxy = || ProxyDrainStateArgs {
        cluster: "local-dev".to_string(),
        proxy_name: "proxy-local".to_string(),
    };
    let broker = || DescribeBrokerArgs {
        cluster: "local-dev".to_string(),
        broker_name: "broker-a".to_string(),
    };

    for _ in 0..2 {
        facade.broker_config_summary(broker_config()).await.unwrap();
        facade.message_metadata(message()).await.unwrap();
        facade.proxy_drain_state(proxy()).await.unwrap();
        facade.describe_broker(broker()).await.unwrap();
        // Past the lifetime the other Broker reads share, inside the two that were set longer.
        tokio::time::advance(shared + Duration::from_millis(1)).await;
    }

    assert_eq!(counters.broker_config_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.message_metadata_queries.load(Ordering::SeqCst), 1);
    // A lifetime of zero keeps nothing.
    assert_eq!(counters.proxy_drain_queries.load(Ordering::SeqCst), 2);
    assert_eq!(counters.broker_queries.load(Ordering::SeqCst), 2);
}
