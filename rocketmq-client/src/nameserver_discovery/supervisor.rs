// Copyright 2023 The RocketMQ Rust Authors
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

#[cfg(feature = "nameserver-dns-discovery")]
use std::sync::Arc;
#[cfg(feature = "nameserver-dns-discovery")]
use std::time::Duration;

#[cfg(feature = "nameserver-dns-discovery")]
use arc_swap::ArcSwap;
#[cfg(feature = "nameserver-dns-discovery")]
use rocketmq_error::RocketMQError;
#[cfg(feature = "nameserver-dns-discovery")]
use rocketmq_error::RocketMQResult;
#[cfg(feature = "nameserver-dns-discovery")]
use rocketmq_runtime::ChildServiceContext;
#[cfg(feature = "nameserver-dns-discovery")]
use tokio::time::Instant;
#[cfg(feature = "nameserver-dns-discovery")]
use tracing::warn;

#[cfg(feature = "nameserver-dns-discovery")]
use super::dns::resolve_dns;
#[cfg(feature = "nameserver-dns-discovery")]
use super::dns::DnsLookup;
#[cfg(feature = "nameserver-dns-discovery")]
use super::dns::HickoryDnsLookup;
#[cfg(feature = "nameserver-dns-discovery")]
use super::dns::ResolvedDnsEndpoints;
#[cfg(feature = "nameserver-dns-discovery")]
use super::EndpointSnapshot;
#[cfg(feature = "nameserver-dns-discovery")]
use super::Freshness;
#[cfg(feature = "nameserver-dns-discovery")]
use super::NameServerDiscoveryConfig;
#[cfg(feature = "nameserver-dns-discovery")]
use super::ResolvedNameServerEndpoint;
#[cfg(feature = "nameserver-dns-discovery")]
use crate::runtime::spawn_client_adaptive_task_with_context;
#[cfg(feature = "nameserver-dns-discovery")]
use crate::runtime::ClientAdaptiveTaskControl;
#[cfg(feature = "nameserver-dns-discovery")]
use crate::runtime::ClientAdaptiveTaskHandle;

#[cfg(feature = "nameserver-dns-discovery")]
const RETRY_BASE: Duration = Duration::from_secs(1);
#[cfg(feature = "nameserver-dns-discovery")]
const RETRY_MAX: Duration = Duration::from_secs(60);
#[cfg(feature = "nameserver-dns-discovery")]
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

#[cfg(feature = "nameserver-dns-discovery")]
type EndpointPublisher = Arc<dyn Fn(Arc<[ResolvedNameServerEndpoint]>) + Send + Sync>;

pub(crate) struct NameServerDiscoverySupervisor {
    #[cfg(feature = "nameserver-dns-discovery")]
    state: Arc<ArcSwap<EndpointSnapshot>>,
    #[cfg(feature = "nameserver-dns-discovery")]
    task_handle: ClientAdaptiveTaskHandle,
}

impl NameServerDiscoverySupervisor {
    #[cfg(feature = "nameserver-dns-discovery")]
    pub(crate) async fn start_dns(
        config: NameServerDiscoveryConfig,
        parent: &ChildServiceContext,
        client_id: &str,
        publish: EndpointPublisher,
    ) -> RocketMQResult<Arc<Self>> {
        let resolver = Arc::new(HickoryDnsLookup::new().map_err(initial_resolution_error)?);
        Self::start_with_resolver(config, parent, client_id, resolver, publish).await
    }

    #[cfg(feature = "nameserver-dns-discovery")]
    async fn start_with_resolver(
        config: NameServerDiscoveryConfig,
        parent: &ChildServiceContext,
        client_id: &str,
        resolver: Arc<dyn DnsLookup>,
        publish: EndpointPublisher,
    ) -> RocketMQResult<Arc<Self>> {
        let initial = resolve_dns(resolver.as_ref(), &config)
            .await
            .map_err(initial_resolution_error)?;
        let now = Instant::now();
        let initial_snapshot = success_snapshot(&EndpointSnapshot::unavailable(now), initial.clone(), now);
        let initial_endpoints = initial_snapshot.endpoints.clone();
        let state = Arc::new(ArcSwap::from_pointee(initial_snapshot));

        let client_id = Arc::<str>::from(client_id);
        let initial_delay = jittered_refresh(initial.ttl, &client_id);
        let refresh_state = Arc::new(tokio::sync::Mutex::new(RefreshLoopState {
            retry_attempt: 0,
            last_success: now,
        }));
        let task_state = state.clone();
        let refresh_publisher = publish.clone();
        let task_handle =
            spawn_client_adaptive_task_with_context(parent, "nameserver-discovery.refresh", initial_delay, move || {
                let resolver = resolver.clone();
                let config = config.clone();
                let state = task_state.clone();
                let refresh_state = refresh_state.clone();
                let publish = refresh_publisher.clone();
                let client_id = client_id.clone();
                async move {
                    let resolved = resolve_dns(resolver.as_ref(), &config).await;
                    let mut refresh_state = refresh_state.lock().await;
                    let now = Instant::now();
                    match resolved {
                        Ok(resolved) => {
                            let current = state.load_full();
                            let next = success_snapshot(&current, resolved.clone(), now);
                            let changed = next.generation != current.generation;
                            state.store(Arc::new(next));
                            if changed {
                                publish(state.load().endpoints.clone());
                            }
                            refresh_state.last_success = now;
                            refresh_state.retry_attempt = 0;
                            ClientAdaptiveTaskControl::ContinueAfter(jittered_refresh(resolved.ttl, &client_id))
                        }
                        Err(error) => {
                            let current = state.load_full();
                            let (next, changed) =
                                failure_snapshot(&current, now, refresh_state.last_success, config.stale_max());
                            let generation = next.generation;
                            state.store(Arc::new(next));
                            if changed {
                                publish(state.load().endpoints.clone());
                            }
                            warn!(
                                error_kind = ?error.kind(),
                                generation,
                                "NameServer DNS refresh failed; retained bounded last-known-good state"
                            );
                            let delay = retry_delay(&client_id, refresh_state.retry_attempt);
                            refresh_state.retry_attempt = refresh_state.retry_attempt.saturating_add(1);
                            ClientAdaptiveTaskControl::ContinueAfter(delay)
                        }
                    }
                }
            })
            .map_err(|error| RocketMQError::internal("spawn NameServer discovery refresh task", error))?;
        let supervisor = Arc::new(Self { state, task_handle });
        publish(initial_endpoints);

        Ok(supervisor)
    }

    #[cfg(feature = "nameserver-dns-discovery")]
    pub(crate) fn snapshot(&self) -> Arc<EndpointSnapshot> {
        self.state.load_full()
    }

    #[cfg(feature = "nameserver-dns-discovery")]
    pub(crate) fn task_count(&self) -> usize {
        self.task_handle.task_count()
    }

    pub(crate) async fn shutdown(&self) {
        #[cfg(feature = "nameserver-dns-discovery")]
        {
            let report = self.task_handle.shutdown(SHUTDOWN_TIMEOUT).await;
            if !report.is_healthy() {
                warn!(report = %report.to_json(), "NameServer discovery shutdown was unhealthy");
            }
        }
    }
}

#[cfg(feature = "nameserver-dns-discovery")]
struct RefreshLoopState {
    retry_attempt: u32,
    last_success: Instant,
}

#[cfg(feature = "nameserver-dns-discovery")]
fn initial_resolution_error(error: super::dns::DnsResolutionError) -> RocketMQError {
    RocketMQError::ConfigInvalidValue {
        key: "nameserver_discovery.dns",
        value: "initial lookup".to_string(),
        reason: error.to_string(),
    }
}

#[cfg(feature = "nameserver-dns-discovery")]
fn success_snapshot(current: &EndpointSnapshot, resolved: ResolvedDnsEndpoints, now: Instant) -> EndpointSnapshot {
    let generation = if current.same_endpoint_set(&resolved.endpoints) {
        current.generation
    } else {
        current.generation.saturating_add(1)
    };
    EndpointSnapshot {
        generation,
        resolved_at: now,
        valid_until: now + resolved.ttl,
        freshness: Freshness::Fresh,
        endpoints: Arc::from(resolved.endpoints),
    }
}

#[cfg(feature = "nameserver-dns-discovery")]
fn failure_snapshot(
    current: &EndpointSnapshot,
    now: Instant,
    last_success: Instant,
    stale_max: Duration,
) -> (EndpointSnapshot, bool) {
    if !current.endpoints.is_empty() && now.saturating_duration_since(last_success) <= stale_max {
        return (
            EndpointSnapshot {
                generation: current.generation,
                resolved_at: current.resolved_at,
                valid_until: current.valid_until,
                freshness: Freshness::Stale,
                endpoints: current.endpoints.clone(),
            },
            false,
        );
    }

    let changed = !current.endpoints.is_empty();
    (
        EndpointSnapshot {
            generation: current.generation.saturating_add(u64::from(changed)),
            resolved_at: current.resolved_at,
            valid_until: current.valid_until,
            freshness: Freshness::Unavailable,
            endpoints: Arc::from([]),
        },
        changed,
    )
}

#[cfg(feature = "nameserver-dns-discovery")]
fn jittered_refresh(ttl: Duration, client_id: &str) -> Duration {
    let permille = 800_u128 + u128::from(stable_hash(client_id.as_bytes(), 0) % 201);
    let nanos = ttl.as_nanos().saturating_mul(permille) / 1_000;
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

#[cfg(feature = "nameserver-dns-discovery")]
fn retry_delay(client_id: &str, attempt: u32) -> Duration {
    let multiplier = 1_u32 << attempt.min(6);
    let ceiling = RETRY_BASE.saturating_mul(multiplier).min(RETRY_MAX);
    let ceiling_millis = u64::try_from(ceiling.as_millis()).unwrap_or(u64::MAX);
    let jitter_millis = stable_hash(client_id.as_bytes(), attempt.saturating_add(1)) % ceiling_millis.saturating_add(1);
    Duration::from_millis(jitter_millis.max(1))
}

#[cfg(feature = "nameserver-dns-discovery")]
fn stable_hash(value: &[u8], discriminator: u32) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.iter().copied().chain(discriminator.to_le_bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

#[cfg(all(feature = "nameserver-dns-discovery", any(test, feature = "test-support")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameServerDiscoveryLifecycleProbe {
    pub initial_generation: u64,
    pub initial_fresh: bool,
    pub initial_task_count: usize,
    pub publish_count: usize,
    pub task_count_after_shutdown: usize,
}

#[cfg(all(feature = "nameserver-dns-discovery", any(test, feature = "test-support")))]
pub async fn run_nameserver_discovery_lifecycle_probe(
    service_context: ChildServiceContext,
) -> NameServerDiscoveryLifecycleProbe {
    use std::future::pending;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::dns::AddressFamily;
    use super::dns::DnsFamilyResult;
    use super::dns::DnsResolutionError;
    use super::dns::LookupFuture;
    use super::DnsName;
    use super::NameServerSource;

    struct ProbeDnsLookup {
        calls: AtomicUsize,
        refresh_started: tokio::sync::Notify,
    }

    impl DnsLookup for ProbeDnsLookup {
        fn lookup(&self, _host: &DnsName, family: AddressFamily) -> LookupFuture<'_> {
            let call = self.calls.fetch_add(1, Ordering::AcqRel);
            if call < 2 {
                let addresses = match family {
                    AddressFamily::Ipv4 => vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))],
                    AddressFamily::Ipv6 => Vec::new(),
                };
                return Box::pin(async move { Ok(DnsFamilyResult::new(addresses, Duration::from_millis(5))) });
            }
            self.refresh_started.notify_waiters();
            Box::pin(pending::<Result<DnsFamilyResult, DnsResolutionError>>())
        }
    }

    let resolver = Arc::new(ProbeDnsLookup {
        calls: AtomicUsize::new(0),
        refresh_started: tokio::sync::Notify::new(),
    });
    let config = NameServerDiscoveryConfig::new(NameServerSource::dns("namesrv.default.svc", 9876).unwrap())
        .with_refresh_bounds(Duration::from_millis(5), Duration::from_millis(5))
        .unwrap();
    let published = Arc::new(AtomicUsize::new(0));
    let publish_count = published.clone();
    let supervisor = NameServerDiscoverySupervisor::start_with_resolver(
        config,
        &service_context,
        "probe-client",
        resolver.clone(),
        Arc::new(move |_| {
            publish_count.fetch_add(1, Ordering::AcqRel);
        }),
    )
    .await
    .expect("fake NameServer discovery should start");
    let initial = supervisor.snapshot();
    let initial_generation = initial.generation;
    let initial_fresh = initial.freshness == Freshness::Fresh;
    let initial_task_count = supervisor.task_count();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if resolver.calls.load(Ordering::Acquire) > 2 {
                break;
            }
            resolver.refresh_started.notified().await;
        }
    })
    .await
    .expect("fake NameServer refresh should enter its lookup");
    supervisor.shutdown().await;

    NameServerDiscoveryLifecycleProbe {
        initial_generation,
        initial_fresh,
        initial_task_count,
        publish_count: published.load(Ordering::Acquire),
        task_count_after_shutdown: supervisor.task_count(),
    }
}

#[cfg(all(test, feature = "nameserver-dns-discovery"))]
mod tests {
    use std::future::pending;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::nameserver_discovery::dns::AddressFamily;
    use crate::nameserver_discovery::dns::DnsFamilyResult;
    use crate::nameserver_discovery::dns::DnsResolutionError;
    use crate::nameserver_discovery::dns::LookupFuture;
    use crate::nameserver_discovery::DnsName;

    struct BlockingAfterInitialLookup {
        calls: AtomicUsize,
        refresh_started: tokio::sync::Notify,
    }

    impl BlockingAfterInitialLookup {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                refresh_started: tokio::sync::Notify::new(),
            }
        }

        async fn wait_for_refresh(&self) {
            loop {
                if self.calls.load(Ordering::Acquire) > 2 {
                    return;
                }
                self.refresh_started.notified().await;
            }
        }
    }

    impl DnsLookup for BlockingAfterInitialLookup {
        fn lookup(&self, _host: &DnsName, family: AddressFamily) -> LookupFuture<'_> {
            let call = self.calls.fetch_add(1, Ordering::AcqRel);
            if call < 2 {
                let addresses = match family {
                    AddressFamily::Ipv4 => vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))],
                    AddressFamily::Ipv6 => Vec::new(),
                };
                return Box::pin(async move { Ok(DnsFamilyResult::new(addresses, Duration::from_millis(5))) });
            }
            self.refresh_started.notify_waiters();
            Box::pin(pending::<Result<DnsFamilyResult, DnsResolutionError>>())
        }
    }

    struct AlwaysFailingLookup;

    impl DnsLookup for AlwaysFailingLookup {
        fn lookup(&self, _host: &DnsName, _family: AddressFamily) -> LookupFuture<'_> {
            Box::pin(async {
                Err(DnsResolutionError::new(
                    super::super::dns::DnsErrorKind::Empty,
                    "no endpoints",
                ))
            })
        }
    }
    fn endpoint(address: [u8; 4]) -> ResolvedNameServerEndpoint {
        ResolvedNameServerEndpoint::new(
            super::super::NameServerAuthority::parse("namesrv.default.svc:9876").unwrap(),
            (IpAddr::V4(Ipv4Addr::from(address)), 9876).into(),
        )
    }

    #[test]
    fn endpoint_changes_advance_generation_but_reordering_does_not() {
        let now = Instant::now();
        let unavailable = EndpointSnapshot::unavailable(now);
        let first = success_snapshot(
            &unavailable,
            ResolvedDnsEndpoints {
                endpoints: vec![endpoint([10, 0, 0, 1]), endpoint([10, 0, 0, 2])],
                ttl: Duration::from_secs(30),
            },
            now,
        );
        assert_eq!(first.generation, 1);

        let reordered = success_snapshot(
            &first,
            ResolvedDnsEndpoints {
                endpoints: vec![endpoint([10, 0, 0, 2]), endpoint([10, 0, 0, 1])],
                ttl: Duration::from_secs(30),
            },
            now,
        );
        assert_eq!(reordered.generation, 1);

        let same = success_snapshot(
            &first,
            ResolvedDnsEndpoints {
                endpoints: first.endpoints.to_vec(),
                ttl: Duration::from_secs(30),
            },
            now,
        );
        assert_eq!(same.generation, 1);
        assert_eq!(same.freshness, Freshness::Fresh);
    }

    #[test]
    fn transient_failure_becomes_stale_then_unavailable() {
        let now = Instant::now();
        let fresh = EndpointSnapshot {
            generation: 1,
            resolved_at: now,
            valid_until: now + Duration::from_secs(30),
            freshness: Freshness::Fresh,
            endpoints: Arc::from([endpoint([10, 0, 0, 1])]),
        };
        let (stale, changed) = failure_snapshot(&fresh, now + Duration::from_secs(10), now, Duration::from_secs(30));
        assert!(!changed);
        assert_eq!(stale.generation, 1);
        assert_eq!(stale.freshness, Freshness::Stale);

        let (unavailable, changed) =
            failure_snapshot(&stale, now + Duration::from_secs(31), now, Duration::from_secs(30));
        assert!(changed);
        assert_eq!(unavailable.generation, 2);
        assert_eq!(unavailable.freshness, Freshness::Unavailable);
        assert!(unavailable.endpoints.is_empty());
    }

    #[test]
    fn recovery_restores_freshness_without_advancing_an_unchanged_set() {
        let now = Instant::now();
        let stale = EndpointSnapshot {
            generation: 1,
            resolved_at: now,
            valid_until: now,
            freshness: Freshness::Stale,
            endpoints: Arc::from([endpoint([10, 0, 0, 1])]),
        };
        let recovered = success_snapshot(
            &stale,
            ResolvedDnsEndpoints {
                endpoints: stale.endpoints.to_vec(),
                ttl: Duration::from_secs(30),
            },
            now + Duration::from_secs(10),
        );
        assert_eq!(recovered.generation, 1);
        assert_eq!(recovered.freshness, Freshness::Fresh);
        assert_eq!(recovered.resolved_at, now + Duration::from_secs(10));
    }

    #[test]
    fn refresh_and_retry_jitter_are_deterministic_and_client_specific() {
        let ttl = Duration::from_secs(30);
        let first = jittered_refresh(ttl, "client-a");
        assert_eq!(first, jittered_refresh(ttl, "client-a"));
        assert!((Duration::from_secs(24)..=Duration::from_secs(30)).contains(&first));
        assert_ne!(first, jittered_refresh(ttl, "client-b"));

        let retry = retry_delay("client-a", 20);
        assert!(retry <= RETRY_MAX);
        assert!(!retry.is_zero());
    }

    #[tokio::test]
    async fn shutdown_cancels_an_inflight_lookup_and_joins_the_refresh_task() {
        let resolver = Arc::new(BlockingAfterInitialLookup::new());
        let config =
            NameServerDiscoveryConfig::new(super::super::NameServerSource::dns("namesrv.default.svc", 9876).unwrap())
                .with_refresh_bounds(Duration::from_millis(5), Duration::from_millis(5))
                .unwrap();
        let published = Arc::new(AtomicUsize::new(0));
        let publish_count = published.clone();
        let supervisor = NameServerDiscoverySupervisor::start_with_resolver(
            config,
            &crate::runtime::test_service_context("nameserver-discovery-shutdown-test"),
            "client-a",
            resolver.clone(),
            Arc::new(move |_| {
                publish_count.fetch_add(1, Ordering::AcqRel);
            }),
        )
        .await
        .unwrap();

        assert_eq!(supervisor.snapshot().generation, 1);
        assert_eq!(supervisor.snapshot().freshness, Freshness::Fresh);
        assert_eq!(supervisor.task_count(), 1);
        tokio::time::timeout(Duration::from_secs(1), resolver.wait_for_refresh())
            .await
            .expect("refresh lookup should start");

        supervisor.shutdown().await;
        assert_eq!(supervisor.task_count(), 0);
        assert_eq!(published.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn initial_dns_failure_prevents_start_without_spawning_a_task() {
        let parent = crate::runtime::test_service_context("nameserver-discovery-initial-failure-test");
        let initial_components = parent.task_group().component_count();
        let config =
            NameServerDiscoveryConfig::new(super::super::NameServerSource::dns("namesrv.default.svc", 9876).unwrap());
        let result = NameServerDiscoverySupervisor::start_with_resolver(
            config,
            &parent,
            "client-a",
            Arc::new(AlwaysFailingLookup),
            Arc::new(|_| {}),
        )
        .await;

        assert!(matches!(result, Err(RocketMQError::ConfigInvalidValue { .. })));
        assert_eq!(parent.task_group().component_count(), initial_components);
    }
}
