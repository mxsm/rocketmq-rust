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

use std::any::Any;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_auth::cal_signature_segments_with_algorithm;
use rocketmq_auth::cal_signature_with_algorithm;
use rocketmq_auth::Acl;
use rocketmq_auth::AclAuthorizationHandler;
use rocketmq_auth::AuthConfig;
use rocketmq_auth::AuthenticationProvider;
use rocketmq_auth::AuthenticationStrategy;
use rocketmq_auth::AuthorizationHandler;
use rocketmq_auth::AuthorizationMetadataProvider;
use rocketmq_auth::AuthorizationStrategy;
use rocketmq_auth::DefaultAuthenticationContext;
use rocketmq_auth::DefaultAuthorizationContext;
use rocketmq_auth::LocalAuthorizationMetadataProvider;
use rocketmq_auth::Policy;
use rocketmq_auth::PolicyDecision;
use rocketmq_auth::PolicyEntry;
use rocketmq_auth::PolicyResource;
use rocketmq_auth::PolicyType;
use rocketmq_auth::SignatureAlgorithm;
use rocketmq_auth::StatefulAuthenticationStrategy;
use rocketmq_auth::StatefulAuthorizationStrategy;
use rocketmq_auth::WhiteList;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_security_api::Action;
use rocketmq_security_api::ResourcePattern;
use rocketmq_security_api::ResourceType;

fn bench_signature(c: &mut Criterion) {
    let mut group = c.benchmark_group("auth_signature");
    let content = b"aliceTopicAqueueId0clientIdCID-123body-bytes";
    let content_segments: [&[u8]; 6] = [
        b"alice".as_slice(),
        b"TopicA".as_slice(),
        b"queueId0".as_slice(),
        b"clientId".as_slice(),
        b"CID-123".as_slice(),
        b"body-bytes".as_slice(),
    ];
    let secret = "benchmark-secret";

    for algorithm in [
        SignatureAlgorithm::HmacSha1,
        SignatureAlgorithm::HmacSha256,
        SignatureAlgorithm::HmacMd5,
    ] {
        group.bench_with_input(
            BenchmarkId::new(algorithm.java_name(), "contiguous"),
            &algorithm,
            |b, algorithm| {
                b.iter(|| {
                    cal_signature_with_algorithm(black_box(content), black_box(secret), black_box(*algorithm))
                        .expect("benchmark signature should calculate")
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new(algorithm.java_name(), "segmented"),
            &algorithm,
            |b, algorithm| {
                b.iter(|| {
                    cal_signature_segments_with_algorithm(
                        black_box(content_segments),
                        black_box(secret),
                        black_box(*algorithm),
                    )
                    .expect("benchmark segmented signature should calculate")
                })
            },
        );
    }

    group.finish();
}

fn bench_white_list(c: &mut Criterion) {
    let white_list = WhiteList::from_global_patterns([
        "10.10.*.*",
        "192.168.1.{1,2,3}",
        "172.16.0.*;172.16.1.*",
        "2001:db8::{1,2}",
    ]);

    let mut group = c.benchmark_group("auth_white_list");
    for (name, source_ip) in [
        ("global_ipv4_wildcard_hit", "10.10.8.9"),
        ("global_ipv4_brace_hit", "192.168.1.2"),
        ("global_ipv4_miss", "192.168.2.2"),
        ("global_ipv6_brace_hit", "2001:db8::2"),
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(name), source_ip, |b, source_ip| {
            b.iter(|| white_list.matches(None, Some(black_box(source_ip))))
        });
    }

    group.finish();
}

fn bench_acl_authorization(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime should build");
    let mut provider = LocalAuthorizationMetadataProvider::new();
    provider
        .initialize(AuthConfig::default(), None)
        .expect("authorization provider should initialize");
    let provider = Arc::new(provider);
    let entries = (0..128)
        .map(|index| {
            PolicyEntry::of(
                PolicyResource::of(
                    ResourceType::Topic,
                    Some(format!("Topic{index}")),
                    ResourcePattern::Literal,
                ),
                vec![Action::Pub],
                None,
                PolicyDecision::Allow,
            )
        })
        .collect::<Vec<_>>();
    let policy = Policy::of_entries(PolicyType::Custom, entries);
    let acl = Acl::of("alice", rocketmq_auth::SubjectType::User, policy);
    runtime
        .block_on(provider.create_acl(acl))
        .expect("benchmark ACL should be created");

    let handler = AclAuthorizationHandler::new(provider);
    let context = DefaultAuthorizationContext::of(
        "alice",
        rocketmq_auth::SubjectType::User,
        PolicyResource::of_topic("Topic127"),
        Action::Pub,
        "127.0.0.1",
    );

    c.bench_function("auth_acl_authorization/128_literal_topic_entries", |b| {
        b.iter(|| {
            runtime
                .block_on(handler.handle(black_box(&context)))
                .expect("benchmark authorization should allow")
        })
    });
}

fn bench_stateful_authentication_cache(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("benchmark runtime should build");
    let provider = Arc::new(AllowAuthenticationProvider);
    let strategy = StatefulAuthenticationStrategy::new_with_acl_generation(
        AuthConfig {
            authentication_enabled: true,
            stateful_authentication_cache_max_num: 1024,
            stateful_authentication_cache_expired_second: 60,
            ..AuthConfig::default()
        },
        Some(provider),
        Arc::new(AtomicU64::new(0)),
    );
    let mut context = DefaultAuthenticationContext::new();
    context
        .base
        .set_channel_id(Some(CheetahString::from_static_str("channel-a")));
    context.set_username(CheetahString::from_static_str("alice"));

    runtime
        .block_on(strategy.authenticate(&context))
        .expect("benchmark authentication cache should warm");

    c.bench_function("auth_stateful_authentication/cache_hit", |b| {
        b.iter(|| {
            runtime
                .block_on(strategy.authenticate(black_box(&context)))
                .expect("benchmark authentication cache hit should allow")
        })
    });
}

fn bench_stateful_authorization_cache(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime should build");
    let strategy = StatefulAuthorizationStrategy::new_with_acl_generation(
        AuthConfig {
            authorization_enabled: false,
            stateful_authorization_cache_max_num: 1024,
            stateful_authorization_cache_expired_second: 60,
            ..AuthConfig::default()
        },
        None,
        Arc::new(AtomicU64::new(0)),
    )
    .expect("benchmark authorization strategy should build");
    let mut context = DefaultAuthorizationContext::of(
        "alice",
        rocketmq_auth::SubjectType::User,
        PolicyResource::of_topic("TopicA"),
        Action::Pub,
        "127.0.0.1",
    );
    context.set_channel_id("channel-a".to_owned());

    runtime
        .block_on(strategy.evaluate(&context))
        .expect("benchmark authorization cache should warm");

    c.bench_function("auth_stateful_authorization/cache_hit", |b| {
        b.iter(|| {
            runtime
                .block_on(strategy.evaluate(black_box(&context)))
                .expect("benchmark authorization cache hit should allow")
        })
    });
}

fn bench_stateful_authorization_negative_cache(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime should build");
    let mut disabled_config = AuthConfig {
        config_name: CheetahString::from_static_str("negative-cache-disabled"),
        authorization_enabled: true,
        stateful_authorization_cache_max_num: 1024,
        stateful_authorization_cache_expired_second: 60,
        stateful_authorization_cache_negative_enable: false,
        ..AuthConfig::default()
    };
    let disabled_strategy = StatefulAuthorizationStrategy::new_with_acl_generation(
        disabled_config.clone(),
        None,
        Arc::new(AtomicU64::new(0)),
    )
    .expect("benchmark authorization strategy should build");
    disabled_config.stateful_authorization_cache_negative_enable = true;
    disabled_config.config_name = CheetahString::from_static_str("negative-cache-enabled");
    let enabled_strategy =
        StatefulAuthorizationStrategy::new_with_acl_generation(disabled_config, None, Arc::new(AtomicU64::new(0)))
            .expect("benchmark authorization strategy should build");
    let mut context = DefaultAuthorizationContext::of(
        "alice",
        rocketmq_auth::SubjectType::User,
        PolicyResource::of_topic("TopicDenied"),
        Action::Pub,
        "127.0.0.1",
    );
    context.set_channel_id("channel-denied".to_owned());

    assert!(runtime.block_on(disabled_strategy.evaluate(&context)).is_err());
    assert_eq!(disabled_strategy.cache_size(), 0);
    assert!(runtime.block_on(enabled_strategy.evaluate(&context)).is_err());
    assert_eq!(enabled_strategy.cache_size(), 1);

    let mut group = c.benchmark_group("auth_stateful_authorization_negative_cache");
    group.bench_function("deny_no_cache", |b| {
        b.iter(|| {
            black_box(
                runtime
                    .block_on(disabled_strategy.evaluate(black_box(&context)))
                    .is_err(),
            )
        })
    });
    group.bench_function("deny_negative_cache_enabled", |b| {
        b.iter(|| {
            black_box(
                runtime
                    .block_on(enabled_strategy.evaluate(black_box(&context)))
                    .is_err(),
            )
        })
    });
    group.finish();
}

struct AllowAuthenticationProvider;

impl AuthenticationProvider for AllowAuthenticationProvider {
    type Context = DefaultAuthenticationContext;

    async fn initialize(
        &mut self,
        _config: AuthConfig,
        _metadata_service: Option<Arc<dyn Any + Send + Sync>>,
    ) -> RocketMQResult<()> {
        Ok(())
    }

    async fn authenticate(&self, _context: &Self::Context) -> RocketMQResult<()> {
        Ok(())
    }

    fn new_context_from_metadata(
        &self,
        _metadata: &HashMap<String, String>,
        _request: Box<dyn Any + Send>,
    ) -> Self::Context {
        DefaultAuthenticationContext::new()
    }

    fn new_context_from_command(&self, _command: &RemotingCommand) -> Self::Context {
        DefaultAuthenticationContext::new()
    }
}

criterion_group!(
    benches,
    bench_signature,
    bench_white_list,
    bench_acl_authorization,
    bench_stateful_authentication_cache,
    bench_stateful_authorization_cache,
    bench_stateful_authorization_negative_cache,
);
criterion_main!(benches);
