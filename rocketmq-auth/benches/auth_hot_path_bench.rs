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
use rocketmq_auth::acl::WhiteList;
use rocketmq_auth::authentication::acl_signer;
use rocketmq_auth::authentication::context::default_authentication_context::DefaultAuthenticationContext;
use rocketmq_auth::authentication::provider::AuthenticationProvider;
use rocketmq_auth::authentication::strategy::AuthenticationStrategy;
use rocketmq_auth::authentication::strategy::StatefulAuthenticationStrategy;
use rocketmq_auth::authorization::chain::AclAuthorizationHandler;
use rocketmq_auth::authorization::chain::AuthorizationHandler;
use rocketmq_auth::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use rocketmq_auth::authorization::enums::decision::Decision;
use rocketmq_auth::authorization::enums::policy_type::PolicyType;
use rocketmq_auth::authorization::metadata_provider::AuthorizationMetadataProvider;
use rocketmq_auth::authorization::metadata_provider::LocalAuthorizationMetadataProvider;
use rocketmq_auth::authorization::model::acl::Acl;
use rocketmq_auth::authorization::model::policy::Policy;
use rocketmq_auth::authorization::model::policy_entry::PolicyEntry;
use rocketmq_auth::authorization::model::resource::Resource;
use rocketmq_auth::authorization::strategy::abstract_authorization_strategy::AuthorizationStrategy;
use rocketmq_auth::authorization::strategy::StatefulAuthorizationStrategy;
use rocketmq_auth::config::AuthConfig;
use rocketmq_auth::SignatureAlgorithm;
use rocketmq_common::common::action::Action;
use rocketmq_common::common::resource::resource_pattern::ResourcePattern;
use rocketmq_common::common::resource::resource_type::ResourceType;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

fn bench_signature(c: &mut Criterion) {
    let mut group = c.benchmark_group("auth_signature");
    let content = b"aliceTopicAqueueId0clientIdCID-123body-bytes";
    let secret = "benchmark-secret";

    for algorithm in [
        SignatureAlgorithm::HmacSha1,
        SignatureAlgorithm::HmacSha256,
        SignatureAlgorithm::HmacMd5,
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(algorithm.java_name()),
            &algorithm,
            |b, algorithm| {
                b.iter(|| {
                    acl_signer::cal_signature_with_algorithm(
                        black_box(content),
                        black_box(secret),
                        black_box(*algorithm),
                    )
                    .expect("benchmark signature should calculate")
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
                Resource::of(
                    ResourceType::Topic,
                    Some(format!("Topic{index}")),
                    ResourcePattern::Literal,
                ),
                vec![Action::Pub],
                None,
                Decision::Allow,
            )
        })
        .collect::<Vec<_>>();
    let policy = Policy::of_entries(PolicyType::Custom, entries);
    let acl = Acl::of(
        "alice",
        rocketmq_auth::authentication::enums::subject_type::SubjectType::User,
        policy,
    );
    runtime
        .block_on(provider.create_acl(acl))
        .expect("benchmark ACL should be created");

    let handler = AclAuthorizationHandler::new(provider);
    let context = DefaultAuthorizationContext::of(
        "alice",
        rocketmq_auth::authentication::enums::subject_type::SubjectType::User,
        Resource::of_topic("Topic127"),
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

    runtime.block_on(async {
        strategy
            .authenticate(&context)
            .expect("benchmark authentication cache should warm")
    });

    c.bench_function("auth_stateful_authentication/cache_hit", |b| {
        b.iter(|| {
            strategy
                .authenticate(black_box(&context))
                .expect("benchmark authentication cache hit should allow")
        })
    });
}

fn bench_stateful_authorization_cache(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime should build");
    let _guard = runtime.enter();
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
        rocketmq_auth::authentication::enums::subject_type::SubjectType::User,
        Resource::of_topic("TopicA"),
        Action::Pub,
        "127.0.0.1",
    );
    context.set_channel_id("channel-a".to_owned());

    strategy
        .evaluate(&context)
        .expect("benchmark authorization cache should warm");

    c.bench_function("auth_stateful_authorization/cache_hit", |b| {
        b.iter(|| {
            strategy
                .evaluate(black_box(&context))
                .expect("benchmark authorization cache hit should allow")
        })
    });
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
);
criterion_main!(benches);
