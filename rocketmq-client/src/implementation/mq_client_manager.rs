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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use parking_lot::RwLock;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_transport::runtime::RPCHook;
use tracing::info;

use crate::base::client_config::ClientConfig;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::producer::produce_accumulator::ProduceAccumulator;
use crate::producer::request_future_holder::RequestFutureHolder;

struct ClientPoolEntry {
    generation: u64,
    leases: usize,
    identity: ClientPoolIdentity,
    instance: Arc<MQClientInstance>,
}

#[derive(Clone, Eq, PartialEq)]
struct ClientPoolIdentity {
    namesrv_addr: Option<CheetahString>,
    use_tls: bool,
    vip_channel_enabled: bool,
    api_timeout_millis: u64,
    rpc_hook_identity: Option<usize>,
}

impl ClientPoolIdentity {
    fn new(client_config: &ClientConfig, rpc_hook: Option<&Arc<dyn RPCHook>>) -> Self {
        Self {
            namesrv_addr: client_config.namesrv_addr.clone(),
            use_tls: client_config.use_tls,
            vip_channel_enabled: client_config.vip_channel_enabled,
            api_timeout_millis: client_config.mq_client_api_timeout,
            rpc_hook_identity: rpc_hook.map(|hook| Arc::as_ptr(hook).cast::<()>() as usize),
        }
    }
}

type ClientInstanceHashMap = DashMap<CheetahString /* clientId */, ClientPoolEntry>;
type AccumulatorHashMap = DashMap<CheetahString /* clientId */, Arc<ProduceAccumulator>>;

#[derive(Clone)]
pub struct ClientPool {
    inner: Arc<ClientPoolInner>,
}

struct ClientPoolInner {
    service_context: ChildServiceContext,
    factory_table: Arc<ClientInstanceHashMap>,
    accumulator_table: Arc<AccumulatorHashMap>,
    request_future_holder: Arc<RequestFutureHolder>,
    next_generation: AtomicU64,
    closed: AtomicBool,
    admission: RwLock<()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientPoolToken {
    client_id: CheetahString,
    generation: u64,
}

pub struct PooledClient {
    instance: Arc<MQClientInstance>,
    token: ClientPoolToken,
}

impl PooledClient {
    pub fn instance(&self) -> &Arc<MQClientInstance> {
        &self.instance
    }

    pub fn into_parts(self) -> (Arc<MQClientInstance>, ClientPoolToken) {
        (self.instance, self.token)
    }
}

impl ClientPool {
    pub(crate) fn new(service_context: ChildServiceContext) -> Self {
        let request_future_holder = Arc::new(RequestFutureHolder::new(service_context.child("request-futures")));
        Self {
            inner: Arc::new(ClientPoolInner {
                service_context,
                factory_table: Arc::new(DashMap::with_capacity(128)),
                accumulator_table: Arc::new(DashMap::with_capacity(128)),
                request_future_holder,
                next_generation: AtomicU64::new(0),
                closed: AtomicBool::new(false),
                admission: RwLock::new(()),
            }),
        }
    }

    pub fn instance_count(&self) -> usize {
        self.inner.factory_table.len()
    }

    pub(crate) fn request_future_holder(&self) -> Arc<RequestFutureHolder> {
        Arc::clone(&self.inner.request_future_holder)
    }

    pub fn get_or_create(
        &self,
        client_config: ClientConfig,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> rocketmq_error::RocketMQResult<PooledClient> {
        let _admission = self.inner.admission.read();
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(mq_client_err!("ClientRuntime is shutting down"));
        }
        let client_id = CheetahString::from_string(client_config.build_mq_client_id());
        let identity = ClientPoolIdentity::new(&client_config, rpc_hook.as_ref());

        let mut entry = self.inner.factory_table.entry(client_id.clone()).or_insert_with(|| {
            let generation = self.inner.next_generation.fetch_add(1, Ordering::AcqRel) + 1;
            info!(
                client_id = %client_id,
                generation,
                "Created new MQClientInstance in ClientPool"
            );
            let instance = MQClientInstance::new_arc(
                client_config,
                generation,
                client_id.clone(),
                rpc_hook,
                self.inner.service_context.child(format!("instance-{generation}")),
                Arc::clone(&self.inner.request_future_holder),
            );
            ClientPoolEntry {
                generation,
                leases: 0,
                identity: identity.clone(),
                instance,
            }
        });
        if entry.identity != identity {
            return Err(mq_client_err!(
                "ClientPool configuration conflicts with an existing client-id owner"
            ));
        }
        entry.leases += 1;

        Ok(PooledClient {
            instance: Arc::clone(&entry.instance),
            token: ClientPoolToken {
                client_id,
                generation: entry.generation,
            },
        })
    }

    pub fn get_or_create_produce_accumulator(&self, client_config: ClientConfig) -> Option<Arc<ProduceAccumulator>> {
        let _admission = self.inner.admission.read();
        if self.inner.closed.load(Ordering::Acquire) {
            return None;
        }
        let client_id = CheetahString::from_string(client_config.build_mq_client_id());

        Some(
            self.inner
                .accumulator_table
                .entry(client_id.clone())
                .or_insert_with(|| {
                    info!("Created new ProduceAccumulator for clientId:[{}]", client_id);
                    Arc::new(ProduceAccumulator::new(
                        self.inner.service_context.child(format!("accumulator-{}", client_id)),
                        client_id.as_str(),
                    ))
                })
                .clone(),
        )
    }

    pub async fn release(&self, token: ClientPoolToken) -> bool {
        let should_remove = self
            .inner
            .factory_table
            .get_mut(&token.client_id)
            .is_some_and(|mut current| {
                if current.generation != token.generation || current.leases == 0 {
                    return false;
                }
                current.leases -= 1;
                current.leases == 0
            });
        if !should_remove {
            return false;
        }

        let removed = self
            .inner
            .factory_table
            .remove_if(&token.client_id, |_, current| {
                current.generation == token.generation && current.leases == 0
            })
            .map(|(_, entry)| entry.instance);
        if let Some(instance) = removed {
            instance.shutdown().await;
            true
        } else {
            false
        }
    }

    pub(crate) async fn shutdown_until(&self, deadline: ShutdownDeadline) {
        let instances = {
            let _admission = self.inner.admission.write();
            self.inner.closed.store(true, Ordering::Release);
            let keys = self
                .inner
                .factory_table
                .iter()
                .map(|entry| entry.key().clone())
                .collect::<Vec<_>>();
            let mut instances = Vec::with_capacity(keys.len());
            for key in keys {
                if let Some((_, entry)) = self.inner.factory_table.remove(&key) {
                    instances.push(entry.instance);
                }
            }
            self.inner.accumulator_table.clear();
            instances
        };

        for instance in instances {
            if deadline.is_expired() {
                break;
            }
            let _ = tokio::time::timeout(deadline.remaining(), instance.shutdown()).await;
        }
    }
}
