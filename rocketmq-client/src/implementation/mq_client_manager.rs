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
use rocketmq_observability::metrics::client::ClientMetrics;
use rocketmq_observability::TelemetryHandle;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_transport::api::v1::RPCHook;
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
    resource_budget: ResourceBudget,
    telemetry_handle: TelemetryHandle,
    client_metrics: ClientMetrics,
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
    pub(crate) fn new(
        service_context: ChildServiceContext,
        resource_budget: ResourceBudget,
        telemetry_handle: TelemetryHandle,
        client_metrics: ClientMetrics,
    ) -> Self {
        let request_future_holder = Arc::new(RequestFutureHolder::new(service_context.component("request-futures")));
        Self {
            inner: Arc::new(ClientPoolInner {
                service_context,
                resource_budget,
                telemetry_handle,
                client_metrics,
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
        mut client_config: ClientConfig,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> rocketmq_error::RocketMQResult<PooledClient> {
        let _admission = self.inner.admission.read();
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(mq_client_err!("ClientRuntime is shutting down"));
        }
        client_config.normalize_namesrv_addr()?;
        let client_id = CheetahString::from_string(client_config.build_mq_client_id());
        let identity = ClientPoolIdentity::new(&client_config, rpc_hook.as_ref());

        let mut entry = self.inner.factory_table.entry(client_id.clone()).or_insert_with(|| {
            let generation = self.inner.next_generation.fetch_add(1, Ordering::AcqRel) + 1;
            info!(
                client_id = %client_id,
                generation,
                "Created new MQClientInstance in ClientPool"
            );
            let instance = MQClientInstance::new_arc_with_resource_budget(
                client_config,
                generation,
                client_id.clone(),
                rpc_hook,
                self.inner.service_context.component(format!("instance-{generation}")),
                Arc::clone(&self.inner.request_future_holder),
                self.inner.resource_budget.clone(),
                self.inner.telemetry_handle.clone(),
                self.inner.client_metrics.clone(),
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
                    Arc::new(ProduceAccumulator::with_resource_budget(
                        self.inner
                            .service_context
                            .component(format!("accumulator-{}", client_id)),
                        client_id.as_str(),
                        self.inner.resource_budget.clone(),
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

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_error::RocketMQError;

    use super::*;

    #[test]
    fn invalid_nameserver_address_is_rejected_before_pool_admission() {
        let runtime = crate::runtime::test_client_runtime("invalid-nameserver-pool-test");
        let mut config = ClientConfig::default();
        config.namesrv_addr = Some(CheetahString::from_static_str("missing-port"));

        let error = match runtime.pool().get_or_create(config, None) {
            Ok(_) => panic!("invalid NameServer address must be rejected"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            RocketMQError::ConfigInvalidValue {
                key: "namesrv_addr",
                ..
            }
        ));
        assert_eq!(runtime.pool().instance_count(), 0);
    }

    #[test]
    fn canonical_nameserver_address_drives_pool_identity() {
        let runtime = crate::runtime::test_client_runtime("canonical-nameserver-pool-test");
        let mut first_config = ClientConfig::default();
        first_config.namesrv_addr = Some(CheetahString::from_static_str(" NS-A:9876 ; ns-a:9876 "));
        let first = runtime
            .pool()
            .get_or_create(first_config, None)
            .expect("first pooled client");

        let mut equivalent_config = ClientConfig::default();
        equivalent_config.namesrv_addr = Some(CheetahString::from_static_str("ns-a:9876"));
        let equivalent = runtime
            .pool()
            .get_or_create(equivalent_config, None)
            .expect("equivalent canonical target must share the pool entry");
        assert!(Arc::ptr_eq(first.instance(), equivalent.instance()));

        let mut different_config = ClientConfig::default();
        different_config.namesrv_addr = Some(CheetahString::from_static_str("ns-b:9876"));
        assert!(runtime.pool().get_or_create(different_config, None).is_err());
    }
}
