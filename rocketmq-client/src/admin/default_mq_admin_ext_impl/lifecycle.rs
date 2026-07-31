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

use std::env;

use rocketmq_model::common::FAQUrl;
use tracing::info;

use super::*;

impl DefaultMQAdminExtImpl {
    pub fn new(
        client_runtime: Arc<ClientRuntime>,
        rpc_hook: Option<Arc<dyn RPCHook>>,
        timeout_millis: Duration,
        client_config: ClientConfig,
        admin_ext_group: CheetahString,
    ) -> Self {
        DefaultMQAdminExtImpl {
            client_pool: client_runtime.pool().clone(),
            client_pool_token: None,
            service_state: ServiceState::CreateJust,
            client_instance: None,
            rpc_hook,
            timeout_millis,
            kv_namespace_to_delete_list: vec![CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG)],
            client_config,
            admin_ext_group,
        }
    }

    /// Returns whether the facade has a usable concrete implementation.
    ///
    /// The implementation is now owned directly, so this compatibility query is always true.
    pub fn has_inner(&self) -> bool {
        true
    }

    #[inline]
    pub fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }

    #[inline]
    pub fn client_config_mut(&mut self) -> &mut ClientConfig {
        &mut self.client_config
    }

    #[inline]
    pub fn is_use_tls(&self) -> bool {
        self.client_config.is_use_tls()
    }

    #[inline]
    pub fn set_use_tls(&mut self, use_tls: bool) {
        self.client_config.set_use_tls(use_tls);
    }

    pub(super) fn mq_client_api(&self) -> rocketmq_error::RocketMQResult<Arc<MQClientAPIImpl>> {
        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()
    }

    pub(super) fn remoting_timeout_millis(&self) -> rocketmq_error::RocketMQResult<u64> {
        u64::try_from(self.timeout_millis.as_millis()).map_err(|_| {
            rocketmq_error::RocketMQError::illegal_argument("admin timeout exceeds the supported u64 millisecond range")
        })
    }

    pub(super) async fn start_admin(&mut self) -> rocketmq_error::RocketMQResult<()> {
        match self.service_state {
            ServiceState::CreateJust => {
                self.service_state = ServiceState::StartFailed;
                self.client_config.change_instance_name_to_pid();
                if "{}".eq(&self.client_config.socks_proxy_config) {
                    self.client_config.socks_proxy_config =
                        env::var(SOCKS_PROXY_JSON).unwrap_or_else(|_| "{}".to_string()).into();
                }
                let pooled = self
                    .client_pool
                    .get_or_create(self.client_config.clone(), self.rpc_hook.clone())?;
                let (client_instance, token) = pooled.into_parts();
                self.client_instance = Some(client_instance);
                self.client_pool_token = Some(token);

                let group = self.admin_ext_group.clone();
                let register_ok = self
                    .client_instance
                    .as_mut()
                    .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
                    .register_admin_ext(&group)
                    .await;
                if !register_ok {
                    if let Some(token) = self.client_pool_token.take() {
                        self.client_pool.release(token).await;
                    }
                    self.service_state = ServiceState::StartFailed;
                    return Err(rocketmq_error::RocketMQError::illegal_argument(format!(
                        "The adminExt group[{}] has created already, specified another name please.{}",
                        self.admin_ext_group,
                        FAQUrl::suggest_todo(FAQUrl::GROUP_NAME_DUPLICATE_URL)
                    )));
                }
                if let Err(error) = self
                    .client_instance
                    .as_mut()
                    .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
                    .start()
                    .await
                {
                    if let Some(token) = self.client_pool_token.take() {
                        self.client_pool.release(token).await;
                    }
                    return Err(error);
                }
                self.service_state = ServiceState::Running;
                info!("the adminExt [{}] start OK", self.admin_ext_group);
                Ok(())
            }
            ServiceState::Running | ServiceState::ShutdownAlready | ServiceState::StartFailed => {
                Err(rocketmq_error::RocketMQError::ClientAlreadyStarted)
            }
        }
    }

    pub(super) async fn shutdown_admin(&mut self) {
        match self.service_state {
            ServiceState::CreateJust | ServiceState::ShutdownAlready | ServiceState::StartFailed => {}
            ServiceState::Running => {
                if let Some(instance) = self.client_instance.as_mut() {
                    instance.unregister_admin_ext(&self.admin_ext_group).await;
                }
                if let Some(token) = self.client_pool_token.take() {
                    self.client_pool.release(token).await;
                }
                self.service_state = ServiceState::ShutdownAlready;
            }
        }
    }
}
