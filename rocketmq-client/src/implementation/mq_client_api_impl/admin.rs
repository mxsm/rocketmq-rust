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

use super::request_builder::*;
use super::response_decoder::*;
use super::*;

pub struct AdminClient<'a> {
    api: &'a MQClientAPIImpl,
}

impl AdminClient<'_> {
    pub async fn broker_cluster_info(&self, timeout_millis: u64) -> RocketMQResult<ClusterInfo> {
        self.api.get_broker_cluster_info(timeout_millis).await
    }
}

impl MQClientAPIImpl {
    #[must_use]
    pub fn admin_client(&self) -> AdminClient<'_> {
        AdminClient { api: self }
    }
}

impl MQClientAPIImpl {
    pub(crate) async fn get_kvconfig_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<Option<CheetahString>> {
        let request_header = GetKVConfigRequestHeader::new(namespace, key);
        let request = RemotingCommand::create_request_command(RequestCode::GetKvConfig, request_header);

        let name_server_address_list = self.remoting_client.get_name_server_address_list();
        let mut err_response = None;
        for name_srv_addr in name_server_address_list {
            let response = self
                .remoting_client
                .invoke_request(Some(&name_srv_addr), request.clone(), timeout_millis)
                .await?;
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {
                    let response_header = response
                        .decode_command_custom_header::<GetKVConfigResponseHeader>()
                        .map_err(|error| mq_client_err!(format!("decode GetKVConfigResponseHeader failed: {error}")))?;
                    return Ok(response_header.value);
                }
                ResponseCode::QueryNotFound => return Ok(None),
                _ => err_response = Some(response),
            }
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }

        Ok(None)
    }

    pub(crate) async fn get_kv_config_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<Option<CheetahString>> {
        self.get_kvconfig_value(namespace, key, timeout_millis).await
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn delete_kvconfig_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = DeleteKVConfigRequestHeader::new(namespace, key);
        let request = RemotingCommand::create_request_command(RequestCode::DeleteKvConfig, request_header);

        let name_server_address_list = self.remoting_client.get_name_server_address_list();
        let mut err_response = None;
        for name_srv_addr in name_server_address_list {
            let response = self
                .remoting_client
                .invoke_request(Some(&name_srv_addr), request.clone(), timeout_millis)
                .await?;
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {}
                _ => err_response = Some(response),
            }
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn delete_kv_config_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.delete_kvconfig_value(namespace, key, timeout_millis).await
    }

    pub(crate) async fn get_kvlist_by_namespace(
        &self,
        namespace: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::body::kv_table::KVTable> {
        let request_header = GetKVListByNamespaceRequestHeader::new(namespace);
        let request = RemotingCommand::create_request_command(RequestCode::GetKvlistByNamespace, request_header);

        let response = self
            .remoting_client
            .invoke_request(None, request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_protocol::protocol::body::kv_table::KVTable::decode(body.as_ref());
            }
        }

        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_kv_list_by_namespace(
        &self,
        namespace: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::body::kv_table::KVTable> {
        self.get_kvlist_by_namespace(namespace, timeout_millis).await
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn put_kvconfig_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = PutKVConfigRequestHeader::new(namespace, key, value);
        let request = RemotingCommand::create_request_command(RequestCode::PutKvConfig, request_header);

        let name_server_address_list = self.remoting_client.get_name_server_address_list();
        let mut err_response = None;
        for name_srv_addr in name_server_address_list {
            let response = self
                .remoting_client
                .invoke_request(Some(&name_srv_addr), request.clone(), timeout_millis)
                .await?;
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {}
                _ => err_response = Some(response),
            }
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn put_kv_config_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.put_kvconfig_value(namespace, key, value, timeout_millis).await
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn create_user(
        &self,
        broker_address: CheetahString,
        user_info: &UserInfo,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let mut request_header = CreateUserRequestHeader::default();
        let username = user_info
            .username
            .clone()
            .ok_or_else(|| mq_client_err!(-1, "username is required".to_string()))?;
        request_header.set_username(username);
        let mut request = RemotingCommand::create_request_command(RequestCode::AuthCreateUser, request_header);
        request = request.set_body(user_info.encode()?);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request.clone(), timeout_millis)
            .await?;

        let mut err_response = None;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {}
            _ => err_response = Some(response),
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn update_user(
        &self,
        broker_address: CheetahString,
        user_info: &UserInfo,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let mut request_header = UpdateUserRequestHeader::default();
        let username = user_info
            .username
            .clone()
            .ok_or_else(|| mq_client_err!(-1, "username is required".to_string()))?;
        request_header.set_username(username);
        let mut request = RemotingCommand::create_request_command(RequestCode::AuthUpdateUser, request_header);
        request = request.set_body(user_info.encode()?);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request.clone(), timeout_millis)
            .await?;

        let mut err_response = None;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {}
            _ => err_response = Some(response),
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn create_acl(
        &self,
        broker_address: CheetahString,
        acl_info: &AclInfo,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let subject = acl_info
            .subject
            .clone()
            .ok_or_else(|| mq_client_err!(-1, "ACL subject is required".to_string()))?;
        let request_header = CreateAclRequestHeader { subject };
        let request = RemotingCommand::create_request_command(RequestCode::AuthCreateAcl, request_header)
            .set_body(acl_info.encode()?);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn update_acl(
        &self,
        broker_address: CheetahString,
        acl_info: &AclInfo,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let subject = acl_info
            .subject
            .clone()
            .ok_or_else(|| mq_client_err!(-1, "ACL subject is required".to_string()))?;
        let request_header = UpdateAclRequestHeader { subject };
        let request = RemotingCommand::create_request_command(RequestCode::AuthUpdateAcl, request_header)
            .set_body(acl_info.encode()?);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn create_and_update_plain_access_config(
        &self,
        broker_address: CheetahString,
        plain_access_config: &PlainAccessConfig,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = create_and_update_plain_access_config_request(plain_access_config)?;

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, ToString::to_string)
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn delete_plain_access_config(
        &self,
        broker_address: CheetahString,
        access_key: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = delete_plain_access_config_request(&access_key);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, ToString::to_string)
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn update_global_white_addrs_config(
        &self,
        broker_address: CheetahString,
        global_white_addrs: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = UpdateGlobalWhiteAddrsConfigRequestHeader { global_white_addrs };
        let request =
            RemotingCommand::create_request_command(RequestCode::UpdateGlobalWhiteAddrsConfig, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn get_broker_cluster_acl_version_info(
        &self,
        broker_address: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<ClusterAclVersionInfo> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterAclInfo);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => decode_cluster_acl_version_info_response_body(response.body()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn delete_acl(
        &self,
        broker_address: CheetahString,
        subject: CheetahString,
        resource: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let resource_option = if resource.is_empty() { None } else { Some(resource) };
        let request_header = DeleteAclRequestHeader::new(subject, resource_option);
        let request = RemotingCommand::create_request_command(RequestCode::AuthDeleteAcl, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn list_acl(
        &self,
        broker_address: CheetahString,
        subject_filter: CheetahString,
        resource_filter: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<AclInfo>> {
        let request_header = ListAclRequestHeader {
            subject_filter,
            resource_filter,
        };
        let request = RemotingCommand::create_request_command(RequestCode::AuthListAcl, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.get_body() {
                    Vec::<AclInfo>::decode(body.as_ref())
                } else {
                    Ok(Vec::new())
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn get_acl(
        &self,
        broker_address: CheetahString,
        subject: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<AclInfo> {
        let request = get_acl_request(subject);
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let body = response
                    .get_body()
                    .ok_or_else(|| mq_client_err!("get_acl response body is empty".to_string()))?;
                AclInfo::decode(body.as_ref())
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn get_user(
        &self,
        broker_address: CheetahString,
        username: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<Option<UserInfo>> {
        let request_header = GetUserRequestHeader {
            username: username.clone(),
        };
        let request = RemotingCommand::create_request_command(RequestCode::AuthGetUser, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let _body = response.get_body();
                if let Some(body) = response.get_body() {
                    let user_info = UserInfo::decode(body)?;
                    Ok(Some(user_info))
                } else {
                    Ok(None)
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map(|s| s.to_string()).unwrap_or_default()
            )),
        }
    }

    pub(crate) async fn list_users(
        &self,
        broker_address: CheetahString,
        filter: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<UserInfo>> {
        let request_header = ListUsersRequestHeader { filter };
        let request = RemotingCommand::create_request_command(RequestCode::AuthListUsers, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.get_body() {
                    Vec::<UserInfo>::decode(body.as_ref())
                } else {
                    Ok(Vec::new())
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn list_user(
        &self,
        broker_address: CheetahString,
        filter: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<UserInfo>> {
        self.list_users(broker_address, filter, timeout_millis).await
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn delete_user(
        &self,
        broker_address: CheetahString,
        username: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let mut request_header = DeleteUserRequestHeader::default();
        request_header.set_username(username);
        let request = RemotingCommand::create_request_command(RequestCode::AuthDeleteUser, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request.clone(), timeout_millis)
            .await?;

        let mut err_response = None;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {}
            _ => err_response = Some(response),
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn update_name_server_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        special_name_servers: Option<Vec<CheetahString>>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let body = mix_all::properties_to_string(&properties);
        if body.is_empty() {
            return Ok(());
        }
        let invoke_name_servers = if let Some(name_servers) = special_name_servers {
            if !name_servers.is_empty() {
                name_servers
            } else {
                self.get_name_server_address_list()
            }
        } else {
            self.get_name_server_address_list()
        };
        if invoke_name_servers.is_empty() {
            return Ok(());
        }
        let empty_header = EmptyHeader {};
        let mut request = RemotingCommand::create_request_command(RequestCode::UpdateNamesrvConfig, empty_header);

        request = request.set_body(body.to_string());
        let mut err_response = None;
        for name_srv_addr in invoke_name_servers {
            let response = self
                .remoting_client
                .invoke_request(Some(&name_srv_addr), request.clone(), timeout_millis)
                .await?;
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {}
                _ => err_response = Some(response),
            }
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn add_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<i32> {
        let request_header = AddWritePermOfBrokerRequestHeader::new(broker_name);
        let request = RemotingCommand::create_request_command(RequestCode::AddWritePermOfBroker, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&namesrv_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let request_header = response.decode_command_custom_header_fast::<AddWritePermOfBrokerResponseHeader>()?;
            return Ok(request_header.get_add_topic_count());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn wipe_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<i32> {
        let request_header = WipeWritePermOfBrokerRequestHeader::new(broker_name);
        let request = RemotingCommand::create_request_command(RequestCode::WipeWritePermOfBroker, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&namesrv_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let request_header = response.decode_command_custom_header_fast::<WipeWritePermOfBrokerResponseHeader>()?;
            return Ok(request_header.get_wipe_topic_count());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_broker_cluster_info(&self, timeout_millis: u64) -> RocketMQResult<ClusterInfo> {
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerClusterInfo, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(None, request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return ClusterInfo::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_broker_runtime_info(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::body::kv_table::KVTable> {
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerRuntimeInfo, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_protocol::protocol::body::kv_table::KVTable::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn sync_broker_member_group(
        &self,
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
        is_compatible_with_old_name_srv: bool,
    ) -> RocketMQResult<Option<BrokerMemberGroup>> {
        if is_compatible_with_old_name_srv {
            self.get_broker_member_group_compatible(cluster_name, broker_name).await
        } else {
            self.get_broker_member_group(cluster_name, broker_name).await
        }
    }

    pub(super) async fn get_broker_member_group(
        &self,
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
    ) -> RocketMQResult<Option<BrokerMemberGroup>> {
        let request_header = GetBrokerMemberGroupRequestHeader::new(cluster_name.clone(), broker_name.clone());
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerMemberGroup, request_header);
        let mut response = self.remoting_client.invoke_request(None, request, 3000).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.take_body() {
                let response_body = GetBrokerMemberGroupResponseBody::decode(body.as_ref())?;
                return Ok(Some(
                    response_body
                        .broker_member_group
                        .unwrap_or_else(|| empty_broker_member_group(cluster_name, broker_name)),
                ));
            }
        }
        Ok(Some(empty_broker_member_group(cluster_name, broker_name)))
    }

    pub(super) async fn get_broker_member_group_compatible(
        &self,
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
    ) -> RocketMQResult<Option<BrokerMemberGroup>> {
        let request_header = GetRouteInfoRequestHeader {
            topic: CheetahString::from_string(format!(
                "{}{}",
                TopicValidator::SYNC_BROKER_MEMBER_GROUP_PREFIX,
                broker_name
            )),
            ..Default::default()
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetRouteinfoByTopic, request_header);
        let mut response = self.remoting_client.invoke_request(None, request, 3000).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.take_body() {
                let topic_route_data = TopicRouteData::decode(body.as_ref())?;
                return Ok(Some(broker_member_group_from_route_data(
                    cluster_name,
                    broker_name,
                    &topic_route_data,
                )));
            }
        }
        Ok(Some(empty_broker_member_group(cluster_name, broker_name)))
    }

    pub(crate) async fn get_broker_lite_info(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<GetBrokerLiteInfoResponseBody> {
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerLiteInfo, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return GetBrokerLiteInfoResponseBody::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn check_rocksdb_cq_write_progress(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        check_store_time: i64,
        timeout_millis: u64,
    ) -> RocketMQResult<CheckRocksdbCqWriteResult> {
        let request_header = CheckRocksdbCqWriteProgressRequestHeader {
            topic,
            check_store_time,
            rpc: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::CheckRocksdbCqWriteProgress, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                let result: CheckRocksdbCqWriteResult = serde_json::from_slice(body.as_ref()).map_err(|e| {
                    mq_client_err!(-1, format!("Failed to deserialize CheckRocksdbCqWriteResult: {}", e))
                })?;
                return Ok(result);
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn query_consume_queue(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        queue_id: i32,
        index: i64,
        count: i32,
        consumer_group: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<QueryConsumeQueueResponseBody> {
        let request_header = QueryConsumeQueueRequestHeader {
            topic,
            queue_id,
            index,
            count,
            consumer_group: if consumer_group.is_empty() {
                None
            } else {
                Some(consumer_group)
            },
            rpc: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::QueryConsumeQueue, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                let result: QueryConsumeQueueResponseBody = serde_json::from_slice(body.as_ref()).map_err(|e| {
                    mq_client_err!(
                        -1,
                        format!("Failed to deserialize QueryConsumeQueueResponseBody: {}", e)
                    )
                })?;
                return Ok(result);
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_lite_group_info(
        &self,
        addr: &CheetahString,
        group: CheetahString,
        lite_topic: CheetahString,
        top_k: i32,
        timeout_millis: u64,
    ) -> RocketMQResult<GetLiteGroupInfoResponseBody> {
        let request_header = GetLiteGroupInfoRequestHeader {
            group,
            lite_topic,
            top_k,
            rpc: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetLiteGroupInfo, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return GetLiteGroupInfoResponseBody::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_lite_client_info(
        &self,
        addr: &CheetahString,
        parent_topic: CheetahString,
        group: CheetahString,
        client_id: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<GetLiteClientInfoResponseBody> {
        let request_header = GetLiteClientInfoRequestHeader {
            parent_topic: Some(parent_topic),
            group: Some(group),
            client_id: Some(client_id),
            max_count: 1000,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetLiteClientInfo, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return GetLiteClientInfoResponseBody::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn trigger_lite_dispatch(
        &self,
        addr: &CheetahString,
        group: CheetahString,
        client_id: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = TriggerLiteDispatchRequestHeader {
            group,
            client_id: if client_id.is_empty() { None } else { Some(client_id) },
        };
        let request = RemotingCommand::create_request_command(RequestCode::TriggerLiteDispatch, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn sync_lite_subscription(
        &self,
        broker_addr: &CheetahString,
        lite_subscription_dto: LiteSubscriptionDTO,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = lite_subscription_ctl_request(lite_subscription_dto)?;
        let response = self
            .remoting_client
            .invoke_request(Some(broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string()),
                broker_addr.to_string()
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn sync_lite_subscription_async(
        &self,
        broker_addr: &CheetahString,
        lite_subscription_dto: LiteSubscriptionDTO,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.sync_lite_subscription(broker_addr, lite_subscription_dto, timeout_millis)
            .await
    }

    pub(crate) async fn get_lite_topic_info(
        &self,
        addr: &CheetahString,
        parent_topic: &CheetahString,
        lite_topic: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<GetLiteTopicInfoResponseBody> {
        let request_header = GetLiteTopicInfoRequestHeader {
            parent_topic: parent_topic.clone(),
            lite_topic: lite_topic.clone(),
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetLiteTopicInfo, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return GetLiteTopicInfoResponseBody::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }
    pub(crate) async fn get_parent_topic_info(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<GetParentTopicInfoResponseBody> {
        let request_header = GetParentTopicInfoRequestHeader { topic, rpc: None };
        let request = RemotingCommand::create_request_command(RequestCode::GetParentTopicInfo, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return GetParentTopicInfoResponseBody::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn delete_subscription_group(
        &self,
        addr: &CheetahString,
        group_name: CheetahString,
        clean_offset: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = DeleteSubscriptionGroupRequestHeader {
            group_name,
            clean_offset,
            rpc_request_header: None,
        };

        let request = RemotingCommand::create_request_command(RequestCode::DeleteSubscriptionGroup, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }

        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn reset_master_flush_offset(
        &self,
        broker_addr: &CheetahString,
        master_flush_offset: i64,
    ) -> RocketMQResult<()> {
        let request_header = ResetMasterFlushOffsetHeader {
            master_flush_offset: Some(master_flush_offset),
        };

        let request = RemotingCommand::create_request_command(RequestCode::ResetMasterFlushOffset, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(broker_addr), request, 3000)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }

        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(super) async fn get_topic_list_from_name_server_by_code(
        &self,
        request_code: RequestCode,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicList> {
        let request = RemotingCommand::create_request_command(request_code, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(None, request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return TopicList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_topic_list_from_name_server(&self, timeout_millis: u64) -> RocketMQResult<TopicList> {
        self.get_topic_list_from_name_server_by_code(RequestCode::GetAllTopicListFromNameserver, timeout_millis)
            .await
    }

    pub(crate) async fn get_all_topic_list_from_name_server(&self, timeout_millis: u64) -> RocketMQResult<TopicList> {
        self.get_topic_list_from_name_server(timeout_millis).await
    }

    pub(crate) async fn get_system_topic_list(&self, timeout_millis: u64) -> RocketMQResult<TopicList> {
        let mut topic_list = self
            .get_topic_list_from_name_server_by_code(RequestCode::GetSystemTopicListFromNs, timeout_millis)
            .await?;
        merge_system_topic_list_from_broker(self, &mut topic_list, timeout_millis).await?;
        Ok(topic_list)
    }

    pub(crate) async fn get_unit_topic_list(
        &self,
        contain_retry: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicList> {
        let mut topic_list = self
            .get_topic_list_from_name_server_by_code(RequestCode::GetUnitTopicList, timeout_millis)
            .await?;
        filter_retry_topics_like_java(&mut topic_list, contain_retry);
        Ok(topic_list)
    }

    pub(crate) async fn get_has_unit_sub_topic_list(
        &self,
        contain_retry: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicList> {
        let mut topic_list = self
            .get_topic_list_from_name_server_by_code(RequestCode::GetHasUnitSubTopicList, timeout_millis)
            .await?;
        filter_retry_topics_like_java(&mut topic_list, contain_retry);
        Ok(topic_list)
    }

    pub(crate) async fn get_has_unit_sub_un_unit_topic_list(
        &self,
        contain_retry: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicList> {
        let mut topic_list = self
            .get_topic_list_from_name_server_by_code(RequestCode::GetHasUnitSubUnunitTopicList, timeout_millis)
            .await?;
        filter_retry_topics_like_java(&mut topic_list, contain_retry);
        Ok(topic_list)
    }

    pub(crate) async fn get_topics_by_cluster(
        &self,
        cluster: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::body::topic::topic_list::TopicList> {
        let request_header = GetTopicsByClusterRequestHeader::new(cluster);
        let request = RemotingCommand::create_request_command(RequestCode::GetTopicsByCluster, request_header);
        let response = self
            .remoting_client
            .invoke_request(None, request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_protocol::protocol::body::topic::topic_list::TopicList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_cluster_list(
        &self,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<HashSet<CheetahString>> {
        let topic_route_data = self
            .get_topic_route_info_from_name_server(&topic, timeout_millis)
            .await?
            .ok_or_else(|| mq_client_err!(format!("Topic route not found for: {topic}")))?;
        let cluster_info = self.get_broker_cluster_info(timeout_millis).await?;
        Ok(cluster_names_for_topic_route(&cluster_info, &topic_route_data))
    }

    pub(crate) async fn get_system_topic_list_from_broker(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::body::topic::topic_list::TopicList> {
        let request =
            RemotingCommand::create_request_command(RequestCode::GetSystemTopicListFromBroker, EmptyHeader {});
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_protocol::protocol::body::topic::topic_list::TopicList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_consume_stats(
        &self,
        addr: &CheetahString,
        request_header: rocketmq_protocol::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats> {
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumeStats, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn query_consume_time_span(
        &self,
        addr: &CheetahString,
        request_header: QueryConsumeTimeSpanRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<rocketmq_protocol::protocol::body::queue_time_span::QueueTimeSpan>> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryConsumeTimeSpan, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                let body: QueryConsumeTimeSpanBody = serde_json::from_slice(body.as_ref())?;
                return Ok(body.consume_time_span_set);
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_topic_stats_info(
        &self,
        addr: &CheetahString,
        request_header: GetTopicStatsInfoRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable> {
        let request = RemotingCommand::create_request_command(RequestCode::GetTopicStatsInfo, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_topic_config(
        &self,
        addr: &CheetahString,
        request_header: GetTopicConfigRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicConfigAndQueueMapping> {
        let request = RemotingCommand::create_request_command(RequestCode::GetTopicConfig, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return serde_json::from_slice(body.as_ref()).map_err(Into::into);
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn create_static_topic(
        &self,
        addr: &CheetahString,
        default_topic: CheetahString,
        topic_config: TopicConfig,
        mapping_detail: TopicQueueMappingDetail,
        force: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let topic = topic_config
            .topic_name
            .clone()
            .filter(|topic| !topic.is_empty())
            .ok_or_else(|| rocketmq_error::RocketMQError::IllegalArgument("Topic name is required".into()))?;
        let read_queue_nums = i32::try_from(topic_config.read_queue_nums).map_err(|_| {
            rocketmq_error::RocketMQError::IllegalArgument("readQueueNums exceeds Java int range".into())
        })?;
        let write_queue_nums = i32::try_from(topic_config.write_queue_nums).map_err(|_| {
            rocketmq_error::RocketMQError::IllegalArgument("writeQueueNums exceeds Java int range".into())
        })?;
        let perm = i32::try_from(topic_config.perm)
            .map_err(|_| rocketmq_error::RocketMQError::IllegalArgument("perm exceeds Java int range".into()))?;
        let topic_sys_flag = i32::try_from(topic_config.topic_sys_flag).map_err(|_| {
            rocketmq_error::RocketMQError::IllegalArgument("topicSysFlag exceeds Java int range".into())
        })?;

        let request_header = CreateTopicRequestHeader {
            topic,
            default_topic,
            read_queue_nums,
            write_queue_nums,
            perm,
            topic_filter_type: CheetahString::from_static_str(topic_config.topic_filter_type.as_str()),
            topic_sys_flag: Some(topic_sys_flag),
            order: topic_config.order,
            attributes: None,
            force: Some(force),
            topic_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::UpdateAndCreateStaticTopic, request_header)
            .set_body(mapping_detail.encode()?);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }

        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub(crate) async fn query_topic_consume_by_who(
        &self,
        addr: &CheetahString,
        request_header: rocketmq_protocol::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::body::group_list::GroupList> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryTopicConsumeByWho, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_protocol::protocol::body::group_list::GroupList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn query_topics_by_consumer(
        &self,
        addr: &CheetahString,
        request_header: QueryTopicsByConsumerRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::body::topic::topic_list::TopicList> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryTopicsByConsumer, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_protocol::protocol::body::topic::topic_list::TopicList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn query_subscription_by_consumer(
        &self,
        addr: &CheetahString,
        request_header: QuerySubscriptionByConsumerRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<SubscriptionData> {
        let request = RemotingCommand::create_request_command(RequestCode::QuerySubscriptionByConsumer, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let body = response
                .get_body()
                .ok_or_else(|| mq_client_err!("query_subscription_by_consumer response body is empty".to_string()))?;
            let response_body: QuerySubscriptionResponseBody = serde_json::from_slice(body.as_ref())?;
            return response_body.subscription_data.ok_or_else(|| {
                mq_client_err!("query_subscription_by_consumer response subscriptionData is empty".to_string())
            });
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub(super) async fn invoke_broker_success_request(
        &self,
        addr: &CheetahString,
        request_code: RequestCode,
        timeout_millis: u64,
    ) -> RocketMQResult<bool> {
        let request = RemotingCommand::create_request_command(request_code, EmptyHeader {});
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(true);
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn clean_expired_consume_queue(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<bool> {
        self.invoke_broker_success_request(addr, RequestCode::CleanExpiredConsumequeue, timeout_millis)
            .await
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn delete_expired_commit_log(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<bool> {
        self.invoke_broker_success_request(addr, RequestCode::DeleteExpiredCommitlog, timeout_millis)
            .await
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn clean_unused_topic(&self, addr: &CheetahString, timeout_millis: u64) -> RocketMQResult<bool> {
        self.invoke_broker_success_request(addr, RequestCode::CleanUnusedTopic, timeout_millis)
            .await
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn clean_unused_topic_by_addr(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<bool> {
        self.clean_unused_topic(addr, timeout_millis).await
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn create_topic(
        &self,
        addr: &CheetahString,
        default_topic: CheetahString,
        topic_config: &TopicConfig,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = create_topic_request_header_like_java(default_topic, topic_config)?;
        self.update_or_create_topic(addr, request_header, timeout_millis).await
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn create_topic_list(
        &self,
        address: &CheetahString,
        topic_config_list: Vec<TopicConfig>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = create_topic_list_request(topic_config_list)?;
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, address.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub(crate) async fn update_or_create_topic(
        &self,
        addr: &CheetahString,
        request_header: CreateTopicRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::UpdateAndCreateTopic, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn create_subscription_group_list(
        &self,
        address: &CheetahString,
        configs: Vec<SubscriptionGroupConfig>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = create_subscription_group_list_request(configs)?;
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, address.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn delete_topic_in_broker(
        &self,
        addr: &CheetahString,
        request_header: DeleteTopicRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::DeleteTopicInBroker, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn delete_topic_in_name_server(
        &self,
        addr: &CheetahString,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.delete_topic_in_nameserver(
            addr,
            DeleteTopicFromNamesrvRequestHeader::new(topic, cluster_name),
            timeout_millis,
        )
        .await
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn delete_topic_in_nameserver(
        &self,
        addr: &CheetahString,
        request_header: DeleteTopicFromNamesrvRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::DeleteTopicInNamesrv, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn invoke_broker_to_reset_offset(
        &self,
        addr: &CheetahString,
        request_header: ResetOffsetRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<HashMap<MessageQueue, i64>> {
        let request = RemotingCommand::create_request_command(RequestCode::InvokeBrokerToResetOffset, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return reset_offset_table_from_response(&response);
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn query_correction_offset(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        group: CheetahString,
        filter_groups: Option<Vec<CheetahString>>,
        timeout_millis: u64,
    ) -> RocketMQResult<HashMap<i32, i64>> {
        let request = query_correction_offset_request(topic, group, filter_groups);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return QueryCorrectionOffsetBody::decode(body.as_ref()).map(|body| body.correction_offsets);
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn view_broker_stats_data(
        &self,
        addr: &CheetahString,
        request_header: rocketmq_protocol::protocol::header::view_broker_stats_data_request_header::ViewBrokerStatsDataRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::subscription::broker_stats_data::BrokerStatsData> {
        let request = RemotingCommand::create_request_command(RequestCode::ViewBrokerStatsData, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_protocol::protocol::subscription::broker_stats_data::BrokerStatsData::decode(
                    body.as_ref(),
                );
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn fetch_consume_stats_in_broker(
        &self,
        addr: &CheetahString,
        request_header: rocketmq_protocol::protocol::header::get_consume_stats_in_broker_header::GetConsumeStatsInBrokerHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::admin::consume_stats_list::ConsumeStatsList> {
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerConsumeStats, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_protocol::protocol::admin::consume_stats_list::ConsumeStatsList::decode(body.as_ref());
            }
        }

        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn clone_group_offset(
        &self,
        addr: &CheetahString,
        src_group: CheetahString,
        dest_group: CheetahString,
        topic: CheetahString,
        is_offline: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = CloneGroupOffsetRequestHeader {
            src_group,
            dest_group,
            topic: Some(topic),
            offline: is_offline,
            rpc_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::CloneGroupOffset, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }

        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }
}

impl MQClientAPIImpl {
    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn update_broker_config(
        &self,
        addr: &CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let validator_input = properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<String, String>>();
        crate::base::validators::Validators::check_broker_config(&validator_input)?;

        let body = mix_all::properties_to_string(&properties);
        if body.is_empty() {
            return Ok(());
        }

        let request =
            RemotingCommand::create_remoting_command(RequestCode::UpdateBrokerConfig).set_body(body.to_string());
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn add_broker(
        &self,
        addr: &CheetahString,
        broker_config_path: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = AddBrokerRequestHeader {
            config_path: Some(broker_config_path),
        };
        let request = RemotingCommand::create_request_command(RequestCode::AddBroker, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn remove_broker(
        &self,
        addr: &CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = RemoveBrokerRequestHeader {
            broker_name,
            broker_cluster_name: cluster_name,
            broker_id,
        };
        let request = RemotingCommand::create_request_command(RequestCode::RemoveBroker, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn notify_min_broker_id_changed(
        &self,
        broker_addr: &CheetahString,
        request_header: NotifyMinBrokerIdChangeRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::NotifyMinBrokerIdChange, request_header);
        self.remoting_client
            .invoke_request_oneway(broker_addr, request, timeout_millis)
            .await;
        Ok(())
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn export_rocksdb_config_to_json(
        &self,
        broker_addr: CheetahString,
        config_types: Vec<CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let mut config_type = config_types
            .into_iter()
            .map(|config_type| config_type.as_str().trim().to_string())
            .filter(|config_type| !config_type.is_empty())
            .collect::<Vec<_>>()
            .join(";");
        if !config_type.is_empty() {
            config_type.push(';');
        }

        let request_header = ExportRocksdbConfigToJsonRequestHeader {
            config_type: CheetahString::from(config_type),
        };
        let request = RemotingCommand::create_request_command(RequestCode::ExportRocksdbConfigToJson, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn export_rocks_db_config_to_json(
        &self,
        broker_addr: CheetahString,
        config_types: Vec<CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.export_rocksdb_config_to_json(broker_addr, config_types, timeout_millis)
            .await
    }

    pub(crate) async fn get_all_consumer_offset_json(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<CheetahString> {
        let request = get_all_consumer_offset_request();
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        consumer_offset_json_from_response(&response)
    }

    pub(crate) async fn get_broker_config(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<HashMap<CheetahString, CheetahString>> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerConfig);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let body = response
                    .get_body()
                    .ok_or_else(|| mq_client_err!("Broker config response body is empty".to_string()))?;
                let body_str = String::from_utf8_lossy(body.as_ref());
                mix_all::string_to_properties(body_str.as_ref())
                    .ok_or_else(|| mq_client_err!("Failed to parse broker config response body".to_string()))
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub async fn get_name_server_config(
        &self,
        name_servers: Option<Vec<CheetahString>>,
        timeout_millis: Duration,
    ) -> RocketMQResult<Option<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>>> {
        // Determine which name servers to invoke
        let invoke_name_servers = match name_servers {
            Some(servers) if !servers.is_empty() => servers,
            _ => self.remoting_client.get_name_server_address_list().to_vec(),
        };

        if invoke_name_servers.is_empty() {
            return Ok(None);
        }

        // Create request command
        let request = RemotingCommand::create_remoting_command(RequestCode::GetNamesrvConfig);
        let timeout_millis = duration_millis_to_u64("getNameServerConfig", timeout_millis)?;
        let mut config_map = HashMap::with_capacity(4);
        // Iterate through each name server
        for name_server in invoke_name_servers {
            // Make synchronous call with timeout
            let response = self
                .remoting_client
                .invoke_request(Some(&name_server), request.clone(), timeout_millis)
                .await?;
            // Check response code
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {
                    // Parse response body as properties
                    match response.get_body() {
                        Some(body) => {
                            let body_str = String::from_utf8_lossy(body.as_ref()).to_string();
                            // if body_str contains =, return from Java version
                            let properties = if body_str.contains('=') {
                                mix_all::string_to_properties(&body_str).unwrap_or_default()
                            } else {
                                SerdeJsonUtils::from_json_str::<HashMap<CheetahString, CheetahString>>(&body_str)
                                    .map_err(|e| mq_client_err!(format!("Failed to parse namesrv config JSON: {e}")))?
                            };

                            config_map.insert(name_server.clone(), properties);
                        }
                        None => return Err(mq_client_err!("Body is empty".to_string())),
                    }
                }
                _code => {
                    return Err(mq_client_err!(
                        response.code(),
                        response.remark().map_or("".to_string(), |s| s.to_string())
                    ));
                }
            }
        }
        Ok(Some(config_map))
    }

    pub async fn probe_name_server(&self, name_server: &CheetahString, timeout_millis: Duration) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(
            RequestCode::GetNamesrvConfig,
            GetNamesrvConfigRequestHeader::for_probe(),
        );
        let timeout_millis = duration_millis_to_u64("probeNameServer", timeout_millis)?;
        let response = self
            .remoting_client
            .invoke_request(Some(name_server), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub async fn get_controller_metadata(
        &self,
        controller_address: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<GetMetaDataResponseHeader> {
        let request = RemotingCommand::create_remoting_command(RequestCode::ControllerGetMetadataInfo);
        let response = self
            .remoting_client
            .invoke_request(Some(&controller_address), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => match response.decode_command_custom_header_fast::<GetMetaDataResponseHeader>() {
                Ok(header) => Ok(header),
                Err(_) => Err(mq_client_err!("Could not decode GetMetaDataResponseHeader".to_string())),
            },
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub async fn get_controller_meta_data(
        &self,
        controller_address: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<GetMetaDataResponseHeader> {
        self.get_controller_metadata(controller_address, timeout_millis).await
    }

    pub async fn get_in_sync_state_data(
        &self,
        controller_address: CheetahString,
        brokers: Vec<CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<BrokerReplicasInfo> {
        let controller_meta_data = self.get_controller_metadata(controller_address, timeout_millis).await?;
        let leader_address = controller_leader_address(controller_meta_data)?;

        let request = RemotingCommand::create_remoting_command(RequestCode::ControllerGetSyncStateData);
        let body = serde_json::to_vec(&brokers)
            .map_err(|e| mq_client_err!(format!("Failed to serialize broker names: {}", e)))?;
        let request = request.set_body(body);
        let response = self
            .remoting_client
            .invoke_request(Some(&leader_address), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.body() {
                    serde_json::from_slice(body)
                        .map_err(|e| mq_client_err!(format!("Failed to decode BrokerReplicasInfo: {}", e)))
                } else {
                    Err(mq_client_err!(
                        "get_in_sync_state_data response body is empty".to_string()
                    ))
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub async fn get_broker_epoch_cache(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<EpochEntryCache> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerEpochCache);
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.body() {
                    match EpochEntryCache::decode(body) {
                        Ok(value) => Ok(value),
                        Err(e) => Err(mq_client_err!(format!("decode EpochEntryCache failed: {}", e))),
                    }
                } else {
                    Err(mq_client_err!(
                        "get_broker_epoch_cache response body is empty".to_string()
                    ))
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub async fn get_broker_ha_status(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<HARuntimeInfo> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerHaStatus);
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.body() {
                    serde_json::from_slice(body)
                        .map_err(|e| mq_client_err!(format!("decode HARuntimeInfo failed: {}", e)))
                } else {
                    Err(mq_client_err!("get_broker_ha_status response body is empty".to_string()))
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn update_and_get_group_forbidden(
        &self,
        broker_addr: &CheetahString,
        request_header: UpdateGroupForbiddenRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<GroupForbidden> {
        let request = RemotingCommand::create_request_command(RequestCode::UpdateAndGetGroupForbidden, request_header);
        let broker_addr_vip = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, broker_addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr_vip), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let Some(body) = response.body() else {
                    return Err(mq_client_err!(
                        "update_and_get_group_forbidden response body is empty".to_string()
                    ));
                };
                SerdeJsonUtils::from_json_slice(body.as_ref())
                    .map_err(|error| mq_client_err!(format!("decode GroupForbidden failed: {error}")))
            }
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string()),
                broker_addr.to_string()
            )),
        }
    }

    pub async fn get_controller_config(
        &self,
        controller_address: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<HashMap<CheetahString, CheetahString>> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetControllerConfig);
        let response = self
            .remoting_client
            .invoke_request(Some(&controller_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let body = response
                    .body()
                    .ok_or_else(|| mq_client_err!("Controller config response body is empty".to_string()))?;
                controller_config_from_response_body(body)
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn update_controller_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        controllers: Vec<CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let body = mix_all::properties_to_string(&properties);
        if body.is_empty() || controllers.is_empty() {
            return Ok(());
        }

        let request =
            RemotingCommand::create_remoting_command(RequestCode::UpdateControllerConfig).set_body(body.to_string());
        let mut err_response = None;
        for controller_addr in controllers {
            let response = self
                .remoting_client
                .invoke_request(Some(&controller_addr), request.clone(), timeout_millis)
                .await?;
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {}
                _ => err_response = Some(response),
            }
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response
                    .remark()
                    .map_or_else(String::new, |remark| remark.to_string())
            ));
        }
        Ok(())
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn elect_master(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: Option<u64>,
        timeout_millis: u64,
    ) -> RocketMQResult<(ElectMasterResponseHeader, BrokerMemberGroup)> {
        let controller_meta_data = self.get_controller_metadata(controller_addr, timeout_millis).await?;
        let leader_address = controller_leader_address(controller_meta_data)?;
        let designate_elect = broker_id.is_some();
        let broker_id = match broker_id {
            Some(broker_id) => i64::try_from(broker_id)
                .map_err(|error| mq_client_err!(format!("brokerId is out of range for i64: {error}")))?,
            None => -1,
        };
        let request_header = ElectMasterRequestHeader::new(
            cluster_name,
            broker_name,
            broker_id,
            designate_elect,
            rocketmq_runtime::common::time_utils::current_millis(),
        );
        let request = RemotingCommand::create_request_command(RequestCode::ControllerElectMaster, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(&leader_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let response_header = response
                    .decode_command_custom_header_fast::<ElectMasterResponseHeader>()
                    .map_err(|error| mq_client_err!(format!("Could not decode ElectMasterResponseHeader: {error}")))?;
                let broker_member_group = response
                    .body()
                    .ok_or_else(|| mq_client_err!("elect_master response body is empty".to_string()))
                    .and_then(|body| {
                        serde_json::from_slice::<BrokerMemberGroup>(body)
                            .map_err(|error| mq_client_err!(format!("decode BrokerMemberGroup failed: {error}")))
                    })?;
                Ok((response_header, broker_member_group))
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn clean_controller_broker_data(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_controller_ids_to_clean: Option<CheetahString>,
        clean_living_broker: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let controller_meta_data = self.get_controller_metadata(controller_addr, timeout_millis).await?;
        let leader_address = controller_leader_address(controller_meta_data)?;
        let request_header = CleanBrokerDataRequestHeader {
            cluster_name: if cluster_name.is_empty() {
                None
            } else {
                Some(cluster_name)
            },
            broker_name,
            broker_controller_ids_to_clean,
            clean_living_broker,
            ..Default::default()
        };
        let request = RemotingCommand::create_request_command(RequestCode::CleanBrokerData, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(&leader_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn update_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let body = mix_all::properties_to_string(&properties);
        if body.is_empty() {
            return Ok(());
        }

        let request = RemotingCommand::create_remoting_command(RequestCode::UpdateColdDataFlowCtrConfig);
        let request = request.set_body(body.to_string());
        let broker_addr =
            mix_all::broker_vip_channel(self.client_config.is_vip_channel_enabled(), broker_addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(broker_addr).as_ref(), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map(|s| s.to_string()).unwrap_or_default()
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn remove_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        if consumer_group.is_empty() {
            return Ok(());
        }

        let request = RemotingCommand::create_request_command(RequestCode::RemoveColdDataFlowCtrConfig, EmptyHeader {})
            .set_body(consumer_group.to_string());
        let broker_addr_vip =
            mix_all::broker_vip_channel(self.client_config.is_vip_channel_enabled(), broker_addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr_vip), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string()),
                broker_addr.to_string()
            )),
        }
    }

    pub async fn get_cold_data_flow_ctr_info(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<CheetahString> {
        let request = RemotingCommand::create_request_command(RequestCode::GetColdDataFlowCtrInfo, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(response
                .body()
                .filter(|body| !body.is_empty())
                .map(|body| CheetahString::from_string(String::from_utf8_lossy(body.as_ref()).into_owned()))
                .unwrap_or_default()),
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string()),
                broker_addr.to_string()
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn set_commit_log_read_ahead_mode(
        &self,
        broker_addr: CheetahString,
        mode: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<CheetahString> {
        let mut request = RemotingCommand::create_request_command(RequestCode::SetCommitlogReadMode, EmptyHeader {});
        request.ensure_ext_fields_initialized();
        request.add_ext_field(file_readahead_mode::READ_AHEAD_MODE, mode);
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(response.remark().cloned().unwrap_or_default()),
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string()),
                broker_addr.to_string()
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub async fn export_pop_record(&self, broker_addr: CheetahString, timeout_millis: u64) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::PopRollback, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }

        Err(client_broker_err!(
            response.code(),
            response.remark().map_or_else(String::new, |remark| remark.to_string()),
            broker_addr.to_string()
        ))
    }

    pub fn init_remoting_version() {
        if let Err(error) = remoting_command_facade::initialize_remoting_version(CURRENT_VERSION as i32) {
            warn!(
                initialized = error.initialized(),
                requested = error.requested(),
                "client retained the remoting version selected earlier in process bootstrap"
            );
        }
    }

    pub(crate) async fn get_all_topic_config(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper> {
        let request = RemotingCommand::create_request_command(RequestCode::GetAllTopicConfig, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_protocol::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper::decode(
                    body.as_ref(),
                );
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_all_subscription_group_config(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper> {
        let request =
            RemotingCommand::create_request_command(RequestCode::GetAllSubscriptionGroupConfig, EmptyHeader {});
        let mut response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.take_body() {
                return rocketmq_protocol::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper::decode(
                    body.as_ref(),
                );
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_all_subscription_group(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper> {
        self.get_all_subscription_group_config(addr, timeout_millis).await
    }

    pub(crate) async fn get_subscription_group_config(
        &self,
        addr: &CheetahString,
        group: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig>
    {
        let request = RemotingCommand::create_request_command(
            RequestCode::GetSubscriptionGroupConfig,
            rocketmq_protocol::protocol::header::get_subscription_group_config_request_header::GetSubscriptionGroupConfigRequestHeader {
                group,
                rpc_request_header: None,
            },
        );
        let mut response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.take_body() {
                return rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig::decode(
                    body.as_ref(),
                );
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn create_subscription_group(
        &self,
        addr: &CheetahString,
        config: &rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request =
            RemotingCommand::create_request_command(RequestCode::UpdateAndCreateSubscriptionGroup, EmptyHeader {})
                .set_body(config.encode()?);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub(crate) async fn get_consumer_running_info(
        &self,
        addr: &CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        jstack: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::body::consumer_running_info::ConsumerRunningInfo> {
        let request_header = GetConsumerRunningInfoRequestHeader {
            consumer_group,
            client_id,
            jstack_enable: jstack,
            rpc_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumerRunningInfo, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let mut response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let Some(body) = response.take_body() else {
                    return Err(mq_client_err!(
                        "get_consumer_running_info response body is empty".to_string()
                    ));
                };
                rocketmq_protocol::protocol::body::consumer_running_info::ConsumerRunningInfo::decode(&body).map_err(
                    |error| mq_client_err!(format!("decode get_consumer_running_info response failed: {error}")),
                )
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn consume_message_directly(
        &self,
        client_addr: &CheetahString,
        request_header: ConsumeMessageDirectlyResultRequestHeader,
        message: &MessageExt,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult>
    {
        let body = MessageDecoder::encode(message, false)?;
        let request = RemotingCommand::create_request_command(RequestCode::ConsumeMessageDirectly, request_header)
            .set_body(body.to_vec());
        let mut response = self
            .remoting_client
            .invoke_request(Some(client_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.take_body() {
                    rocketmq_protocol::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult::decode(
                        body.as_ref(),
                    )
                } else {
                    Err(mq_client_err!(
                        "consume_message_directly response body is empty".to_string()
                    ))
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub async fn view_message(
        &self,
        addr: &CheetahString,
        request_header: ViewMessageRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<MessageExt> {
        let request = RemotingCommand::create_request_command(RequestCode::ViewMessageById, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.get_body() {
                    let mut bytes = body.clone();
                    let body_len = bytes.len();
                    MessageDecoder::decode(&mut bytes, true, true, false, false, false).ok_or_else(|| {
                        mq_client_err!(format!(
                            "Failed to decode message from view_message response body: body_len={}, possible causes: \
                             CRC check failed or malformed message data",
                            body_len
                        ))
                    })
                } else {
                    Err(mq_client_err!("view_message response body is empty".to_string()))
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn resume_check_half_message(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<bool> {
        let request_header = ResumeCheckHalfMessageRequestHeader {
            topic,
            msg_id: Some(msg_id),
        };
        let request = RemotingCommand::create_request_command(RequestCode::ResumeCheckHalfMessage, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(true),
            _ => {
                error!(
                    "Failed to resume half message check logic. Remark={}",
                    response.remark().map_or("", |remark| remark)
                );
                Ok(false)
            }
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn switch_timer_engine(
        &self,
        broker_addr: &CheetahString,
        engine_type: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let mut request = RemotingCommand::create_request_command(RequestCode::SwitchTimerEngine, EmptyHeader {});
        request.ensure_ext_fields_initialized();
        request.add_ext_field(MessageConst::TIMER_ENGINE_TYPE, engine_type);
        let response = self
            .remoting_client
            .invoke_request(Some(broker_addr), request, timeout_millis)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }

        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            broker_addr.to_string()
        ))
    }

    pub(super) fn build_queue_offset_sorted_map(
        topic: &str,
        msg_found_list: &[MessageExt],
    ) -> RocketMQResult<HashMap<String, Vec<i64>>> {
        let mut sort_map: HashMap<String, Vec<i64>> = HashMap::with_capacity(16);
        for message_ext in msg_found_list {
            let key: String;
            let dispatch = message_ext
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
                ))
                .unwrap_or_default();
            if mix_all::is_lmq(Some(topic)) && message_ext.reconsume_times() == 0 && !dispatch.is_empty() {
                // process LMQ
                let queues: Vec<&str> = dispatch.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                let data = message_ext
                    .property(&CheetahString::from_static_str(
                        MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                    ))
                    .unwrap_or_default();
                let queue_offsets: Vec<&str> = data.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                // LMQ topic has only 1 queue, which queue id is 0
                key = ExtraInfoUtil::get_start_offset_info_map_key(topic, mix_all::LMQ_QUEUE_ID as i64);
                let Some(position) = queues.iter().position(|&q| q == topic) else {
                    warn!(
                        "LMQ dispatch queue does not contain topic={}, dispatch={}",
                        topic, dispatch
                    );
                    continue;
                };
                let Some(offset_value) = queue_offsets.get(position) else {
                    warn!(
                        "LMQ queue offset is missing for topic={}, dispatch={}, offsets={}",
                        topic, dispatch, data
                    );
                    continue;
                };
                let Ok(offset) = offset_value.parse::<i64>() else {
                    warn!(
                        "LMQ queue offset is invalid for topic={}, offset={}",
                        topic, offset_value
                    );
                    continue;
                };
                sort_map
                    .entry(key)
                    .or_insert_with(|| Vec::with_capacity(4))
                    .push(offset);
                continue;
            }
            // Value of POP_CK is used to determine whether it is a pop retry,
            // cause topic could be rewritten by broker.
            key = ExtraInfoUtil::get_start_offset_info_map_key_with_pop_ck(
                message_ext.topic(),
                message_ext
                    .property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK))
                    .clone()
                    .as_ref()
                    .map(|item| item.as_str()),
                message_ext.queue_id() as i64,
            )?;
            sort_map
                .entry(key)
                .or_insert_with(|| Vec::with_capacity(4))
                .push(message_ext.queue_offset());
        }
        Ok(sort_map)
    }
}

pub(super) fn pop_msg_queue_offset_for_index(
    queue_id_key: &str,
    queue_offset: i64,
    sort_map: &HashMap<String, Vec<i64>>,
    msg_offset_info: &HashMap<String, Vec<i64>>,
) -> Option<i64> {
    let index = sort_map
        .get(queue_id_key)?
        .iter()
        .position(|&offset| offset == queue_offset)?;
    msg_offset_info
        .get(queue_id_key)
        .and_then(|offsets| offsets.get(index))
        .copied()
}

pub(super) fn empty_broker_member_group(
    cluster_name: &CheetahString,
    broker_name: &CheetahString,
) -> BrokerMemberGroup {
    BrokerMemberGroup::new(cluster_name.clone(), broker_name.clone())
}

pub(super) fn broker_member_group_from_route_data(
    cluster_name: &CheetahString,
    broker_name: &CheetahString,
    topic_route_data: &TopicRouteData,
) -> BrokerMemberGroup {
    let mut broker_member_group = empty_broker_member_group(cluster_name, broker_name);
    if let Some(broker_data) = topic_route_data
        .broker_datas
        .iter()
        .find(|broker_data| broker_data.cluster() == cluster_name.as_str() && broker_data.broker_name() == broker_name)
    {
        broker_member_group
            .broker_addrs
            .extend(broker_data.broker_addrs().clone());
    }
    broker_member_group
}

pub(super) fn cluster_names_for_topic_route(
    cluster_info: &ClusterInfo,
    topic_route_data: &TopicRouteData,
) -> HashSet<CheetahString> {
    let mut cluster_names = HashSet::new();
    let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() else {
        return cluster_names;
    };

    for broker_data in &topic_route_data.broker_datas {
        cluster_names.extend(
            cluster_addr_table
                .iter()
                .filter(|(_, broker_names)| broker_names.contains(broker_data.broker_name()))
                .map(|(cluster_name, _)| cluster_name.clone()),
        );
    }

    cluster_names
}

pub(super) fn encode_topic_attributes_like_java(
    attributes: &HashMap<CheetahString, CheetahString>,
) -> Option<CheetahString> {
    if attributes.is_empty() {
        return None;
    }

    let encoded = AttributeParser::parse_to_string(
        &attributes
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<String, String>>(),
    );
    if encoded.is_empty() {
        None
    } else {
        Some(CheetahString::from_string(encoded))
    }
}

pub(super) fn topic_config_u32_to_java_i32(field_name: &'static str, value: u32) -> RocketMQResult<i32> {
    i32::try_from(value).map_err(|_| mq_client_err!(format!("{field_name} value {value} exceeds Java int range")))
}

pub(super) fn create_topic_request_header_like_java(
    default_topic: CheetahString,
    topic_config: &TopicConfig,
) -> RocketMQResult<CreateTopicRequestHeader> {
    Validators::check_topic_config(topic_config)?;
    let topic = topic_config
        .topic_name
        .clone()
        .ok_or_else(|| mq_client_err!("topicConfig.topicName is required".to_string()))?;

    Ok(CreateTopicRequestHeader {
        topic,
        default_topic,
        read_queue_nums: topic_config_u32_to_java_i32("readQueueNums", topic_config.read_queue_nums)?,
        write_queue_nums: topic_config_u32_to_java_i32("writeQueueNums", topic_config.write_queue_nums)?,
        perm: topic_config_u32_to_java_i32("perm", topic_config.perm)?,
        topic_filter_type: CheetahString::from_static_str(topic_config.topic_filter_type.as_str()),
        topic_sys_flag: Some(topic_config_u32_to_java_i32(
            "topicSysFlag",
            topic_config.topic_sys_flag,
        )?),
        order: topic_config.order,
        attributes: encode_topic_attributes_like_java(&topic_config.attributes),
        force: None,
        topic_request_header: None,
    })
}

pub(super) fn query_correction_offset_request(
    topic: CheetahString,
    group: CheetahString,
    filter_groups: Option<Vec<CheetahString>>,
) -> RemotingCommand {
    let filter_groups = filter_groups.map(|groups| {
        CheetahString::from_string(
            groups
                .into_iter()
                .map(|group| group.to_string())
                .collect::<Vec<_>>()
                .join(","),
        )
    });
    let request_header = QueryCorrectionOffsetHeader {
        filter_groups,
        compare_group: group,
        topic,
        topic_request_header: None,
    };
    RemotingCommand::create_request_command(RequestCode::QueryCorrectionOffset, request_header)
}

pub(super) fn split_lite_dispatch_value(value: &str) -> Vec<&str> {
    value
        .split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER)
        .filter(|segment| !segment.is_empty())
        .collect()
}

pub(super) fn parse_lite_order_count_info_like_java(
    order_count_info: Option<&CheetahString>,
    msg_count: usize,
) -> Option<Vec<i32>> {
    let order_count_info = order_count_info?.as_str();
    if order_count_info.trim().is_empty() {
        return None;
    }
    let infos = order_count_info.split(';').collect::<Vec<_>>();
    if infos.len() != msg_count {
        return None;
    }
    Some(infos.into_iter().map(parse_lite_order_count_like_java).collect())
}

pub(super) fn parse_lite_order_count_like_java(info: &str) -> i32 {
    if info.trim().is_empty() {
        return 0;
    }
    if !info.contains(MessageConst::KEY_SEPARATOR) {
        return info.parse::<i32>().unwrap_or_default();
    }
    let split = info.split(MessageConst::KEY_SEPARATOR).collect::<Vec<_>>();
    if split.len() != 3 {
        return 0;
    }
    split[2].parse::<i32>().unwrap_or_default()
}

pub(super) fn filter_retry_topics_like_java(topic_list: &mut TopicList, contain_retry: bool) {
    if contain_retry {
        return;
    }
    topic_list
        .topic_list
        .retain(|topic| !topic.as_str().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX));
}

pub(super) fn controller_leader_address(header: GetMetaDataResponseHeader) -> RocketMQResult<CheetahString> {
    header
        .controller_leader_address
        .ok_or_else(|| mq_client_err!("Controller leader address is not available".to_string()))
}

pub(super) fn controller_config_from_response_body(
    body: &[u8],
) -> RocketMQResult<HashMap<CheetahString, CheetahString>> {
    let body_str = String::from_utf8_lossy(body);
    mix_all::string_to_properties(body_str.as_ref())
        .ok_or_else(|| mq_client_err!("Failed to parse controller config response body".to_string()))
}

pub(super) fn should_fetch_system_topic_list_from_broker(topic_list: &TopicList) -> bool {
    !topic_list.topic_list.is_empty()
        && topic_list
            .broker_addr
            .as_ref()
            .is_some_and(|broker_addr| !broker_addr.as_str().trim().is_empty())
}

pub(super) fn append_system_topic_list_from_broker_like_java(topic_list: &mut TopicList, broker_topic_list: TopicList) {
    if !broker_topic_list.topic_list.is_empty() {
        topic_list.topic_list.extend(broker_topic_list.topic_list);
    }
}

pub(super) async fn merge_system_topic_list_from_broker(
    api: &MQClientAPIImpl,
    topic_list: &mut TopicList,
    timeout_millis: u64,
) -> RocketMQResult<()> {
    if !should_fetch_system_topic_list_from_broker(topic_list) {
        return Ok(());
    }
    let Some(broker_addr) = topic_list.broker_addr.clone() else {
        return Ok(());
    };
    let broker_topic_list = api
        .get_system_topic_list_from_broker(&broker_addr, timeout_millis)
        .await?;
    append_system_topic_list_from_broker_like_java(topic_list, broker_topic_list);
    Ok(())
}

pub(super) fn decode_cluster_acl_version_info_response_body(
    body: Option<&bytes::Bytes>,
) -> RocketMQResult<ClusterAclVersionInfo> {
    let body = body.ok_or_else(|| mq_client_err!("get_broker_cluster_acl_version_info response body is empty"))?;
    SerdeJsonUtils::from_json_slice(body.as_ref())
        .map_err(|error| mq_client_err!(format!("decode ClusterAclVersionInfo failed: {error}")))
}

pub(super) fn admin_message_matches_query(
    topic: &CheetahString,
    key: &CheetahString,
    msg: &MessageExt,
    unique_key_flag: bool,
) -> bool {
    if msg.topic().as_str() != topic.as_str() {
        return false;
    }

    if unique_key_flag {
        if let Some(uniq_id) = MessageClientIDSetter::get_uniq_id(msg) {
            if uniq_id.as_str() == key.as_str() {
                return true;
            }
        }
        return msg.msg_id.as_str() == key.as_str();
    }

    let Some(keys) = MessageTrait::get_keys(msg) else {
        return false;
    };

    keys.split(MessageConst::KEY_SEPARATOR)
        .any(|candidate| candidate == key.as_str())
}

impl MQClientAPIImpl {
    pub(super) async fn invoke_admin_request(
        &self,
        address: &str,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> RocketMQResult<RemotingCommand> {
        let address = CheetahString::from_slice(address);
        self.remoting_client
            .invoke_request(Some(&address), request, timeout_millis)
            .await
    }
}

#[cfg(feature = "admin-full")]
impl MqClientAdminInner for MQClientAPIImpl {
    async fn query_message(
        &self,
        address: &str,
        unique_key_flag: bool,
        decompress_body: bool,
        request_header: QueryMessageRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<MessageExt>> {
        let topic = request_header.topic.clone();
        let key = request_header.key.clone();
        let mut request = RemotingCommand::create_request_command(RequestCode::QueryMessage, request_header);
        request.ensure_ext_fields_initialized();
        request.add_ext_field(
            mix_all::UNIQUE_MSG_QUERY_FLAG,
            CheetahString::from_static_str(if unique_key_flag { "true" } else { "false" }),
        );

        let mut response = self.invoke_admin_request(address, request, timeout_millis).await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let Some(mut body) = response.take_body() else {
                    return Err(mq_client_err!("query_message response body is empty"));
                };
                Ok(MessageDecoder::decodes_batch(&mut body, true, decompress_body)
                    .into_iter()
                    .filter(|msg| admin_message_matches_query(&topic, &key, msg, unique_key_flag))
                    .collect())
            }
            ResponseCode::QueryNotFound => Ok(Vec::new()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |s| s.to_string())
            )),
        }
    }

    async fn get_topic_stats_info(
        &self,
        address: &str,
        request_header: GetTopicStatsInfoRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicStatsTable> {
        let request = RemotingCommand::create_request_command(RequestCode::GetTopicStatsInfo, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return TopicStatsTable::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn query_consume_time_span(
        &self,
        address: &str,
        request_header: QueryConsumeTimeSpanRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<QueueTimeSpan>> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryConsumeTimeSpan, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                let body: QueryConsumeTimeSpanBody = serde_json::from_slice(body.as_ref())?;
                return Ok(body.consume_time_span_set);
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn update_or_create_topic(
        &self,
        address: &str,
        request_header: CreateTopicRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::UpdateAndCreateTopic, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn update_or_create_subscription_group(
        &self,
        address: &str,
        config: SubscriptionGroupConfig,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request =
            RemotingCommand::create_request_command(RequestCode::UpdateAndCreateSubscriptionGroup, EmptyHeader {})
                .set_body(config.encode()?);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn delete_topic_in_broker(
        &self,
        address: &str,
        request_header: DeleteTopicRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::DeleteTopicInBroker, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn delete_topic_in_nameserver(
        &self,
        address: &str,
        request_header: DeleteTopicFromNamesrvRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::DeleteTopicInNamesrv, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn delete_kv_config(
        &self,
        address: &str,
        request_header: DeleteKVConfigRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::DeleteKvConfig, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn delete_subscription_group(
        &self,
        address: &str,
        request_header: DeleteSubscriptionGroupRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::DeleteSubscriptionGroup, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn invoke_broker_to_reset_offset(
        &self,
        address: &str,
        request_header: ResetOffsetRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<HashMap<MessageQueue, i64>> {
        let request = RemotingCommand::create_request_command(RequestCode::InvokeBrokerToResetOffset, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        reset_offset_table_from_response(&response)
    }

    async fn view_message(
        &self,
        address: &str,
        request_header: ViewMessageRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<MessageExt> {
        let request = RemotingCommand::create_request_command(RequestCode::ViewMessageById, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.get_body() {
                    let mut bytes = body.clone();
                    MessageDecoder::decode(&mut bytes, true, true, false, false, false)
                        .ok_or_else(|| mq_client_err!("view_message response body decode failed"))
                } else {
                    Err(mq_client_err!("view_message response body is empty"))
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |s| s.to_string())
            )),
        }
    }

    async fn get_broker_cluster_info(&self, address: &str, timeout_millis: u64) -> RocketMQResult<ClusterInfo> {
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerClusterInfo, EmptyHeader {});
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return ClusterInfo::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn get_consumer_connection_list(
        &self,
        address: &str,
        request_header: GetConsumerConnectionListRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<ConsumerConnection> {
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumerConnectionList, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return ConsumerConnection::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn query_topics_by_consumer(
        &self,
        address: &str,
        request_header: QueryTopicsByConsumerRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicList> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryTopicsByConsumer, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return TopicList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn query_subscription_by_consumer(
        &self,
        address: &str,
        request_header: QuerySubscriptionByConsumerRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<SubscriptionData> {
        let request = RemotingCommand::create_request_command(RequestCode::QuerySubscriptionByConsumer, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let body = response
                .get_body()
                .ok_or_else(|| mq_client_err!("query_subscription_by_consumer response body is empty"))?;
            let response_body: QuerySubscriptionResponseBody = serde_json::from_slice(body.as_ref())?;
            return response_body
                .subscription_data
                .ok_or_else(|| mq_client_err!("query_subscription_by_consumer response subscriptionData is empty"));
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn get_consume_stats(
        &self,
        address: &str,
        request_header: GetConsumeStatsRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<ConsumeStats> {
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumeStats, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return ConsumeStats::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn query_topic_consume_by_who(
        &self,
        address: &str,
        request_header: QueryTopicConsumeByWhoRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<GroupList> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryTopicConsumeByWho, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return GroupList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn get_consumer_running_info(
        &self,
        address: &str,
        request_header: GetConsumerRunningInfoRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<ConsumerRunningInfo> {
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumerRunningInfo, request_header);
        let mut response = self.invoke_admin_request(address, request, timeout_millis).await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let Some(body) = response.take_body() else {
                    return Err(mq_client_err!("get_consumer_running_info response body is empty"));
                };
                ConsumerRunningInfo::decode(body.as_ref())
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |s| s.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    async fn consume_message_directly(
        &self,
        address: &str,
        request_header: ConsumeMessageDirectlyResultRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<ConsumeMessageDirectlyResult> {
        let request = RemotingCommand::create_request_command(RequestCode::ConsumeMessageDirectly, request_header);
        let mut response = self.invoke_admin_request(address, request, timeout_millis).await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let Some(body) = response.take_body() else {
                    return Err(mq_client_err!("consume_message_directly response body is empty"));
                };
                ConsumeMessageDirectlyResult::decode(body.as_ref())
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |s| s.to_string())
            )),
        }
    }
}
