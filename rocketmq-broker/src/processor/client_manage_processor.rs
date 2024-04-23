/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 use std::{collections::HashMap, sync::Arc};

 use rocketmq_remoting::{
     code::request_code::RequestCode,
     protocol::{
         header::unregister_client_request_header::UnregisterClientRequestHeader,
         remoting_command::RemotingCommand,
     },
     runtime::server::ConnectionHandlerContext,
 };
 
 use crate::client::{
     client_channel_info::ClientChannelInfo, manager::producer_manager::ProducerManager,
 };
 
 #[derive(Default)]
 pub struct ClientManageProcessor {
     consumer_group_heartbeat_table:
         HashMap<String /* ConsumerGroup */, i32 /* HeartbeatFingerprint */>,
     producer_manager: Arc<ProducerManager>,
 }
 
 impl ClientManageProcessor {
     pub fn new(producer_manager: Arc<ProducerManager>) -> Self {
         Self {
             consumer_group_heartbeat_table: HashMap::new(),
             producer_manager,
         }
     }
 }
 
 impl ClientManageProcessor {
     pub async fn process_request(
         &self,
         ctx: ConnectionHandlerContext<'_>,
         request_code: RequestCode,
         request: RemotingCommand,
     ) -> RemotingCommand {
         let response = match request_code {
             RequestCode::HeartBeat => self.heart_beat(ctx, request),
             RequestCode::UnregisterClient => self.unregister_client(ctx, request),
             RequestCode::CheckClientConfig => {
                 unimplemented!("CheckClientConfig")
             }
             _ => {
                 unimplemented!("CheckClientConfig")
             }
         };
         response.unwrap()
     }
 }
 
 impl ClientManageProcessor {
     fn unregister_client(
         &self,
         ctx: ConnectionHandlerContext<'_>,
         request: RemotingCommand,
     ) -> Option<RemotingCommand> {
         let request_header = request
             .decode_command_custom_header::<UnregisterClientRequestHeader>()
             .unwrap();
 
         let client_channel_info = ClientChannelInfo::new(
             ctx.remoting_address().to_string(),
             request_header.client_id.clone(),
             request.language(),
             request.version(),
         );
 
         if let Some(ref group) = request_header.producer_group {
             self.producer_manager
                 .unregister_producer(group, &client_channel_info);
         }
 
         if let Some(ref _group) = request_header.consumer_group {
             unimplemented!()
         }
 
         Some(RemotingCommand::create_response_command())
     }
 
     fn heart_beat(
         &self,
         ctx: ConnectionHandlerContext<'_>,
         request: RemotingCommand,
     ) -> Option<RemotingCommand> {
         None
     }
 }
 