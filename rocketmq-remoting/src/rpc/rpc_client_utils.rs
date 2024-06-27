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

use std::any::Any;

use bytes::Bytes;

use crate::protocol::remoting_command::RemotingCommand;
use crate::rpc::rpc_request::RpcRequest;
use crate::rpc::rpc_response::RpcResponse;

pub struct RpcClientUtils;

impl RpcClientUtils {
    pub fn create_command_for_rpc_request(rpc_request: RpcRequest) -> RemotingCommand {
        let cmd = RemotingCommand::create_request_command(rpc_request.code, rpc_request.header);
        cmd.set_body(Self::encode_body(rpc_request.body))
    }

    pub fn create_command_for_rpc_response(mut rpc_response: RpcResponse) -> RemotingCommand {
        let mut cmd = match rpc_response.header.take() {
            None => RemotingCommand::create_response_command_with_code(rpc_response.code),
            Some(value) => RemotingCommand::create_response_command()
                .set_command_custom_header_origin(Some(value)),
        };
        match rpc_response.exception {
            None => {}
            Some(value) => cmd.set_remark_ref(Some(value.1.clone())),
        }
        cmd.set_body(Self::encode_body(rpc_response.body))
    }

    pub fn encode_body(_body: Option<Box<dyn Any>>) -> Option<Bytes> {
        /*if body.is_none() {
            return None;
        }
        let body = body.unwrap();
        if body.is::<Bytes>() {
            return Some(body.downcast_ref::<Bytes>().unwrap().clone());
        } else if body.is::<Vec<u8>>() {
            return Some(Bytes::from(
                body.downcast_ref::<Vec<u8>>().unwrap().as_ref(),
            ));
        }
        /*else if body.is::<dyn RemotingSerializable<Output = Self>>() {
             return Some(Bytes::from(
                body.downcast_ref::<dyn RemotingSerializable<Output = Self>>()
                    .unwrap()
                    .encode(),
            ));

        }*/
        else {
            None
        }*/
        None
    }
}
