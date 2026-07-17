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

use std::net::SocketAddr;

use rocketmq_proxy_core::GrpcTransportContext;
use tonic::Request;
use tonic::Status;

pub(crate) fn ingress_context_interceptor(local_addr: SocketAddr) -> impl tonic::service::Interceptor + Clone {
    move |mut request: Request<()>| -> Result<Request<()>, Status> {
        request.extensions_mut().insert(GrpcTransportContext::new(local_addr));
        Ok(request)
    }
}
