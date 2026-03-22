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

use tonic::Request;
use tonic::Status;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct GrpcTransportContext {
    local_addr: String,
}

impl GrpcTransportContext {
    pub(crate) fn new(local_addr: SocketAddr) -> Self {
        Self {
            local_addr: local_addr.to_string(),
        }
    }

    pub(crate) fn local_addr(&self) -> &str {
        self.local_addr.as_str()
    }
}

pub(crate) fn ingress_context_interceptor(local_addr: SocketAddr) -> impl tonic::service::Interceptor + Clone {
    move |mut request: Request<()>| -> Result<Request<()>, Status> {
        request.extensions_mut().insert(GrpcTransportContext::new(local_addr));
        Ok(request)
    }
}

pub(crate) fn request_local_addr<T>(request: &Request<T>) -> Option<&str> {
    request
        .extensions()
        .get::<GrpcTransportContext>()
        .map(GrpcTransportContext::local_addr)
}
