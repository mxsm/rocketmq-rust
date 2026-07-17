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

use crate::GrpcTransportContext;

/// Creates an interceptor that records the listening address for every ingress request.
pub fn ingress_context_interceptor(local_addr: SocketAddr) -> impl tonic::service::Interceptor + Clone {
    move |mut request: Request<()>| -> Result<Request<()>, Status> {
        request.extensions_mut().insert(GrpcTransportContext::new(local_addr));
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use tonic::service::Interceptor;

    use super::*;

    #[test]
    fn interceptor_attaches_transport_context() {
        let local_addr = "127.0.0.1:8081".parse().expect("valid address");
        let mut interceptor = ingress_context_interceptor(local_addr);

        let request = interceptor.call(Request::new(())).expect("intercepted request");
        let context = request
            .extensions()
            .get::<GrpcTransportContext>()
            .expect("transport context");

        assert_eq!(context.local_addr(), local_addr.to_string());
    }
}
