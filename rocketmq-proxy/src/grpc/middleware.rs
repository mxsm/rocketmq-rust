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
#[cfg(feature = "tls")]
use rocketmq_proxy_core::VerifiedTlsIdentity;
use tonic::Request;
use tonic::Status;

pub(crate) fn ingress_context_interceptor(local_addr: SocketAddr) -> impl tonic::service::Interceptor + Clone {
    move |mut request: Request<()>| -> Result<Request<()>, Status> {
        request.extensions_mut().insert(GrpcTransportContext::new(local_addr));
        attach_verified_tls_identity(&mut request)?;
        Ok(request)
    }
}

#[cfg(feature = "tls")]
fn attach_verified_tls_identity(request: &mut Request<()>) -> Result<(), Status> {
    use tonic::transport::server::TcpConnectInfo;
    use tonic::transport::server::TlsConnectInfo;

    let Some(connect_info) = request.extensions().get::<TlsConnectInfo<TcpConnectInfo>>() else {
        return Ok(());
    };
    let Some(certificates) = connect_info.peer_certs() else {
        return Ok(());
    };
    let Some(leaf) = certificates.first() else {
        return Err(Status::unauthenticated("verified client certificate chain is empty"));
    };
    request
        .extensions_mut()
        .insert(VerifiedTlsIdentity::from_leaf_certificate_der(leaf.as_ref().to_vec()));
    Ok(())
}

#[cfg(not(feature = "tls"))]
fn attach_verified_tls_identity(_request: &mut Request<()>) -> Result<(), Status> {
    Ok(())
}
