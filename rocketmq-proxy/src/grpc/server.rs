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

use std::future::Future;
use std::sync::Arc;

use tokio::sync::watch;
use tonic::transport::Server;

use crate::config::ProxyConfig;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::grpc::service::ProxyGrpcService;
use crate::processor::MessagingProcessor;
use crate::proto::v2::messaging_service_server::MessagingServiceServer;

pub async fn serve<P, F>(config: Arc<ProxyConfig>, service: ProxyGrpcService<P>, shutdown: F) -> ProxyResult<()>
where
    P: MessagingProcessor + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let addr = config.grpc.socket_addr()?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let shutdown_signal = shutdown_tx.clone();
    let housekeeping_service = service.clone();
    tokio::spawn(async move {
        let mut shutdown_rx = shutdown_rx;
        housekeeping_service
            .run_housekeeping_until(async move {
                loop {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    if shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await;
    });
    let service = MessagingServiceServer::new(service)
        .max_decoding_message_size(config.grpc.max_decoding_message_size)
        .max_encoding_message_size(config.grpc.max_encoding_message_size);

    let result = Server::builder()
        .concurrency_limit_per_connection(config.grpc.concurrency_limit_per_connection)
        .add_service(service)
        .serve_with_shutdown(addr, async move {
            shutdown.await;
            let _ = shutdown_signal.send(true);
        })
        .await;
    let _ = shutdown_tx.send(true);

    result.map_err(|error| ProxyError::Transport {
        message: format!("proxy gRPC server failed: {error}"),
    })
}
