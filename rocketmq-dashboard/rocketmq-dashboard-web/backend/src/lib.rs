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
#![recursion_limit = "256"]

pub mod admin;
pub mod api;
pub mod config;
pub mod error;
pub mod middleware;
pub mod model;
pub mod service;
pub mod state;

use crate::api::build_router;
use crate::config::AppConfig;
use crate::state::AppState;
use std::net::SocketAddr;
use tokio::net::TcpListener;

pub async fn run(config: AppConfig) -> anyhow::Result<()> {
    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port).parse()?;
    let state = AppState::try_new(config).await?;
    let app = build_router(state);
    let listener = TcpListener::bind(addr).await?;

    tracing::info!("RocketMQ Dashboard Web backend listening on http://{addr}");
    axum::serve(listener, app).await?;

    Ok(())
}
