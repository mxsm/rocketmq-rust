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

//! Durable reverse channel used by connector-initiated HTTP/2 sessions.
//!
//! The control plane appends bounded read-only query and cancellation commands
//! to PostgreSQL. Connectors poll the log, submit idempotent responses, and can
//! resume from the last contiguous response after reconnecting.

mod http;
mod model;
mod repository;
mod service;

pub(crate) use http::PostgresConnectorChannelService;
pub(crate) use http::router;
pub(crate) use model::ConnectorChannelStatus;
pub(crate) use model::ConnectorCommand;
pub(crate) use model::ConnectorLiveness;
pub(crate) use model::ConnectorPrincipal;
pub(crate) use model::MAX_SOURCES;
pub(crate) use model::PollRequest;
pub(crate) use model::PollResponse;
pub(crate) use model::RegisterAcknowledgement;
pub(crate) use model::ResponseDisposition;
pub(crate) use model::SessionScope;
pub(crate) use model::channel_schema;
pub(crate) use model::validate_channel_schema;
pub(crate) use model::validate_poll_request;
pub(crate) use model::validate_response;
pub(crate) use repository::ConnectorChannelStore;
pub(crate) use repository::PostgresConnectorChannelStore;
pub(crate) use service::ConnectorChannelService;
