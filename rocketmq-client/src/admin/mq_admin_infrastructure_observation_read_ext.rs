// Copyright 2026 The RocketMQ Rust Authors
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

//! Exact-target infrastructure observations for the `admin-read` capability.

use std::collections::HashMap;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::body::broker_replicas_info::BrokerReplicasInfo;
use rocketmq_protocol::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_protocol::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;

use super::default_mq_admin_ext::DefaultMQAdminExt;

/// Stable failure classification for exact infrastructure observation RPCs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InfrastructureObservationReadErrorCode {
    SourceUnavailable,
    Timeout,
    PermissionDenied,
    NotFound,
    RateLimited,
    InvalidResponse,
}

/// Address-free error returned by infrastructure observation RPCs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InfrastructureObservationReadError {
    code: InfrastructureObservationReadErrorCode,
    retryable: bool,
}

impl InfrastructureObservationReadError {
    pub const fn new(code: InfrastructureObservationReadErrorCode, retryable: bool) -> Self {
        Self { code, retryable }
    }

    pub const fn code(self) -> InfrastructureObservationReadErrorCode {
        self.code
    }

    pub const fn retryable(self) -> bool {
        self.retryable
    }
}

impl std::fmt::Display for InfrastructureObservationReadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "infrastructure observation failed: {:?}", self.code)
    }
}

impl std::error::Error for InfrastructureObservationReadError {}

/// Additive read-only access to four existing exact-target admin RPCs.
///
/// Target endpoints are accepted only from trusted adapters. Neither returned
/// errors nor their `Debug` representation retain endpoint or remote detail.
#[allow(async_fn_in_trait)]
pub trait MQAdminInfrastructureObservationReadExt: Send {
    async fn broker_ha_runtime_at(
        &self,
        broker_endpoint: CheetahString,
    ) -> Result<HARuntimeInfo, InfrastructureObservationReadError>;

    async fn controller_sync_state_at(
        &self,
        controller_endpoint: CheetahString,
        broker_names: Vec<CheetahString>,
    ) -> Result<BrokerReplicasInfo, InfrastructureObservationReadError>;

    async fn controller_metadata_at(
        &self,
        controller_endpoint: CheetahString,
    ) -> Result<GetMetaDataResponseHeader, InfrastructureObservationReadError>;

    async fn nameserver_config_at(
        &self,
        nameserver_endpoint: CheetahString,
    ) -> Result<HashMap<CheetahString, CheetahString>, InfrastructureObservationReadError>;
}

impl MQAdminInfrastructureObservationReadExt for DefaultMQAdminExt {
    async fn broker_ha_runtime_at(
        &self,
        broker_endpoint: CheetahString,
    ) -> Result<HARuntimeInfo, InfrastructureObservationReadError> {
        self.inner()
            .mq_client_api()
            .map_err(sanitized_error)?
            .get_broker_ha_status(
                broker_endpoint,
                self.inner().remoting_timeout_millis().map_err(sanitized_error)?,
            )
            .await
            .map_err(sanitized_error)
    }

    async fn controller_sync_state_at(
        &self,
        controller_endpoint: CheetahString,
        broker_names: Vec<CheetahString>,
    ) -> Result<BrokerReplicasInfo, InfrastructureObservationReadError> {
        self.inner()
            .mq_client_api()
            .map_err(sanitized_error)?
            .get_in_sync_state_data_at(
                controller_endpoint,
                broker_names,
                self.inner().remoting_timeout_millis().map_err(sanitized_error)?,
            )
            .await
            .map_err(sanitized_error)
    }

    async fn controller_metadata_at(
        &self,
        controller_endpoint: CheetahString,
    ) -> Result<GetMetaDataResponseHeader, InfrastructureObservationReadError> {
        self.inner()
            .mq_client_api()
            .map_err(sanitized_error)?
            .get_controller_metadata(
                controller_endpoint,
                self.inner().remoting_timeout_millis().map_err(sanitized_error)?,
            )
            .await
            .map_err(sanitized_error)
    }

    async fn nameserver_config_at(
        &self,
        nameserver_endpoint: CheetahString,
    ) -> Result<HashMap<CheetahString, CheetahString>, InfrastructureObservationReadError> {
        let mut responses = self
            .inner()
            .mq_client_api()
            .map_err(sanitized_error)?
            .get_name_server_config(
                Some(vec![nameserver_endpoint.clone()]),
                Duration::from_millis(self.inner().remoting_timeout_millis().map_err(sanitized_error)?),
            )
            .await
            .map_err(sanitized_error)?
            .ok_or_else(invalid_response)?;
        if responses.len() != 1 {
            return Err(invalid_response());
        }
        responses.remove(&nameserver_endpoint).ok_or_else(invalid_response)
    }
}

fn sanitized_error(error: RocketMQError) -> InfrastructureObservationReadError {
    let view = error.boundary_view();
    let code = match view.http().status.as_u16() {
        401 | 403 => InfrastructureObservationReadErrorCode::PermissionDenied,
        404 => InfrastructureObservationReadErrorCode::NotFound,
        408 | 504 => InfrastructureObservationReadErrorCode::Timeout,
        429 => InfrastructureObservationReadErrorCode::RateLimited,
        400 | 413 | 422 => InfrastructureObservationReadErrorCode::InvalidResponse,
        _ => InfrastructureObservationReadErrorCode::SourceUnavailable,
    };
    InfrastructureObservationReadError::new(code, view.is_retryable())
}

const fn invalid_response() -> InfrastructureObservationReadError {
    InfrastructureObservationReadError::new(InfrastructureObservationReadErrorCode::InvalidResponse, false)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use rocketmq_error::Error;
    use rocketmq_error::TRANSPORT_CONNECTION_FAILED;

    #[test]
    fn public_error_is_typed_and_never_retains_endpoint_detail() {
        let error = sanitized_error(RocketMQError::Network(Arc::new(Error::caused_by(
            &TRANSPORT_CONNECTION_FAILED,
            std::io::Error::other("private-controller.internal:9878 at 10.23.45.67:10911"),
        ))));
        assert_eq!(error.code(), InfrastructureObservationReadErrorCode::SourceUnavailable);
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains("10.23.45.67"));
        assert!(!rendered.contains("private-controller"));
    }
}
