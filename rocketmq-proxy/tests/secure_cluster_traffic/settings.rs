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

use super::*;
use hmac::{digest::KeyInit, Hmac, Mac};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::Read,
    net::IpAddr,
    path::PathBuf,
};
use tonic::{
    codegen::http::Uri,
    metadata::MetadataValue,
    transport::{Certificate, ClientTlsConfig, Endpoint},
    Request,
};

pub(super) type TestResult<T> = Result<T, &'static str>;

pub(super) struct Settings {
    pub(super) run_id: String,
    pub(super) endpoints: Vec<EndpointSettings>,
    pub(super) ca_pem: Vec<u8>,
    access_key: String,
    secret_key: String,
    report_path: PathBuf,
}

impl Settings {
    pub(super) fn from_env() -> TestResult<Self> {
        if required("ROCKETMQ_PROXY_LIVE_SECURE_TRAFFIC", 1)? != "1" {
            return Err("explicit_secure_traffic_authorization_required");
        }
        let run_id = required("ROCKETMQ_PROXY_LIVE_RUN_ID", 36)?;
        if !uuid::Uuid::parse_str(&run_id).is_ok_and(|value| !value.is_nil() && value.to_string() == run_id) {
            return Err("canonical_task_run_id_required");
        }
        let namespace = required("ROCKETMQ_PROXY_LIVE_NAMESPACE", 63)?;
        let service = required("ROCKETMQ_PROXY_LIVE_SERVICE", 63)?;
        if !dns_label(&namespace)
            || !dns_label(&service)
            || ["default", "kube-system", "kube-public", "kube-node-lease"].contains(&namespace.as_str())
        {
            return Err("exact_dedicated_namespace_and_service_required");
        }
        let endpoints = [
            ("pod_a", "ROCKETMQ_PROXY_LIVE_POD_A_URL"),
            ("pod_b", "ROCKETMQ_PROXY_LIVE_POD_B_URL"),
            ("service", "ROCKETMQ_PROXY_LIVE_SERVICE_URL"),
        ]
        .into_iter()
        .map(|(kind, key)| EndpointSettings::parse(kind, required(key, 1024)?, &namespace, &service))
        .collect::<TestResult<Vec<_>>>()?;
        if endpoints[0].host == endpoints[1].host {
            return Err("two_distinct_actual_pod_ip_endpoints_required");
        }
        let ca_path = PathBuf::from(required("ROCKETMQ_PROXY_LIVE_CA_FILE", 4096)?);
        if !ca_path.is_absolute() {
            return Err("explicit_task_ca_file_required");
        }
        let file = File::open(ca_path).map_err(|_| "task_ca_file_unavailable")?;
        if !file.metadata().map_err(|_| "task_ca_metadata_failed")?.is_file() {
            return Err("task_ca_must_be_a_regular_file");
        }
        let mut ca_pem = Vec::new();
        file.take(64 * 1024 + 1)
            .read_to_end(&mut ca_pem)
            .map_err(|_| "task_ca_read_failed")?;
        if ca_pem.is_empty() || ca_pem.len() > 64 * 1024 {
            return Err("task_ca_outside_bounded_size");
        }
        let access_key = required("ROCKETMQ_PROXY_LIVE_ACCESS_KEY", 256)?;
        if !access_key
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || value == b'_' || value == b'-')
        {
            return Err("explicit_safe_task_access_key_required");
        }
        let report_path = PathBuf::from(required("ROCKETMQ_PROXY_LIVE_PRIVATE_REPORT", 4096)?);
        if !report_path.is_absolute() {
            return Err("absolute_private_report_required");
        }
        Ok(Self {
            run_id,
            endpoints,
            ca_pem,
            access_key,
            secret_key: required("ROCKETMQ_PROXY_LIVE_SECRET_KEY", 1024)?,
            report_path,
        })
    }

    pub(super) fn open_report(&self) -> std::io::Result<File> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(&self.report_path)
    }

    pub(super) fn request<T>(&self, body: T, client_id: &str, signed: bool) -> TestResult<Request<T>> {
        let mut request = Request::new(body);
        request.set_timeout(RPC_TIMEOUT);
        for key in ["x-mq-client-id", "channel-id"] {
            request.metadata_mut().insert(
                key,
                MetadataValue::try_from(client_id).map_err(|_| "client_metadata_invalid")?,
            );
        }
        if signed {
            let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
            let mut signer =
                Hmac::<Sha256>::new_from_slice(self.secret_key.as_bytes()).map_err(|_| "task_signing_key_invalid")?;
            signer.update(timestamp.as_bytes());
            let signature = hex::encode(signer.finalize().into_bytes());
            let authorization = format!(
                "MQv2-HMAC-SHA256 Credential={}, SignedHeaders=x-mq-date-time, Signature={signature}",
                self.access_key,
            );
            request.metadata_mut().insert(
                "x-mq-date-time",
                MetadataValue::try_from(timestamp).map_err(|_| "timestamp_metadata_invalid")?,
            );
            request.metadata_mut().insert(
                "authorization",
                MetadataValue::try_from(authorization).map_err(|_| "authorization_metadata_invalid")?,
            );
        }
        Ok(request)
    }
}

pub(super) struct EndpointSettings {
    pub(super) kind: &'static str,
    url: String,
    host: String,
    scheme: i32,
}

impl EndpointSettings {
    fn parse(kind: &'static str, url: String, namespace: &str, service: &str) -> TestResult<Self> {
        let uri: Uri = url.parse().map_err(|_| "secure_endpoint_invalid")?;
        if uri.scheme_str() != Some("https")
            || uri.port_u16() != Some(8081)
            || uri
                .path_and_query()
                .is_some_and(|value| !matches!(value.as_str(), "" | "/"))
            || uri.authority().is_none_or(|authority| authority.as_str().contains('@'))
        {
            return Err("exact_https_proxy_endpoint_required");
        }
        let host = uri
            .host()
            .ok_or("endpoint_host_missing")?
            .trim_matches(['[', ']'])
            .to_owned();
        let scheme = if kind == "service" {
            let expected = format!("{service}.{namespace}.svc");
            if host != expected && host != format!("{expected}.cluster.local") {
                return Err("service_endpoint_not_the_explicit_task_service");
            }
            v2::AddressScheme::DomainName as i32
        } else {
            let address = host.parse::<IpAddr>().map_err(|_| "actual_pod_ip_endpoint_required")?;
            if address.is_loopback() || address.is_unspecified() {
                return Err("pod_endpoint_must_not_be_a_local_forward");
            }
            match address {
                IpAddr::V4(_) => v2::AddressScheme::IPv4 as i32,
                IpAddr::V6(_) => v2::AddressScheme::IPv6 as i32,
            }
        };
        Ok(Self {
            kind,
            url,
            host,
            scheme,
        })
    }

    pub(super) fn digest(&self) -> String {
        hex::encode(Sha256::digest(self.url.as_bytes()))
    }

    pub(super) fn proto(&self) -> v2::Endpoints {
        v2::Endpoints {
            scheme: self.scheme,
            addresses: vec![v2::Address {
                host: self.host.clone(),
                port: 8081,
            }],
        }
    }

    pub(super) async fn connect(&self, ca: &[u8]) -> TestResult<Channel> {
        // No domain_name override: rustls must validate the URL's actual Pod IP
        // or Service DNS SAN against the explicitly supplied task CA.
        let endpoint = Endpoint::from_shared(self.url.clone())
            .map_err(|_| "https_endpoint_invalid")?
            .connect_timeout(Duration::from_secs(5))
            .timeout(RPC_TIMEOUT)
            .tls_config(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca)))
            .map_err(|_| "task_ca_tls_configuration_failed")?;
        endpoint
            .connect()
            .await
            .map_err(|_| "verified_tls_endpoint_connection_failed")
    }
}

pub(super) fn required(key: &str, maximum: usize) -> TestResult<String> {
    std::env::var(key)
        .ok()
        .filter(|value| {
            !value.is_empty() && value.len() <= maximum && value.trim() == value && !value.chars().any(char::is_control)
        })
        .ok_or("required_private_traffic_setting_missing_or_invalid")
}

pub(super) fn require_flag(key: &str) -> TestResult<()> {
    if required(key, 1)? != "1" {
        return Err("explicit_probe_mode_authorization_required");
    }
    Ok(())
}

pub(super) fn digest(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

fn dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        && value.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
}
