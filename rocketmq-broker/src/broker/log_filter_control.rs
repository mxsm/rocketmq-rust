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

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::TimeUtils::current_millis;
use rocketmq_observability::LogFilterHandle;
use rocketmq_observability::LogFilterInputs;
use rocketmq_observability::LogFilterReloadRequest;
use rocketmq_observability::LogFilterResolver;
use rocketmq_observability::ResolvedLogFilter;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::ServiceContext;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::time::Instant;

pub(crate) const LOG_FILTER_KEY: &str = "logFilter";
pub(crate) const LOG_FILTER_REASON_KEY: &str = "logFilterReason";
pub(crate) const LOG_FILTER_TTL_KEY: &str = "logFilterTtlSeconds";
pub(crate) const LOG_FILTER_REQUEST_ID_KEY: &str = "logFilterRequestId";
pub(crate) const LOG_FILTER_RESTORE_KEY: &str = "logFilterRestore";
pub(crate) const DEFAULT_LOG_FILTER_TTL_SECONDS: u64 = 1_800;
pub(crate) const MIN_LOG_FILTER_TTL_SECONDS: u64 = 60;
pub(crate) const MAX_LOG_FILTER_TTL_SECONDS: u64 = 7_200;

pub(crate) const LOG_FILTER_KEYS: [&str; 5] = [
    LOG_FILTER_KEY,
    LOG_FILTER_REASON_KEY,
    LOG_FILTER_TTL_KEY,
    LOG_FILTER_REQUEST_ID_KEY,
    LOG_FILTER_RESTORE_KEY,
];

#[derive(Debug, Error)]
pub(crate) enum BrokerLogFilterControlError {
    #[error("log filter audit failed: {0}")]
    Audit(String),
    #[error("log filter TTL scheduling failed: {0}")]
    Scheduling(String),
    #[error("log filter reload failed: {0}")]
    Reload(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BrokerLogFilterRequest {
    pub filter: Option<String>,
    pub reason: String,
    pub ttl_seconds: u64,
    pub request_id: String,
    pub operator: String,
    pub source_ip: String,
    pub restore: bool,
    pub super_user: bool,
}

impl BrokerLogFilterRequest {
    pub(crate) fn parse(
        properties: &std::collections::HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString>,
        operator: &str,
        source_ip: impl Into<String>,
        super_user: bool,
    ) -> Result<Self, String> {
        reject_unknown_or_mixed_keys(properties)?;
        let operator = required_bounded("AccessKey", operator, 256)?;
        let reason = required_property(properties, LOG_FILTER_REASON_KEY, 512)?;
        let request_id = required_property(properties, LOG_FILTER_REQUEST_ID_KEY, 128)?;
        let restore = match properties.get(LOG_FILTER_RESTORE_KEY) {
            Some(value) if value.eq_ignore_ascii_case("true") => true,
            Some(value) if value.eq_ignore_ascii_case("false") => false,
            Some(_) => return Err(format!("{LOG_FILTER_RESTORE_KEY} must be true or false")),
            None => false,
        };
        let ttl_seconds = match properties.get(LOG_FILTER_TTL_KEY) {
            Some(value) => value
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("{LOG_FILTER_TTL_KEY} must be an integer"))?,
            None => DEFAULT_LOG_FILTER_TTL_SECONDS,
        };
        if !(MIN_LOG_FILTER_TTL_SECONDS..=MAX_LOG_FILTER_TTL_SECONDS).contains(&ttl_seconds) {
            return Err(format!(
                "{LOG_FILTER_TTL_KEY} must be between {MIN_LOG_FILTER_TTL_SECONDS} and {MAX_LOG_FILTER_TTL_SECONDS}"
            ));
        }

        let filter = properties
            .get(LOG_FILTER_KEY)
            .map(|value| required_bounded(LOG_FILTER_KEY, value, 4_096))
            .transpose()?;
        if restore {
            if filter.is_some() {
                return Err(format!(
                    "{LOG_FILTER_RESTORE_KEY}=true cannot be combined with {LOG_FILTER_KEY}"
                ));
            }
        } else {
            let filter = filter
                .as_deref()
                .ok_or_else(|| format!("{LOG_FILTER_KEY} is required"))?;
            validate_filter_policy(filter, super_user)?;
        }

        Ok(Self {
            filter,
            reason,
            ttl_seconds,
            request_id,
            operator,
            source_ip: source_ip.into(),
            restore,
            super_user,
        })
    }
}

fn reject_unknown_or_mixed_keys(
    properties: &std::collections::HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString>,
) -> Result<(), String> {
    let mut unsupported = properties
        .keys()
        .filter(|key| !LOG_FILTER_KEYS.contains(&key.as_str()))
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    unsupported.sort();
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "log filter updates cannot be mixed with broker config keys: {}",
            unsupported.join(",")
        ))
    }
}

fn required_property(
    properties: &std::collections::HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString>,
    key: &'static str,
    max_len: usize,
) -> Result<String, String> {
    let value = properties.get(key).ok_or_else(|| format!("{key} is required"))?;
    required_bounded(key, value, max_len)
}

fn required_bounded(key: &'static str, value: &str, max_len: usize) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("{key} must not be blank"));
    }
    if value.len() > max_len {
        return Err(format!("{key} exceeds the maximum length of {max_len}"));
    }
    Ok(value.to_owned())
}

fn validate_filter_policy(filter: &str, super_user: bool) -> Result<(), String> {
    LogFilterResolver::resolve(LogFilterInputs {
        runtime: Some(filter),
        ..LogFilterInputs::default()
    })
    .map_err(|error| error.to_string())?;
    if super_user {
        return Ok(());
    }

    let mut has_info_baseline = false;
    for directive in filter.split(',').map(str::trim) {
        if directive.contains(['[', ']', '{', '}']) {
            return Err("span and field directives require a super-user break-glass request".to_string());
        }
        let Some((target, level)) = directive.split_once('=') else {
            if directive.eq_ignore_ascii_case("info") {
                has_info_baseline = true;
                continue;
            }
            return Err("non-super-users must keep the global log baseline at info".to_string());
        };
        if !target.trim().starts_with("rocketmq_") {
            return Err("non-super-users may only target rocketmq_* modules".to_string());
        }
        if !matches!(
            level.trim().to_ascii_lowercase().as_str(),
            "off" | "error" | "warn" | "info" | "debug" | "trace"
        ) {
            return Err("unsupported target log level".to_string());
        }
    }
    if !has_info_baseline {
        return Err("non-super-users must include an explicit info baseline".to_string());
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct ActiveOverride {
    generation: u64,
    deadline: Instant,
    audit: AuditContext,
}

#[derive(Debug, Clone)]
struct AuditContext {
    request_id: String,
    operator: String,
    source_ip: String,
    reason: String,
    ttl_seconds: u64,
    super_user: bool,
}

impl From<&BrokerLogFilterRequest> for AuditContext {
    fn from(request: &BrokerLogFilterRequest) -> Self {
        Self {
            request_id: request.request_id.clone(),
            operator: request.operator.clone(),
            source_ip: request.source_ip.clone(),
            reason: request.reason.clone(),
            ttl_seconds: request.ttl_seconds,
            super_user: request.super_user,
        }
    }
}

enum TtlCommand {
    Set(ActiveOverride),
    Clear,
}

pub(crate) struct BrokerLogFilterControl {
    handle: LogFilterHandle,
    baseline: ResolvedLogFilter,
    blocking: BlockingExecutor,
    audit_path: PathBuf,
    ttl_sender: mpsc::Sender<TtlCommand>,
    operation: Arc<Mutex<()>>,
    active: Arc<std::sync::Mutex<Option<ActiveOverride>>>,
    next_generation: AtomicU64,
}

impl BrokerLogFilterControl {
    pub(crate) fn start(
        handle: LogFilterHandle,
        service_context: &ServiceContext,
        store_path_root_dir: &str,
    ) -> Result<Arc<Self>, BrokerLogFilterControlError> {
        let baseline = handle.current();
        let audit_path = PathBuf::from(store_path_root_dir)
            .join("logs")
            .join("log-filter-audit.jsonl");
        let (ttl_sender, ttl_receiver) = mpsc::channel(16);
        let operation = Arc::new(Mutex::new(()));
        let active = Arc::new(std::sync::Mutex::new(None));
        let cancellation = service_context.task_group().cancellation_token();
        service_context
            .spawn_service(
                "broker.log-filter-ttl",
                run_ttl_controller(TtlControllerContext {
                    handle: handle.clone(),
                    baseline: baseline.clone(),
                    blocking: service_context.blocking().clone(),
                    audit_path: audit_path.clone(),
                    operation: Arc::clone(&operation),
                    active: Arc::clone(&active),
                    receiver: ttl_receiver,
                    cancellation,
                }),
            )
            .map_err(|error| BrokerLogFilterControlError::Scheduling(error.to_string()))?;

        Ok(Arc::new(Self {
            handle,
            baseline,
            blocking: service_context.blocking().clone(),
            audit_path,
            ttl_sender,
            operation,
            active,
            next_generation: AtomicU64::new(1),
        }))
    }

    pub(crate) fn baseline(&self) -> &ResolvedLogFilter {
        &self.baseline
    }

    pub(crate) async fn apply(
        &self,
        request: BrokerLogFilterRequest,
    ) -> Result<ResolvedLogFilter, BrokerLogFilterControlError> {
        let _operation = self.operation.lock().await;
        let old_filter = self.handle.current();
        let target_filter = if request.restore {
            self.baseline.clone()
        } else {
            LogFilterResolver::resolve(LogFilterInputs {
                runtime: request.filter.as_deref(),
                ..LogFilterInputs::default()
            })
            .map_err(|error| BrokerLogFilterControlError::Reload(error.to_string()))?
        };
        let audit = AuditContext::from(&request);
        self.append_audit(AuditRecord::request(
            "intent",
            &audit,
            old_filter.filter(),
            target_filter.filter(),
            "pending",
        ))
        .await?;

        let previous = self.active.lock().unwrap_or_else(|error| error.into_inner()).clone();
        let scheduled = if request.restore {
            None
        } else {
            Some(ActiveOverride {
                generation: self.next_generation.fetch_add(1, Ordering::Relaxed),
                deadline: Instant::now() + Duration::from_secs(request.ttl_seconds),
                audit: audit.clone(),
            })
        };
        if let Err(error) = self.send_schedule(scheduled.clone()).await {
            let _ = self
                .append_audit(AuditRecord::request(
                    "scheduling_failure",
                    &audit,
                    old_filter.filter(),
                    old_filter.filter(),
                    "failure",
                ))
                .await;
            return Err(error);
        }

        let reload_started = Instant::now();
        let reload_result = if request.restore {
            self.handle.restore(&self.baseline)
        } else {
            let filter = request
                .filter
                .as_deref()
                .ok_or_else(|| BrokerLogFilterControlError::Reload("logFilter is required".to_string()))?;
            self.handle.reload(LogFilterReloadRequest::new(filter))
        };
        let reload_duration_millis = reload_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let resolved = match reload_result {
            Ok(resolved) => resolved,
            Err(error) => {
                if self.send_schedule(previous.clone()).await.is_err() {
                    if self.handle.restore(&self.baseline).is_err() {
                        rocketmq_observability::metrics::log_filter::record_rollback_failure("rocketmq-broker");
                    }
                    *self.active.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                    rocketmq_observability::metrics::log_filter::set_expiry_timestamp("rocketmq-broker", 0);
                }
                let _ = self
                    .append_audit(
                        AuditRecord::request(
                            "reload_failure",
                            &audit,
                            old_filter.filter(),
                            old_filter.filter(),
                            "failure",
                        )
                        .with_reload_duration(reload_duration_millis),
                    )
                    .await;
                return Err(BrokerLogFilterControlError::Reload(error.to_string()));
            }
        };
        *self.active.lock().unwrap_or_else(|error| error.into_inner()) = scheduled;

        if let Err(error) = self
            .append_audit(
                AuditRecord::request("success", &audit, old_filter.filter(), resolved.filter(), "success")
                    .with_reload_duration(reload_duration_millis),
            )
            .await
        {
            let rollback = self.handle.restore(&self.baseline);
            let _ = self.send_schedule(None).await;
            *self.active.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
            rocketmq_observability::metrics::log_filter::set_expiry_timestamp("rocketmq-broker", 0);
            if let Err(rollback_error) = rollback {
                rocketmq_observability::metrics::log_filter::record_rollback_failure("rocketmq-broker");
                tracing::error!(error = %rollback_error, "broker log filter rollback after audit failure failed");
            }
            return Err(error);
        }
        rocketmq_observability::metrics::log_filter::set_expiry_timestamp(
            "rocketmq-broker",
            if request.restore {
                0
            } else {
                current_millis() / 1_000 + request.ttl_seconds
            },
        );
        Ok(resolved)
    }

    pub(crate) async fn audit_rejection(
        &self,
        properties: &std::collections::HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString>,
        operator: &str,
        source_ip: &str,
        super_user: bool,
        result: &'static str,
    ) {
        let current = self.handle.current();
        let context = AuditContext {
            request_id: bounded_audit_value(
                properties
                    .get(LOG_FILTER_REQUEST_ID_KEY)
                    .map(cheetah_string::CheetahString::as_str),
                "<missing>",
                128,
            ),
            operator: bounded_audit_value(Some(operator), "<missing>", 256),
            source_ip: bounded_audit_value(Some(source_ip), "<unknown>", 128),
            reason: bounded_audit_value(
                properties
                    .get(LOG_FILTER_REASON_KEY)
                    .map(cheetah_string::CheetahString::as_str),
                "<missing>",
                512,
            ),
            ttl_seconds: properties
                .get(LOG_FILTER_TTL_KEY)
                .and_then(|value| value.trim().parse().ok())
                .unwrap_or(DEFAULT_LOG_FILTER_TTL_SECONDS),
            super_user,
        };
        let requested_filter = bounded_audit_value(
            properties
                .get(LOG_FILTER_KEY)
                .map(cheetah_string::CheetahString::as_str),
            current.filter(),
            4_096,
        );
        let record = AuditRecord::request_with_authorization(
            "rejected",
            &context,
            current.filter(),
            &requested_filter,
            false,
            result,
        );
        if let Err(error) = self.append_audit(record).await {
            tracing::warn!(error = %error, "failed to persist rejected broker log filter request audit");
        }
    }

    async fn send_schedule(&self, active: Option<ActiveOverride>) -> Result<(), BrokerLogFilterControlError> {
        let command = active.map_or(TtlCommand::Clear, TtlCommand::Set);
        self.ttl_sender
            .send(command)
            .await
            .map_err(|error| BrokerLogFilterControlError::Scheduling(error.to_string()))
    }

    async fn append_audit(&self, record: AuditRecord) -> Result<(), BrokerLogFilterControlError> {
        append_audit(&self.blocking, self.audit_path.clone(), record).await
    }
}

struct TtlControllerContext {
    handle: LogFilterHandle,
    baseline: ResolvedLogFilter,
    blocking: BlockingExecutor,
    audit_path: PathBuf,
    operation: Arc<Mutex<()>>,
    active: Arc<std::sync::Mutex<Option<ActiveOverride>>>,
    receiver: mpsc::Receiver<TtlCommand>,
    cancellation: tokio_util::sync::CancellationToken,
}

async fn run_ttl_controller(mut context: TtlControllerContext) {
    let mut scheduled: Option<ActiveOverride> = None;
    loop {
        match scheduled.clone() {
            Some(current) => {
                tokio::select! {
                    _ = context.cancellation.cancelled() => break,
                    command = context.receiver.recv() => match command {
                        Some(TtlCommand::Set(next)) => scheduled = Some(next),
                        Some(TtlCommand::Clear) => scheduled = None,
                        None => break,
                    },
                    _ = tokio::time::sleep_until(current.deadline) => {
                        restore_expired_override(&context, &current).await;
                        scheduled = None;
                    }
                }
            }
            None => {
                tokio::select! {
                    _ = context.cancellation.cancelled() => break,
                    command = context.receiver.recv() => match command {
                        Some(TtlCommand::Set(next)) => scheduled = Some(next),
                        Some(TtlCommand::Clear) => {},
                        None => break,
                    }
                }
            }
        }
    }
}

async fn restore_expired_override(context: &TtlControllerContext, expired: &ActiveOverride) {
    let _operation = context.operation.lock().await;
    let is_current = context
        .active
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .as_ref()
        .is_some_and(|active| active.generation == expired.generation);
    if !is_current {
        return;
    }
    let old_filter = context.handle.current();
    let result = context.handle.restore(&context.baseline);
    let (result_text, new_filter) = match &result {
        Ok(resolved) => ("success", resolved.filter()),
        Err(_) => ("failure", old_filter.filter()),
    };
    let record = AuditRecord::request(
        "ttl_restore",
        &expired.audit,
        old_filter.filter(),
        new_filter,
        result_text,
    );
    if let Err(error) = append_audit(&context.blocking, context.audit_path.clone(), record).await {
        tracing::error!(error = %error, "broker log filter TTL restore audit failed");
    }
    if let Err(error) = result {
        rocketmq_observability::metrics::log_filter::record_auto_restore_failure("rocketmq-broker");
        tracing::error!(error = %error, "broker log filter TTL restore failed");
    } else {
        rocketmq_observability::metrics::log_filter::set_expiry_timestamp("rocketmq-broker", 0);
        *context.active.lock().unwrap_or_else(|error| error.into_inner()) = None;
    }
}

#[derive(Serialize)]
struct AuditRecord {
    timestamp_millis: u64,
    phase: &'static str,
    request_id: String,
    operator: String,
    source_ip: String,
    old_filter: String,
    new_filter: String,
    reason: String,
    ttl_seconds: u64,
    super_user: bool,
    authorized: bool,
    result: &'static str,
    reload_duration_millis: Option<u64>,
}

impl AuditRecord {
    fn request(
        phase: &'static str,
        context: &AuditContext,
        old_filter: &str,
        new_filter: &str,
        result: &'static str,
    ) -> Self {
        Self::request_with_authorization(phase, context, old_filter, new_filter, true, result)
    }

    fn request_with_authorization(
        phase: &'static str,
        context: &AuditContext,
        old_filter: &str,
        new_filter: &str,
        authorized: bool,
        result: &'static str,
    ) -> Self {
        Self {
            timestamp_millis: current_millis(),
            phase,
            request_id: context.request_id.clone(),
            operator: context.operator.clone(),
            source_ip: context.source_ip.clone(),
            old_filter: old_filter.to_owned(),
            new_filter: new_filter.to_owned(),
            reason: context.reason.clone(),
            ttl_seconds: context.ttl_seconds,
            super_user: context.super_user,
            authorized,
            result,
            reload_duration_millis: None,
        }
    }

    fn with_reload_duration(mut self, duration_millis: u64) -> Self {
        self.reload_duration_millis = Some(duration_millis);
        self
    }
}

fn bounded_audit_value(value: Option<&str>, default: &str, max_chars: usize) -> String {
    let value = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default);
    value.chars().take(max_chars).collect()
}

async fn append_audit(
    blocking: &BlockingExecutor,
    path: PathBuf,
    record: AuditRecord,
) -> Result<(), BrokerLogFilterControlError> {
    let operation = move || write_audit_record(path.as_path(), &record);
    let result = match blocking.spawn_io("broker.log-filter-audit", operation).await {
        Ok(result) => result.map_err(|error| BrokerLogFilterControlError::Audit(error.to_string())),
        Err(error) => Err(BrokerLogFilterControlError::Audit(error.to_string())),
    };
    if result.is_err() {
        rocketmq_observability::metrics::log_filter::record_audit_failure("rocketmq-broker");
    }
    result
}

fn write_audit_record(path: &Path, record: &AuditRecord) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, record).map_err(std::io::Error::other)?;
    file.write_all(b"\n")?;
    file.sync_data()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::process::Command;

    use super::*;

    fn base_properties(filter: &str) -> HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString> {
        HashMap::from([
            (LOG_FILTER_KEY.into(), filter.into()),
            (LOG_FILTER_REASON_KEY.into(), "incident investigation".into()),
            (LOG_FILTER_REQUEST_ID_KEY.into(), "INC-42".into()),
        ])
    }

    #[test]
    fn normal_operator_requires_info_baseline_and_rocketmq_target() {
        let request = BrokerLogFilterRequest::parse(
            &base_properties("info,rocketmq_broker=debug"),
            "operator",
            "127.0.0.1",
            false,
        )
        .expect("scoped filter should be accepted");
        assert_eq!(request.ttl_seconds, DEFAULT_LOG_FILTER_TTL_SECONDS);

        assert!(BrokerLogFilterRequest::parse(&base_properties("debug"), "operator", "127.0.0.1", false,).is_err());
        assert!(
            BrokerLogFilterRequest::parse(&base_properties("info,hyper=debug"), "operator", "127.0.0.1", false,)
                .is_err()
        );
    }

    #[test]
    fn super_user_can_use_break_glass_global_filter() {
        let request = BrokerLogFilterRequest::parse(&base_properties("debug"), "root", "127.0.0.1", true)
            .expect("super-user global filter should be accepted");

        assert_eq!(request.filter.as_deref(), Some("debug"));
        assert!(request.super_user);
    }

    #[test]
    fn request_rejects_invalid_ttl_and_mixed_config() {
        let mut invalid_ttl = base_properties("info,rocketmq_broker=debug");
        invalid_ttl.insert(LOG_FILTER_TTL_KEY.into(), "59".into());
        assert!(BrokerLogFilterRequest::parse(&invalid_ttl, "operator", "127.0.0.1", false).is_err());

        let mut mixed = base_properties("info,rocketmq_broker=debug");
        mixed.insert("brokerName".into(), "mixed".into());
        assert!(BrokerLogFilterRequest::parse(&mixed, "operator", "127.0.0.1", false).is_err());
    }

    #[test]
    fn restore_rejects_filter_and_requires_audit_fields() {
        let mut restore = base_properties("info");
        restore.insert(LOG_FILTER_RESTORE_KEY.into(), "true".into());
        assert!(BrokerLogFilterRequest::parse(&restore, "operator", "127.0.0.1", false).is_err());

        restore.remove(LOG_FILTER_KEY);
        let request = BrokerLogFilterRequest::parse(&restore, "operator", "127.0.0.1", false)
            .expect("restore should be accepted without a filter");
        assert!(request.restore);
    }

    #[test]
    fn audit_records_are_appended_as_json_lines() {
        let directory = tempfile::tempdir().expect("temporary audit directory should be created");
        let path = directory.path().join("logs").join("log-filter-audit.jsonl");
        let context = AuditContext {
            request_id: "INC-42".to_string(),
            operator: "operator".to_string(),
            source_ip: "127.0.0.1".to_string(),
            reason: "incident investigation".to_string(),
            ttl_seconds: 120,
            super_user: false,
        };

        write_audit_record(
            &path,
            &AuditRecord::request("intent", &context, "info", "info,rocketmq_broker=debug", "pending"),
        )
        .expect("intent audit should be written");
        write_audit_record(
            &path,
            &AuditRecord::request("success", &context, "info", "info,rocketmq_broker=debug", "success"),
        )
        .expect("success audit should be appended");

        let contents = std::fs::read_to_string(path).expect("audit file should be readable");
        let records = contents
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("audit line should be valid JSON"))
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["phase"], "intent");
        assert_eq!(records[1]["phase"], "success");
        assert_eq!(records[1]["request_id"], "INC-42");
        assert_eq!(records[1]["authorized"], true);
    }

    #[test]
    fn control_applies_replaces_and_restores_filters_without_reinstalling_subscriber() {
        let output = Command::new(std::env::current_exe().expect("broker test executable should be available"))
            .arg("--exact")
            .arg("broker::log_filter_control::tests::broker_log_filter_control_child")
            .arg("--ignored")
            .env("ROCKETMQ_BROKER_LOG_FILTER_CHILD", "1")
            .output()
            .expect("broker log-filter child should start");

        assert!(
            output.status.success(),
            "broker log-filter child failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test]
    #[ignore]
    async fn broker_log_filter_control_child() {
        assert_eq!(std::env::var("ROCKETMQ_BROKER_LOG_FILTER_CHILD").as_deref(), Ok("1"));
        let mut telemetry_config = rocketmq_observability::TelemetryBootstrapConfig::default();
        telemetry_config.observability.service_name = "rocketmq-broker-log-filter-test".to_string();
        telemetry_config.observability.subscriber_install_policy =
            rocketmq_observability::SubscriberInstallPolicy::Required;
        telemetry_config.logging.reload.enabled = true;
        let telemetry_guard = rocketmq_observability::install_global(&telemetry_config)
            .expect("reloadable test subscriber should install");
        let handle = telemetry_guard
            .log_filter_handle()
            .expect("reload-enabled telemetry should expose a handle");
        let baseline = handle.current();
        assert_eq!(baseline.filter(), "info");

        let runtime = rocketmq_runtime::RuntimeContext::try_from_current("broker-log-filter-test")
            .expect("Tokio runtime should be available");
        let directory = tempfile::tempdir().expect("temporary broker store should be created");
        let service = runtime.service_context("broker-log-filter-control");
        let control =
            BrokerLogFilterControl::start(handle.clone(), &service, directory.path().to_string_lossy().as_ref())
                .expect("log-filter control should start");

        let ttl_request = test_request("ttl", Some("info,rocketmq_broker=debug"), 1, false);
        let applied = control.apply(ttl_request).await.expect("target DEBUG should apply");
        assert_eq!(applied.filter(), "info,rocketmq_broker=debug");
        tokio::time::timeout(Duration::from_secs(5), async {
            while handle.current() != baseline {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("TTL should restore the startup baseline");

        let first = control.apply(test_request(
            "replace-debug",
            Some("info,rocketmq_broker=debug"),
            60,
            false,
        ));
        let second = control.apply(test_request(
            "replace-trace",
            Some("info,rocketmq_broker=trace"),
            60,
            false,
        ));
        let (first, second) = tokio::join!(first, second);
        first.expect("first concurrent override should apply");
        second.expect("second concurrent override should apply");
        assert_eq!(handle.current().filter(), "info,rocketmq_broker=trace");

        control
            .apply(test_request("explicit-restore", None, 60, true))
            .await
            .expect("explicit restore should return to baseline");
        assert_eq!(handle.current(), baseline);
        control
            .audit_rejection(
                &base_properties("debug"),
                "test-operator",
                "127.0.0.1",
                false,
                "validation_failure",
            )
            .await;

        let audit_path = directory.path().join("logs").join("log-filter-audit.jsonl");
        let audit_contents = std::fs::read_to_string(audit_path).expect("audit evidence should be readable");
        assert!(audit_contents.contains(r#""phase":"ttl_restore""#));
        assert!(audit_contents.contains(r#""request_id":"explicit-restore""#));
        assert!(audit_contents.contains(r#""phase":"rejected""#));
        assert!(audit_contents.contains(r#""authorized":false"#));

        let bad_directory = tempfile::tempdir().expect("temporary invalid audit root should be created");
        std::fs::write(bad_directory.path().join("logs"), b"not-a-directory")
            .expect("invalid audit parent marker should be created");
        let bad_audit_service = runtime.service_context("broker-log-filter-bad-audit");
        let bad_audit_control = BrokerLogFilterControl::start(
            handle.clone(),
            &bad_audit_service,
            bad_directory.path().to_string_lossy().as_ref(),
        )
        .expect("control should start before its first audit write");
        let error = bad_audit_control
            .apply(test_request(
                "audit-failure",
                Some("info,rocketmq_broker=debug"),
                60,
                false,
            ))
            .await
            .expect_err("unwritable audit path must reject the override");
        assert!(matches!(error, BrokerLogFilterControlError::Audit(_)));
        assert_eq!(handle.current(), baseline);

        let stopped_service = runtime.service_context("broker-log-filter-stopped-ttl");
        let stopped_control = BrokerLogFilterControl::start(
            handle.clone(),
            &stopped_service,
            directory.path().to_string_lossy().as_ref(),
        )
        .expect("control should start before TTL task shutdown");
        let stopped_report = stopped_service.task_group().shutdown(Duration::from_secs(2)).await;
        assert!(stopped_report.is_healthy(), "TTL task shutdown should be healthy");
        let error = stopped_control
            .apply(test_request(
                "schedule-failure",
                Some("info,rocketmq_broker=debug"),
                60,
                false,
            ))
            .await
            .expect_err("stopped TTL task must reject the override");
        assert!(matches!(error, BrokerLogFilterControlError::Scheduling(_)));
        assert_eq!(handle.current(), baseline);

        let shutdown = runtime.shutdown_tasks(Duration::from_secs(2)).await;
        assert!(shutdown.is_healthy(), "owned runtime tasks should shut down cleanly");
        telemetry_guard
            .shutdown()
            .into_result()
            .expect("telemetry shutdown should be healthy");
    }

    fn test_request(request_id: &str, filter: Option<&str>, ttl_seconds: u64, restore: bool) -> BrokerLogFilterRequest {
        BrokerLogFilterRequest {
            filter: filter.map(ToOwned::to_owned),
            reason: "broker log-filter control test".to_string(),
            ttl_seconds,
            request_id: request_id.to_string(),
            operator: "test-operator".to_string(),
            source_ip: "127.0.0.1".to_string(),
            restore,
            super_user: false,
        }
    }
}
