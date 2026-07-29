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

//! Read-only operator CLI support for RocketMQ Rust AI SRE.
//!
//! Remote commands are fixed GET operations. Draft commands only validate
//! local JSON and emit a local-only typed artifact to stdout; they cannot
//! submit, approve, or execute a change.

use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_client::Client;
use rocketmq_sre_client::ClientError;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::InspectionRunId;
use rocketmq_sre_contracts::RunbookId;
use rocketmq_sre_contracts::is_sha256_digest;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

const DEFAULT_TOKEN_ENV: &str = "ROCKETMQ_SRE_TOKEN";
const BASE_URL_ENV: &str = "ROCKETMQ_SRE_URL";
const MAX_DRAFT_BYTES: u64 = 256 * 1024;

pub const USAGE: &str = "\
rocketmq-sre [OPTIONS] <COMMAND>

Read-only commands:
  status                         Read process liveness
  readiness                      Read dependency readiness
  openapi                        Read the canonical Phase 5 OpenAPI document
  clusters                       List authorized clusters
  cluster <UUID>                 Read one cluster
  incident <UUID>                Read one incident and bounded context
  inspection <UUID>              Read one inspection and recommendations
  plan <UUID>                    Read one typed plan and status

Local-only draft commands:
  draft-plan <JSON_FILE>         Validate and print a typed local plan draft
  draft-runbook <JSON_FILE>      Validate and print a typed local runbook draft

Options (must precede COMMAND):
  --url <URL>                    Control Plane URL (or ROCKETMQ_SRE_URL)
  --token-env <NAME>             Bearer-token environment variable
  --allow-cluster <UUID>         Repeatable client-side cluster allowlist
  --compact                      Emit compact JSON
  --help                         Print this help

Security boundary:
  Remote commands use GET only. --token is deliberately unsupported; use an
  environment variable. No shell, raw Admin, approve, execute, apply, reset,
  truncate, or arbitrary request command exists.
";

/// Parsed command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    Status,
    Readiness,
    OpenApi,
    Clusters,
    Cluster(ClusterId),
    Incident(IncidentId),
    Inspection(InspectionRunId),
    Plan(ActionPlanId),
    DraftPlan(PathBuf),
    DraftRunbook(PathBuf),
    Help,
}

impl Command {
    const fn requires_remote_auth(&self) -> bool {
        !matches!(
            self,
            Self::Status | Self::Readiness | Self::DraftPlan(_) | Self::DraftRunbook(_) | Self::Help
        )
    }

    const fn is_local(&self) -> bool {
        matches!(self, Self::DraftPlan(_) | Self::DraftRunbook(_) | Self::Help)
    }
}

/// Global CLI configuration.
///
/// This type deliberately does not implement `Debug`: it stores the name of a
/// secret-bearing environment variable and is passed into token resolution.
pub struct CliConfig {
    base_url: Option<String>,
    token_env: String,
    allowed_clusters: Vec<ClusterId>,
    compact: bool,
}

/// Parsed invocation.
pub struct Invocation {
    pub config: CliConfig,
    pub command: Command,
}

/// Stable CLI errors. Secret values and raw server bodies are never included.
#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Usage(String),
    #[error("Control Plane URL is required via --url or ROCKETMQ_SRE_URL")]
    MissingBaseUrl,
    #[error("bearer token is required in environment variable {name}")]
    MissingToken { name: String },
    #[error("environment variable {name} is not valid Unicode")]
    InvalidEnvironment { name: String },
    #[error("draft file exceeds the {MAX_DRAFT_BYTES} byte limit")]
    DraftTooLarge,
    #[error("draft file could not be read")]
    DraftIo(#[source] std::io::Error),
    #[error("draft does not match the typed local contract: {0}")]
    DraftContract(String),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("JSON output failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// One typed local plan step.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalPlanStep {
    pub action_id: String,
    pub descriptor_version: String,
    pub resource: String,
    pub parameters: Value,
    pub evidence_ids: Vec<EvidenceId>,
}

/// Input accepted by `draft-plan`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalPlanDraftInput {
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub diagnosis_revision_id: DiagnosisRevisionId,
    pub expires_at: Option<DateTime<Utc>>,
    pub steps: Vec<LocalPlanStep>,
}

/// Local-only plan draft output.
#[derive(Clone, Debug, Serialize)]
pub struct LocalPlanDraft {
    pub schema_version: &'static str,
    pub mode: &'static str,
    pub id: ActionPlanId,
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub diagnosis_revision_id: DiagnosisRevisionId,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub steps: Vec<LocalPlanStep>,
}

/// A runbook step that cannot encode raw shell or an untyped mutation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LocalRunbookStep {
    Read { description: String, query: String },
    ManualGate { title: String, instructions: String },
    PlanReference { plan_id: ActionPlanId, plan_hash: String },
}

/// Input accepted by `draft-runbook`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalRunbookDraftInput {
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub name: String,
    pub version: String,
    pub steps: Vec<LocalRunbookStep>,
}

/// Local-only runbook draft output.
#[derive(Clone, Debug, Serialize)]
pub struct LocalRunbookDraft {
    pub schema_version: &'static str,
    pub mode: &'static str,
    pub id: RunbookId,
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub name: String,
    pub version: String,
    pub created_at: DateTime<Utc>,
    pub steps: Vec<LocalRunbookStep>,
}

/// Parses command-line arguments excluding the program name.
///
/// # Errors
///
/// Returns [`CliError::Usage`] for an unknown option or command, missing
/// value, invalid UUID, inline token attempt, or trailing argument.
pub fn parse_args(arguments: impl IntoIterator<Item = OsString>) -> Result<Invocation, CliError> {
    let mut arguments = arguments.into_iter();
    let mut base_url = None;
    let mut token_env = DEFAULT_TOKEN_ENV.to_owned();
    let mut allowed_clusters = Vec::new();
    let mut compact = false;
    let mut command_token = None;

    while let Some(argument) = arguments.next() {
        let argument = argument
            .into_string()
            .map_err(|_| CliError::Usage("arguments must be valid Unicode".to_owned()))?;
        if command_token.is_some() {
            return Err(CliError::Usage(
                "unexpected trailing argument; run with --help".to_owned(),
            ));
        }
        match argument.as_str() {
            "--url" => {
                base_url = Some(next_unicode(&mut arguments, "--url")?);
            }
            "--token-env" => {
                let name = next_unicode(&mut arguments, "--token-env")?;
                validate_environment_name(&name)?;
                token_env = name;
            }
            "--allow-cluster" => {
                let value = next_unicode(&mut arguments, "--allow-cluster")?;
                allowed_clusters.push(parse_id("cluster", &value)?);
            }
            "--compact" => compact = true,
            "--help" | "-h" => command_token = Some("--help".to_owned()),
            "--token" => {
                return Err(CliError::Usage(
                    "--token is forbidden; use --token-env to avoid process-list exposure".to_owned(),
                ));
            }
            value if value.starts_with('-') => {
                return Err(CliError::Usage(format!("unknown option {value}; run with --help")));
            }
            _ => command_token = Some(argument),
        }
    }

    let command_token = command_token.ok_or_else(|| CliError::Usage("a command is required".to_owned()))?;
    let command = match command_token.as_str() {
        "--help" => Command::Help,
        "status" => Command::Status,
        "readiness" => Command::Readiness,
        "openapi" => Command::OpenApi,
        "clusters" => Command::Clusters,
        value => {
            return Err(CliError::Usage(format!(
                "command {value} requires arguments or is unknown; run with --help"
            )));
        }
    };

    Ok(Invocation {
        config: CliConfig {
            base_url,
            token_env,
            allowed_clusters,
            compact,
        },
        command,
    })
}

/// Parses command-line arguments while allowing exactly one command operand.
///
/// This wrapper exists so the parser never treats arbitrary trailing tokens as
/// generic HTTP paths.
pub fn parse_process_args(arguments: impl IntoIterator<Item = OsString>) -> Result<Invocation, CliError> {
    let mut input = arguments.into_iter().peekable();
    let mut globals = Vec::new();
    let mut command = None;
    let mut operand = None;

    while let Some(argument) = input.next() {
        let value = argument
            .into_string()
            .map_err(|_| CliError::Usage("arguments must be valid Unicode".to_owned()))?;
        if value == "--token" {
            return Err(CliError::Usage(
                "--token is forbidden; use --token-env to avoid process-list exposure".to_owned(),
            ));
        }
        if command.is_none() && matches!(value.as_str(), "--help" | "-h") {
            command = Some("--help".to_owned());
            continue;
        }
        if command.is_none() && value.starts_with('-') {
            globals.push(OsString::from(&value));
            if matches!(value.as_str(), "--url" | "--token-env" | "--allow-cluster") {
                let option_value = input
                    .next()
                    .ok_or_else(|| CliError::Usage(format!("{value} requires a value")))?;
                globals.push(option_value);
            }
            continue;
        }
        if command.is_none() {
            command = Some(value);
        } else if operand.is_none() {
            operand = Some(value);
        } else {
            return Err(CliError::Usage("only one command operand is accepted".to_owned()));
        }
    }

    let command = command.unwrap_or_else(|| "--help".to_owned());
    if matches!(
        command.as_str(),
        "status" | "readiness" | "openapi" | "clusters" | "--help"
    ) {
        if operand.is_some() {
            return Err(CliError::Usage(format!("{command} does not accept an operand")));
        }
        globals.push(OsString::from(command));
        return parse_args(globals);
    }

    let operand = operand.ok_or_else(|| CliError::Usage(format!("{command} requires one operand")))?;
    globals.push(OsString::from("--help"));
    let parsed = parse_args(globals)?;
    let command = match command.as_str() {
        "cluster" => Command::Cluster(parse_id("cluster", &operand)?),
        "incident" => Command::Incident(parse_id("incident", &operand)?),
        "inspection" => Command::Inspection(parse_id("inspection", &operand)?),
        "plan" => Command::Plan(parse_id("plan", &operand)?),
        "draft-plan" => Command::DraftPlan(PathBuf::from(operand)),
        "draft-runbook" => Command::DraftRunbook(PathBuf::from(operand)),
        _ => {
            return Err(CliError::Usage(format!("unknown command {command}; run with --help")));
        }
    };
    Ok(Invocation {
        config: parsed.config,
        command,
    })
}

/// Executes one fixed command and returns its JSON value.
///
/// # Errors
///
/// Returns a configuration, local draft, client, or serialization error.
pub async fn execute(invocation: &Invocation) -> Result<Value, CliError> {
    if invocation.command.is_local() {
        return execute_local(&invocation.command);
    }

    let base_url = resolve_base_url(&invocation.config)?;
    let token = resolve_token(&invocation.config, &invocation.command)?;
    let mut builder = Client::builder(base_url)?;
    if let Some(token) = token {
        builder = builder.bearer_token(token);
    }
    if !invocation.config.allowed_clusters.is_empty() {
        builder = builder.allowed_clusters(invocation.config.allowed_clusters.iter().copied());
    }
    let client = builder.build()?;

    match invocation.command {
        Command::Status => to_value(client.status().await?),
        Command::Readiness => Ok(client.readiness().await?),
        Command::OpenApi => Ok(client.openapi().await?),
        Command::Clusters => to_value(client.clusters().await?),
        Command::Cluster(id) => to_value(client.cluster(id).await?),
        Command::Incident(id) => to_value(client.incident(id).await?),
        Command::Inspection(id) => to_value(client.inspection(id).await?),
        Command::Plan(id) => to_value(client.plan(id).await?),
        Command::DraftPlan(_) | Command::DraftRunbook(_) | Command::Help => {
            Err(CliError::Usage("local command routing failed".to_owned()))
        }
    }
}

/// Renders command output without logging credentials or configuration.
///
/// # Errors
///
/// Returns a JSON serialization error.
pub fn render(invocation: &Invocation, value: &Value) -> Result<String, CliError> {
    if invocation.config.compact {
        serde_json::to_string(value).map_err(CliError::Json)
    } else {
        serde_json::to_string_pretty(value).map_err(CliError::Json)
    }
}

fn execute_local(command: &Command) -> Result<Value, CliError> {
    match command {
        Command::DraftPlan(path) => {
            let input: LocalPlanDraftInput = read_draft(path)?;
            validate_plan_draft(&input)?;
            to_value(LocalPlanDraft {
                schema_version: "rocketmq-sre.local-plan-draft.v1",
                mode: "local_only",
                id: ActionPlanId::new(),
                cluster_id: input.cluster_id,
                incident_id: input.incident_id,
                diagnosis_revision_id: input.diagnosis_revision_id,
                created_at: Utc::now(),
                expires_at: input.expires_at,
                steps: input.steps,
            })
        }
        Command::DraftRunbook(path) => {
            let input: LocalRunbookDraftInput = read_draft(path)?;
            validate_runbook_draft(&input)?;
            to_value(LocalRunbookDraft {
                schema_version: "rocketmq-sre.local-runbook-draft.v1",
                mode: "local_only",
                id: RunbookId::new(),
                cluster_id: input.cluster_id,
                incident_id: input.incident_id,
                name: input.name,
                version: input.version,
                created_at: Utc::now(),
                steps: input.steps,
            })
        }
        Command::Help => Ok(Value::String(USAGE.to_owned())),
        _ => Err(CliError::Usage("command is not local".to_owned())),
    }
}

fn resolve_base_url(config: &CliConfig) -> Result<String, CliError> {
    if let Some(value) = &config.base_url {
        return Ok(value.clone());
    }
    match env::var(BASE_URL_ENV) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        Ok(_) | Err(env::VarError::NotPresent) => Err(CliError::MissingBaseUrl),
        Err(env::VarError::NotUnicode(_)) => Err(CliError::InvalidEnvironment {
            name: BASE_URL_ENV.to_owned(),
        }),
    }
}

fn resolve_token(config: &CliConfig, command: &Command) -> Result<Option<String>, CliError> {
    match env::var(&config.token_env) {
        Ok(value) if !value.trim().is_empty() && !value.contains(['\r', '\n']) => Ok(Some(value)),
        Ok(_) | Err(env::VarError::NotPresent) if !command.requires_remote_auth() => Ok(None),
        Ok(_) | Err(env::VarError::NotPresent) => Err(CliError::MissingToken {
            name: config.token_env.clone(),
        }),
        Err(env::VarError::NotUnicode(_)) => Err(CliError::InvalidEnvironment {
            name: config.token_env.clone(),
        }),
    }
}

fn read_draft<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, CliError> {
    let metadata = fs::metadata(path).map_err(CliError::DraftIo)?;
    if metadata.len() > MAX_DRAFT_BYTES {
        return Err(CliError::DraftTooLarge);
    }
    let bytes = fs::read(path).map_err(CliError::DraftIo)?;
    serde_json::from_slice(&bytes).map_err(|error| CliError::DraftContract(error.to_string()))
}

fn validate_plan_draft(input: &LocalPlanDraftInput) -> Result<(), CliError> {
    if !(1..=64).contains(&input.steps.len()) {
        return Err(CliError::DraftContract(
            "steps must contain 1-64 typed actions".to_owned(),
        ));
    }
    for (index, step) in input.steps.iter().enumerate() {
        validate_text(&format!("steps[{index}].action_id"), &step.action_id, 255)?;
        validate_text(
            &format!("steps[{index}].descriptor_version"),
            &step.descriptor_version,
            64,
        )?;
        validate_text(&format!("steps[{index}].resource"), &step.resource, 512)?;
        if !step.parameters.is_object() {
            return Err(CliError::DraftContract(format!(
                "steps[{index}].parameters must be an object"
            )));
        }
        if !(1..=32).contains(&step.evidence_ids.len()) {
            return Err(CliError::DraftContract(format!(
                "steps[{index}].evidence_ids must contain 1-32 identifiers"
            )));
        }
    }
    Ok(())
}

fn validate_runbook_draft(input: &LocalRunbookDraftInput) -> Result<(), CliError> {
    validate_text("name", &input.name, 256)?;
    validate_text("version", &input.version, 64)?;
    if !(1..=64).contains(&input.steps.len()) {
        return Err(CliError::DraftContract(
            "steps must contain 1-64 typed entries".to_owned(),
        ));
    }
    for (index, step) in input.steps.iter().enumerate() {
        match step {
            LocalRunbookStep::Read { description, query } => {
                validate_text(&format!("steps[{index}].description"), description, 512)?;
                validate_text(&format!("steps[{index}].query"), query, 1024)?;
            }
            LocalRunbookStep::ManualGate { title, instructions } => {
                validate_text(&format!("steps[{index}].title"), title, 256)?;
                validate_text(&format!("steps[{index}].instructions"), instructions, 4096)?;
            }
            LocalRunbookStep::PlanReference { plan_hash, .. } => {
                if !is_sha256_digest(plan_hash) {
                    return Err(CliError::DraftContract(format!(
                        "steps[{index}].plan_hash must be a sha256 digest"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_text(name: &str, value: &str, maximum: usize) -> Result<(), CliError> {
    let length = value.trim().chars().count();
    if length == 0 || length > maximum {
        return Err(CliError::DraftContract(format!(
            "{name} must contain 1-{maximum} characters"
        )));
    }
    Ok(())
}

fn validate_environment_name(value: &str) -> Result<(), CliError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(CliError::Usage(
            "--token-env must use 1-128 uppercase ASCII letters, digits, or underscores".to_owned(),
        ));
    }
    Ok(())
}

fn next_unicode(arguments: &mut impl Iterator<Item = OsString>, option: &str) -> Result<String, CliError> {
    arguments
        .next()
        .ok_or_else(|| CliError::Usage(format!("{option} requires a value")))?
        .into_string()
        .map_err(|_| CliError::Usage(format!("{option} value must be valid Unicode")))
}

fn parse_id<T>(name: &str, value: &str) -> Result<T, CliError>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| CliError::Usage(format!("{name} identifier must be a UUID")))
}

fn to_value(value: impl Serialize) -> Result<Value, CliError> {
    serde_json::to_value(value).map_err(CliError::Json)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn os(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn parser_exposes_only_fixed_read_and_local_draft_commands() {
        let cluster_id = ClusterId::new();
        let invocation = parse_process_args(os(&[
            "--url",
            "https://sre.example.test",
            "--allow-cluster",
            &cluster_id.to_string(),
            "cluster",
            &cluster_id.to_string(),
        ]))
        .expect("valid cluster command");
        assert!(matches!(invocation.command, Command::Cluster(id) if id == cluster_id));

        for forbidden in ["shell", "raw-admin", "execute", "approve", "apply", "reset", "truncate"] {
            assert!(parse_process_args(os(&[forbidden])).is_err());
        }
    }

    #[test]
    fn parser_forbids_inline_tokens_without_consuming_the_secret() {
        let error = parse_process_args(os(&["--token", "super-secret", "clusters"]))
            .err()
            .expect("inline token must fail");
        let rendered = error.to_string();
        assert!(rendered.contains("--token is forbidden"));
        assert!(!rendered.contains("super-secret"));
    }

    #[test]
    fn help_flag_is_routed_once_without_a_command_operand() {
        let invocation = parse_process_args(os(&["--help"])).expect("help");
        assert_eq!(invocation.command, Command::Help);
    }

    #[test]
    fn runbook_contract_has_no_shell_or_raw_action_variant() {
        let value = serde_json::json!({
            "cluster_id": ClusterId::new(),
            "incident_id": IncidentId::new(),
            "name": "unsafe",
            "version": "1.0.0",
            "steps": [{
                "kind": "shell",
                "command": "rm -rf /"
            }]
        });
        assert!(serde_json::from_value::<LocalRunbookDraftInput>(value).is_err());
    }

    #[test]
    fn plan_draft_requires_typed_steps_and_evidence() {
        let input = LocalPlanDraftInput {
            cluster_id: ClusterId::new(),
            incident_id: IncidentId::new(),
            diagnosis_revision_id: DiagnosisRevisionId::new(),
            expires_at: None,
            steps: vec![LocalPlanStep {
                action_id: "rocketmq.broker.config.plan".to_owned(),
                descriptor_version: "1.0.0".to_owned(),
                resource: "broker-a".to_owned(),
                parameters: serde_json::json!({"maxMessageSize": 4_194_304}),
                evidence_ids: vec![EvidenceId::new()],
            }],
        };
        assert!(validate_plan_draft(&input).is_ok());
    }
}
