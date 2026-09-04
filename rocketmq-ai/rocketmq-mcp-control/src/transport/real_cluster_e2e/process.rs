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

use std::cmp::Reverse;
use std::fs::File;
use std::io::Read;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use super::E2eContext;
use super::E2eError;
use super::E2eResult;
use super::BROKER_BIN_ENV;
use super::MESSAGE_BODY;
use super::NAMESRV_BIN_ENV;

const LOOPBACK: Ipv4Addr = Ipv4Addr::LOCALHOST;
const READINESS_TIMEOUT: Duration = Duration::from_secs(45);
const STOP_TIMEOUT: Duration = Duration::from_secs(8);
const DROP_REAP_TIMEOUT: Duration = Duration::from_secs(2);
const DROP_REAP_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DIAGNOSTIC_BYTES: u64 = 8 * 1024;
const BROKER_THREAD_STACK_BYTES: usize = 16 * 1024 * 1024;

pub(super) struct PortSet {
    pub namesrv: ReservedPort,
    pub namesrv_health: ReservedPort,
    pub broker: ReservedPort,
    pub broker_fast: ReservedPort,
    pub broker_ha: ReservedPort,
    pub broker_health: ReservedPort,
    pub control_https: ReservedPort,
}

impl PortSet {
    pub fn allocate() -> E2eResult<Self> {
        let (broker, broker_fast) = reserve_broker_pair()?;
        Ok(Self {
            namesrv: ReservedPort::allocate()?,
            namesrv_health: ReservedPort::allocate()?,
            broker,
            broker_fast,
            broker_ha: ReservedPort::allocate()?,
            broker_health: ReservedPort::allocate()?,
            control_https: ReservedPort::allocate()?,
        })
    }
}

pub(super) struct ReservedPort {
    listener: Option<TcpListener>,
    port: u16,
}

impl ReservedPort {
    fn allocate() -> E2eResult<Self> {
        let listener = TcpListener::bind(SocketAddr::from((LOOPBACK, 0))).e2e("reserve loopback port")?;
        let port = listener.local_addr().e2e("inspect reserved loopback port")?.port();
        Ok(Self {
            listener: Some(listener),
            port,
        })
    }

    pub const fn value(&self) -> u16 {
        self.port
    }

    pub fn release(&mut self) {
        self.listener.take();
    }
}

fn reserve_broker_pair() -> E2eResult<(ReservedPort, ReservedPort)> {
    for _ in 0..128 {
        let main = ReservedPort::allocate()?;
        let port = main.value();
        if port <= 1026 {
            continue;
        }
        let fast_port = port - 2;
        if let Ok(listener) = TcpListener::bind(SocketAddr::from((LOOPBACK, fast_port))) {
            return Ok((
                main,
                ReservedPort {
                    listener: Some(listener),
                    port: fast_port,
                },
            ));
        }
    }
    Err(E2eError::new(
        "could not reserve the Broker remoting and fast-remoting port pair",
    ))
}

pub(super) struct ClusterProcesses {
    root: PathBuf,
    namesrv_bin: PathBuf,
    broker_bin: PathBuf,
    namesrv_config: PathBuf,
    broker_config: PathBuf,
    namesrv_health: u16,
    broker_health: u16,
    redactions: Vec<String>,
    namesrv: Option<OwnedChild>,
    broker: Option<OwnedChild>,
}

impl ClusterProcesses {
    pub fn new(root: &Path, ports: &PortSet, namesrv_config: PathBuf, broker_config: PathBuf) -> E2eResult<Self> {
        let namesrv_bin = required_binary(NAMESRV_BIN_ENV)?;
        let broker_bin = required_binary(BROKER_BIN_ENV)?;
        let mut redactions = vec![
            "127.0.0.1".to_owned(),
            MESSAGE_BODY.to_owned(),
            ports.namesrv.value().to_string(),
            ports.namesrv_health.value().to_string(),
            ports.broker.value().to_string(),
            ports.broker_fast.value().to_string(),
            ports.broker_ha.value().to_string(),
            ports.broker_health.value().to_string(),
            ports.control_https.value().to_string(),
        ];
        for path in [
            root,
            namesrv_bin.as_path(),
            broker_bin.as_path(),
            namesrv_config.as_path(),
            broker_config.as_path(),
        ] {
            push_path_redactions(&mut redactions, path);
        }
        if let Ok(current_dir) = std::env::current_dir() {
            for path in current_dir.ancestors().take(3) {
                push_path_redactions(&mut redactions, path);
            }
        }
        redactions.sort_by_key(|value| Reverse(value.len()));
        redactions.dedup();
        Ok(Self {
            root: root.to_path_buf(),
            namesrv_bin,
            broker_bin,
            namesrv_config,
            broker_config,
            namesrv_health: ports.namesrv_health.value(),
            broker_health: ports.broker_health.value(),
            redactions,
            namesrv: None,
            broker: None,
        })
    }

    pub async fn start_namesrv(&mut self, ports: &mut PortSet) -> E2eResult<()> {
        ports.namesrv.release();
        ports.namesrv_health.release();
        let config = self.namesrv_config.as_os_str();
        let child = OwnedChild::spawn(
            "NameServer",
            &self.namesrv_bin,
            ["--configFile".as_ref(), config],
            self.namesrv_health,
            &self.root,
            &self.redactions,
        )?;
        self.namesrv = Some(child);
        self.wait_ready("NameServer", self.namesrv_health).await
    }

    pub async fn start_broker(&mut self, ports: &mut PortSet) -> E2eResult<()> {
        ports.broker.release();
        ports.broker_fast.release();
        ports.broker_ha.release();
        ports.broker_health.release();
        self.spawn_broker()?;
        self.wait_ready("Broker", self.broker_health).await
    }

    pub async fn restart_broker(&mut self) -> E2eResult<()> {
        self.stop_broker().await?;
        self.spawn_broker()?;
        self.wait_ready("Broker", self.broker_health).await
    }

    pub async fn ensure_broker_running(&mut self) -> E2eResult<()> {
        let running = match self.broker.as_mut() {
            Some(child) => child.try_wait()?.is_none(),
            None => false,
        };
        if running {
            self.wait_ready("Broker", self.broker_health).await
        } else {
            self.broker.take();
            self.spawn_broker()?;
            self.wait_ready("Broker", self.broker_health).await
        }
    }

    fn spawn_broker(&mut self) -> E2eResult<()> {
        let config = self.broker_config.as_os_str();
        let child = OwnedChild::spawn(
            "Broker",
            &self.broker_bin,
            ["--configFile".as_ref(), config],
            self.broker_health,
            &self.root,
            &self.redactions,
        )?;
        self.broker = Some(child);
        Ok(())
    }

    pub async fn stop_broker(&mut self) -> E2eResult<()> {
        stop_owned_child(&mut self.broker).await
    }

    pub async fn stop_all(&mut self) -> E2eResult<()> {
        let broker = self.stop_broker().await;
        let namesrv = stop_owned_child(&mut self.namesrv).await;
        broker.and(namesrv)
    }

    pub fn all_reaped(&self) -> bool {
        self.namesrv.is_none() && self.broker.is_none()
    }

    pub fn sanitized_diagnostics(&self) -> String {
        let namesrv = self.namesrv.as_ref().map_or_else(
            || "NameServer=<not-owned>".to_owned(),
            |child| child.sanitized_diagnostics(),
        );
        let broker = self.broker.as_ref().map_or_else(
            || "Broker=<not-owned>".to_owned(),
            |child| child.sanitized_diagnostics(),
        );
        format!("NameServer[{namesrv}] Broker[{broker}]")
    }

    async fn wait_ready(&mut self, label: &str, port: u16) -> E2eResult<()> {
        let deadline = tokio::time::Instant::now() + READINESS_TIMEOUT;
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(400))
            .timeout(Duration::from_millis(800))
            .no_proxy()
            .build()
            .e2e("build readiness client")?;
        loop {
            let child = if label == "Broker" {
                self.broker.as_mut()
            } else {
                self.namesrv.as_mut()
            }
            .ok_or_else(|| E2eError::new(format!("{label} ownership is missing")))?;
            if let Some(status) = child.try_wait()? {
                return Err(E2eError::new(format!(
                    "{label} exited before readiness ({status}); {}",
                    child.sanitized_diagnostics()
                )));
            }
            let response = client.get(format!("http://127.0.0.1:{port}/readyz")).send().await;
            if response.is_ok_and(|response| response.status().is_success()) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(E2eError::new(format!(
                    "{label} readiness deadline expired; {}",
                    child.sanitized_diagnostics()
                )));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn stop_owned_child(child: &mut Option<OwnedChild>) -> E2eResult<()> {
    if let Some(owned) = child.as_mut() {
        owned.stop().await?;
    }
    child.take();
    Ok(())
}

struct OwnedChild {
    label: &'static str,
    child: Child,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    redactions: Vec<String>,
    reaped: bool,
}

impl OwnedChild {
    fn spawn<I, S>(
        label: &'static str,
        executable: &Path,
        args: I,
        health_port: u16,
        root: &Path,
        redactions: &[String],
    ) -> E2eResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let stdout_path = root.join(format!("{}.stdout.log", label.to_ascii_lowercase()));
        let stderr_path = root.join(format!("{}.stderr.log", label.to_ascii_lowercase()));
        let stdout = File::create(&stdout_path).e2e("create child stdout capture")?;
        let stderr = File::create(&stderr_path).e2e("create child stderr capture")?;
        let mut command = Command::new(executable);
        command
            .args(args)
            .env("ROCKETMQ_HOME", root)
            .env("ROCKETMQ_HEALTH_BIND_ADDR", format!("127.0.0.1:{health_port}"))
            .env("ROCKETMQ_SHUTDOWN_TIMEOUT_SECONDS", "5")
            .env("ROCKETMQ_SECURITY_PROFILE", "development-insecure-loopback")
            .env("RUST_LOG", "warn")
            .env_remove("NAMESRV_ADDR")
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        if label == "Broker" {
            command.env("RUST_MIN_STACK", BROKER_THREAD_STACK_BYTES.to_string());
        }
        let child = command.spawn().e2e(&format!("spawn {label}"))?;
        Ok(Self {
            label,
            child,
            stdout_path,
            stderr_path,
            redactions: redactions.to_vec(),
            reaped: false,
        })
    }

    fn try_wait(&mut self) -> E2eResult<Option<std::process::ExitStatus>> {
        let status = self.child.try_wait().e2e(&format!("inspect {} child", self.label))?;
        if status.is_some() {
            self.reaped = true;
        }
        Ok(status)
    }

    async fn stop(&mut self) -> E2eResult<()> {
        if self.reaped {
            return Ok(());
        }
        if self.try_wait()?.is_none() {
            if let Err(error) = self.child.kill() {
                if self.try_wait()?.is_none() {
                    return Err(E2eError::new(format!(
                        "terminate {} child: {error}; {}",
                        self.label,
                        self.sanitized_diagnostics()
                    )));
                }
                return Ok(());
            }
        }
        let deadline = tokio::time::Instant::now() + STOP_TIMEOUT;
        loop {
            if self.try_wait()?.is_some() {
                self.reaped = true;
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(E2eError::new(format!(
                    "{} did not exit before the reap deadline; {}",
                    self.label,
                    self.sanitized_diagnostics()
                )));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn force_reap(&mut self) -> E2eResult<()> {
        if self.reaped {
            return Ok(());
        }
        if self.try_wait()?.is_some() {
            return Ok(());
        }
        if let Err(error) = self.child.kill() {
            if self.try_wait()?.is_none() {
                return Err(E2eError::new(format!(
                    "terminate {} child during fallback reap: {error}; {}",
                    self.label,
                    self.sanitized_diagnostics()
                )));
            }
            return Ok(());
        }
        let deadline = Instant::now() + DROP_REAP_TIMEOUT;
        loop {
            if self.try_wait()?.is_some() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(E2eError::new(format!(
                    "{} did not exit before the fallback reap deadline; {}",
                    self.label,
                    self.sanitized_diagnostics()
                )));
            }
            std::thread::sleep(DROP_REAP_POLL_INTERVAL);
        }
    }

    fn sanitized_diagnostics(&self) -> String {
        format!(
            "stdout={} stderr={}",
            sanitize_diagnostic(&tail(&self.stdout_path), &self.redactions),
            sanitize_diagnostic(&tail(&self.stderr_path), &self.redactions)
        )
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.force_reap();
    }
}

fn required_binary(environment: &str) -> E2eResult<PathBuf> {
    let path = std::env::var_os(environment)
        .map(PathBuf::from)
        .ok_or_else(|| E2eError::new(format!("{environment} is required")))?;
    if !path.is_file() {
        return Err(E2eError::new(format!(
            "{environment} does not identify a built executable"
        )));
    }
    Ok(path)
}

fn tail(path: &Path) -> String {
    let Ok(mut file) = File::open(path) else {
        return "<unavailable>".to_owned();
    };
    let length = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    if length > DIAGNOSTIC_BYTES {
        use std::io::Seek;
        let _ = file.seek(std::io::SeekFrom::Start(length - DIAGNOSTIC_BYTES));
    }
    let mut text = String::new();
    let _ = file.read_to_string(&mut text);
    text
}

fn push_path_redactions(redactions: &mut Vec<String>, path: &Path) {
    let native = path.to_string_lossy().into_owned();
    if !native.is_empty() {
        redactions.push(native.replace('\\', "/"));
        redactions.push(native);
    }
}

fn sanitize_diagnostic(input: &str, redactions: &[String]) -> String {
    let mut diagnostic = strip_terminal_sequences(input)
        .chars()
        .map(|character| {
            if character == '\r' || character == '\n' || character == '\t' {
                ' '
            } else if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    for sensitive in redactions {
        if !sensitive.is_empty() {
            let escaped = json_escaped_string_contents(sensitive);
            if !escaped.is_empty() {
                diagnostic = diagnostic.replace(&escaped, "<redacted>");
            }
            diagnostic = diagnostic.replace(sensitive, "<redacted>");
        }
    }
    diagnostic
}

fn json_escaped_string_contents(value: &str) -> String {
    serde_json::to_string(value)
        .ok()
        .and_then(|encoded| {
            encoded
                .strip_prefix('"')
                .and_then(|text| text.strip_suffix('"'))
                .map(str::to_owned)
        })
        .unwrap_or_default()
}

fn strip_terminal_sequences(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut characters = input.chars().peekable();
    while let Some(character) = characters.next() {
        if character != '\u{1b}' {
            output.push(character);
            continue;
        }
        match characters.next() {
            Some('[') => {
                for next in characters.by_ref() {
                    if ('@'..='~').contains(&next) {
                        break;
                    }
                }
            }
            Some(']') => {
                let mut escaped = false;
                for next in characters.by_ref() {
                    if next == '\u{7}' || (escaped && next == '\\') {
                        break;
                    }
                    escaped = next == '\u{1b}';
                }
            }
            Some(_) | None => {}
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use std::process::Stdio;
    use std::time::Duration;
    use std::time::Instant;

    use super::sanitize_diagnostic;
    use super::OwnedChild;
    use super::DROP_REAP_TIMEOUT;

    const REAP_HELPER_ENV: &str = "ROCKETMQ_MCP_CONTROL_REAP_HELPER";
    const REAP_HELPER_TEST: &str = "transport::real_cluster_e2e::process::tests::owned_child_reap_helper";

    #[test]
    fn owned_child_reap_helper() {
        if std::env::var(REAP_HELPER_ENV).as_deref() == Ok("1") {
            loop {
                std::thread::park();
            }
        }
    }

    #[test]
    fn force_reap_is_bounded_and_reports_reaped_only_after_exit() {
        let root = tempfile::tempdir().expect("create child-reap test root");
        let child = Command::new(std::env::current_exe().expect("locate current test executable"))
            .args(["--exact", REAP_HELPER_TEST])
            .env(REAP_HELPER_ENV, "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn owned child-reap helper");
        let mut owned = OwnedChild {
            label: "reap-test",
            child,
            stdout_path: root.path().join("stdout.log"),
            stderr_path: root.path().join("stderr.log"),
            redactions: Vec::new(),
            reaped: false,
        };

        let started = Instant::now();
        owned.force_reap().expect("reap helper within its deadline");

        assert!(started.elapsed() <= DROP_REAP_TIMEOUT + Duration::from_secs(1));
        assert!(owned.reaped);
        assert!(owned.try_wait().expect("inspect reaped helper").is_some());
    }

    #[test]
    fn diagnostics_remove_terminal_sequences_paths_and_endpoints() {
        let redactions = vec![
            r"D:\owned\cluster".to_owned(),
            "127.0.0.1".to_owned(),
            "43210".to_owned(),
            "message-payload".to_owned(),
        ];
        let diagnostic = sanitize_diagnostic(
            "\u{1b}[31mfailed\u{1b}[0m at D:\\owned\\cluster\\broker on 127.0.0.1:43210 message-payload\n",
            &redactions,
        );

        assert_eq!(
            diagnostic,
            "failed at <redacted>\\broker on <redacted>:<redacted> <redacted> "
        );
        assert!(!diagnostic.contains('\u{1b}'));
    }

    #[test]
    fn diagnostics_redact_json_escaped_windows_paths() {
        const WINDOWS_PATH: &str = r"D:\owned\cluster";
        let diagnostic = sanitize_diagnostic(
            r#"child={"diagnostic_path":"D:\\owned\\cluster\\broker"}"#,
            &[WINDOWS_PATH.to_owned()],
        );

        assert_eq!(diagnostic, r#"child={"diagnostic_path":"<redacted>\\broker"}"#);
        assert!(!diagnostic.contains("owned"));
    }
}
