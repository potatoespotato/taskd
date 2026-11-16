//! MCP shim over taskd.
//!
//! A thin redirector: every tool is one HTTP call to the daemon. Completion is
//! *not* delivered here — an MCP server cannot wake a Claude Code agent. The
//! agent watches each task's WebSocket with Monitor instead.

use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::handler::server::ServerHandler;
use rmcp::model::{ServerCapabilities, ServerInfo};
use rmcp::{ErrorData, serve_server, tool, tool_handler, tool_router, transport::stdio};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use taskd_core::DEFAULT_PORT;

fn port() -> u16 {
    std::env::var("TASKD_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PORT)
}

fn base() -> String {
    format!("http://127.0.0.1:{}", port())
}

fn ws_url(task_id: &str) -> String {
    format!("ws://127.0.0.1:{}/events?task_id={task_id}", port())
}

fn session_id() -> String {
    std::env::var("CLAUDE_CODE_SESSION_ID")
        .unwrap_or_else(|_| format!("pid-{}", std::process::id()))
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
struct RunParams {
    /// Shell command, run through `bash -c` in `cwd`.
    cmd: String,
    /// Working directory. Defaults to the daemon's HOME.
    cwd: Option<String>,
    /// Short name used in the completion notification.
    label: Option<String>,
    /// How many trailing log lines to include in the notification.
    notify_tail: Option<u32>,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
struct TaskIdParams {
    task_id: String,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
struct LogParams {
    task_id: String,
    tail: Option<u32>,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
struct StatusParams {
    /// Omit to list every task started by this session.
    task_id: Option<String>,
}

#[derive(Serialize, schemars::JsonSchema)]
struct Started {
    task_id: String,
    label: String,
    log_path: String,
    ws_url: String,
    instruction: String,
}

#[derive(Serialize, schemars::JsonSchema)]
struct Payload {
    body: Value,
}

fn fail(msg: impl Into<String>) -> ErrorData {
    ErrorData::internal_error(msg.into(), None)
}

/// Bring the daemon up on first use; a losing race just fails to bind.
fn ensure_daemon() -> Result<(), ErrorData> {
    let health = format!("{}/health", base());
    if ureq::get(&health).call().is_ok() {
        return Ok(());
    }
    let exe = std::env::current_exe().map_err(|e| fail(format!("current_exe: {e}")))?;
    let daemon = exe.with_file_name("taskd");
    if !daemon.exists() {
        return Err(fail(format!("daemon binary not found at {}", daemon.display())));
    }
    let log = state_dir().join("daemon.log");
    std::fs::create_dir_all(log.parent().unwrap()).map_err(|e| fail(format!("state dir: {e}")))?;
    let out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .map_err(|e| fail(format!("daemon log: {e}")))?;
    Command::new(&daemon)
        .stdin(Stdio::null())
        .stdout(out.try_clone().map_err(|e| fail(e.to_string()))?)
        .stderr(out)
        .process_group(0)
        .spawn()
        .map_err(|e| fail(format!("spawn daemon: {e}")))?;

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if ureq::get(&health).call().is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(fail(format!("taskd did not come up; see {}", log.display())))
}

fn state_dir() -> PathBuf {
    std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".local/state"))
        .join("taskd")
}

fn get(path: &str) -> Result<Value, ErrorData> {
    ensure_daemon()?;
    let mut resp = ureq::get(&format!("{}{path}", base()))
        .call()
        .map_err(|e| fail(format!("taskd GET {path}: {e}")))?;
    resp.body_mut()
        .read_json::<Value>()
        .map_err(|e| fail(format!("decode {path}: {e}")))
}

fn post(path: &str, body: Value) -> Result<Value, ErrorData> {
    ensure_daemon()?;
    let mut resp = ureq::post(&format!("{}{path}", base()))
        .send_json(&body)
        .map_err(|e| fail(format!("taskd POST {path}: {e}")))?;
    resp.body_mut()
        .read_json::<Value>()
        .map_err(|e| fail(format!("decode {path}: {e}")))
}

#[derive(Clone)]
struct Taskd;

#[tool_router]
impl Taskd {
    #[tool(
        description = "Start a long-running shell command under the taskd daemon and return \
                       immediately. Never poll it and never write an `until` loop for it: \
                       right away call Monitor({ws: {url: ws_url}, timeout_ms: 1800000}) for \
                       this task. Completion is pushed with the exit code and the tail of the \
                       log, then the socket closes and the watch ends by itself."
    )]
    fn task_run(&self, Parameters(p): Parameters<RunParams>) -> Result<Json<Started>, ErrorData> {
        let session = session_id();
        let cwd = p.cwd.or_else(|| {
            std::env::current_dir()
                .ok()
                .map(|d| d.display().to_string())
        });
        let task = post(
            "/run",
            json!({
                "cmd": p.cmd,
                "cwd": cwd,
                "label": p.label,
                "session_id": session,
                "notify_tail": p.notify_tail,
            }),
        )?;
        let task_id = task["task_id"].as_str().unwrap_or_default().to_string();
        let url = ws_url(&task_id);
        Ok(Json(Started {
            label: task["label"].as_str().unwrap_or_default().to_string(),
            log_path: task["log_path"].as_str().unwrap_or_default().to_string(),
            instruction: format!(
                "Do not poll. Call Monitor now with ws={{\"url\": \"{url}\"}} and \
                 timeout_ms=1800000, then keep working — completion arrives as a notification \
                 and the watch ends by itself. If the monitor expires first, re-arm it with the \
                 same url: a finished task is replayed on connect."
            ),
            ws_url: url,
            task_id,
        }))
    }

    #[tool(description = "Status of one task, or of every task started by this session.")]
    fn task_status(&self, Parameters(p): Parameters<StatusParams>) -> Result<Json<Payload>, ErrorData> {
        let path = match p.task_id {
            Some(id) => format!("/status?task_id={id}"),
            None => format!("/status?session_id={}", session_id()),
        };
        Ok(Json(Payload { body: get(&path)? }))
    }

    #[tool(description = "Last N lines of a task's combined stdout/stderr.")]
    fn task_logs(&self, Parameters(p): Parameters<LogParams>) -> Result<Json<Payload>, ErrorData> {
        let tail = p.tail.unwrap_or(100);
        Ok(Json(Payload {
            body: get(&format!("/logs?task_id={}&tail={tail}", p.task_id))?,
        }))
    }

    #[tool(description = "Send SIGTERM to a running task's process group.")]
    fn task_kill(&self, Parameters(p): Parameters<TaskIdParams>) -> Result<Json<Payload>, ErrorData> {
        Ok(Json(Payload {
            body: post("/kill", json!({ "task_id": p.task_id }))?,
        }))
    }
}

#[tool_handler]
impl ServerHandler for Taskd {
    fn get_info(&self) -> ServerInfo {
        // ServerInfo / Implementation are #[non_exhaustive]: mutate a default.
        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info.name = "taskd".into();
        info.server_info.version = env!("CARGO_PKG_VERSION").into();
        info.instructions = Some(
            "Long-running shell tasks. Start one with task_run, then immediately watch its \
             ws_url via Monitor({ws: {url}, timeout_ms: 1800000}); the socket closes once the \
             task ends. One monitor per task. Never poll task_status in a loop and never \
             write an `until` loop for a task."
                .into(),
        );
        info
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = serve_server(Taskd, stdio()).await?;
    service.waiting().await?;
    Ok(())
}
