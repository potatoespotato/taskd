//! Types shared by the daemon and the MCP shim.

use serde::{Deserialize, Serialize};

pub const DEFAULT_PORT: u16 = 8787;
pub const DEFAULT_NOTIFY_TAIL: u32 = 20;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRequest {
    pub cmd: String,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub notify_tail: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Running,
    Exited,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSummary {
    pub task_id: String,
    pub label: String,
    pub status: Status,
    pub rc: Option<i32>,
    pub pid: Option<u32>,
    pub cmd: String,
    pub cwd: String,
    pub duration_s: f64,
    pub log_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskList {
    pub tasks: Vec<TaskSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskLogs {
    #[serde(flatten)]
    pub task: TaskSummary,
    pub lines: Vec<String>,
}

/// `1h02m` / `5m12s` / `6s`
pub fn human_duration(seconds: f64) -> String {
    let s = seconds as u64;
    match s {
        0..60 => format!("{s}s"),
        60..3600 => format!("{}m{:02}s", s / 60, s % 60),
        _ => format!("{}h{:02}m", s / 3600, (s % 3600) / 60),
    }
}

/// The frame pushed over WebSocket when a task ends. Line-oriented on purpose:
/// Monitor turns one frame into one notification.
pub fn completion_frame(task: &TaskSummary, tail: &[String]) -> String {
    let verdict = if task.rc == Some(0) { "ok" } else { "FAILED" };
    let rc = task.rc.map_or_else(|| "?".to_string(), |c| c.to_string());
    let head = format!(
        "[taskd] {} {} rc={} in {} (task {}) log={}",
        task.label,
        verdict,
        rc,
        human_duration(task.duration_s),
        task.task_id,
        task.log_path
    );
    if tail.is_empty() {
        return head;
    }
    let mut out = vec![head, format!("--- last {} lines ---", tail.len())];
    out.extend(tail.iter().cloned());
    out.join("\n")
}
