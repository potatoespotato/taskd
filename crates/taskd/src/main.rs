//! taskd — owns long-running shell tasks so nobody has to poll them.
//!
//! The daemon forks the process, therefore holds its exit code, and pushes a
//! completion frame to every WebSocket subscriber of that task, then closes
//! the socket.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade, close_code};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{RwLock, broadcast};
use taskd_core::{
    DEFAULT_NOTIFY_TAIL, DEFAULT_PORT, RunRequest, Status, TaskList, TaskLogs, TaskSummary,
    completion_frame,
};

/// Only ever read the tail of a log file, never the whole thing — parity runs
/// produce megabytes.
const TAIL_WINDOW_BYTES: i64 = 256 * 1024;

#[derive(Clone)]
struct Event {
    task_id: String,
    text: String,
}

struct Entry {
    summary: TaskSummary,
    session_id: String,
    notify_tail: u32,
    started_at: SystemTime,
}

impl Entry {
    fn frame(&self) -> String {
        let tail = tail_lines(Path::new(&self.summary.log_path), self.notify_tail);
        completion_frame(&self.summary, &tail)
    }

    fn snapshot(&self) -> TaskSummary {
        let mut s = self.summary.clone();
        if s.status == Status::Running {
            s.duration_s = round1(elapsed(self.started_at));
        }
        s
    }
}

struct App {
    tasks: RwLock<HashMap<String, Entry>>,
    events: broadcast::Sender<Event>,
    log_dir: PathBuf,
}

type Shared = Arc<App>;

fn elapsed(since: SystemTime) -> f64 {
    since.elapsed().map(|d| d.as_secs_f64()).unwrap_or(0.0)
}

fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

fn next_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:08x}", (nanos ^ (n << 32)) & 0xffff_ffff)
}

fn state_dir() -> PathBuf {
    std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var("HOME").expect("HOME must be set")).join(".local/state")
        })
        .join("taskd")
}

/// Last `count` lines, read from the end of the file.
fn tail_lines(path: &Path, count: u32) -> Vec<String> {
    if count == 0 {
        return Vec::new();
    }
    let Ok(mut file) = std::fs::File::open(path) else {
        return Vec::new();
    };
    let len = file.metadata().map(|m| m.len() as i64).unwrap_or(0);
    let from_end = TAIL_WINDOW_BYTES.min(len);
    if file.seek(SeekFrom::End(-from_end)).is_err() {
        return Vec::new();
    }
    let mut buf = Vec::new();
    if file.read_to_end(&mut buf).is_err() {
        return Vec::new();
    }
    String::from_utf8_lossy(&buf)
        .lines()
        .rev()
        .take(count as usize)
        .map(str::to_owned)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

fn err(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, Json(json!({ "error": msg.into() }))).into_response()
}

async fn health(State(app): State<Shared>) -> impl IntoResponse {
    let tasks = app.tasks.read().await;
    let running = tasks
        .values()
        .filter(|e| e.summary.status == Status::Running)
        .count();
    Json(json!({
        "ok": true,
        "pid": std::process::id(),
        "tasks": tasks.len(),
        "running": running,
        "subscribers": app.events.receiver_count(),
    }))
}

async fn run(State(app): State<Shared>, Json(req): Json<RunRequest>) -> Response {
    let id = next_id();
    let cwd = req
        .cwd
        .unwrap_or_else(|| std::env::var("HOME").unwrap_or_else(|_| "/".into()));
    let label = req
        .label
        .unwrap_or_else(|| req.cmd.split_whitespace().next().unwrap_or("task").into());
    let session_id = req.session_id.unwrap_or_else(|| "unknown".into());
    let notify_tail = req.notify_tail.unwrap_or(DEFAULT_NOTIFY_TAIL);

    if let Err(e) = std::fs::create_dir_all(&app.log_dir) {
        return err(StatusCode::INTERNAL_SERVER_ERROR, format!("log dir: {e}"));
    }
    let log_path = app
        .log_dir
        .join(format!("{id}-{}.log", label.replace('/', "_")));
    let log_file = match std::fs::File::create(&log_path) {
        Ok(f) => f,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, format!("log file: {e}")),
    };
    let log_err = match log_file.try_clone() {
        Ok(f) => f,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, format!("dup log fd: {e}")),
    };

    let mut child = match tokio::process::Command::new("/bin/bash")
        .arg("-c")
        .arg(&req.cmd)
        .current_dir(&cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_err))
        .process_group(0) // own group, so kill takes the whole tree down
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return err(StatusCode::BAD_REQUEST, format!("spawn failed: {e}")),
    };

    let summary = TaskSummary {
        task_id: id.clone(),
        label,
        status: Status::Running,
        rc: None,
        pid: child.id(),
        cmd: req.cmd,
        cwd,
        duration_s: 0.0,
        log_path: log_path.display().to_string(),
    };
    let started_at = SystemTime::now();
    let response = summary.clone();

    app.tasks.write().await.insert(
        id.clone(),
        Entry { summary, session_id, notify_tail, started_at },
    );

    let app2 = app.clone();
    tokio::spawn(async move {
        let rc = child.wait().await.ok().and_then(|s| s.code());
        let frame = {
            let mut tasks = app2.tasks.write().await;
            let Some(entry) = tasks.get_mut(&id) else { return };
            entry.summary.status = Status::Exited;
            entry.summary.rc = rc;
            entry.summary.duration_s = round1(elapsed(entry.started_at));
            entry.frame()
        };
        // No subscriber is not an error: a late one gets the frame on connect.
        let _ = app2.events.send(Event { task_id: id, text: frame });
    });

    Json(response).into_response()
}

#[derive(Deserialize)]
struct StatusQuery {
    task_id: Option<String>,
    session_id: Option<String>,
}

async fn status(State(app): State<Shared>, Query(q): Query<StatusQuery>) -> Response {
    let tasks = app.tasks.read().await;
    if let Some(id) = q.task_id {
        return match tasks.get(&id) {
            Some(e) => Json(e.snapshot()).into_response(),
            None => err(StatusCode::NOT_FOUND, format!("unknown task {id}")),
        };
    }
    let wanted = q.session_id.filter(|s| s != "*");
    let list: Vec<_> = tasks
        .values()
        .filter(|e| wanted.as_ref().is_none_or(|s| *s == e.session_id))
        .map(Entry::snapshot)
        .collect();
    Json(TaskList { tasks: list }).into_response()
}

#[derive(Deserialize)]
struct LogsQuery {
    task_id: String,
    #[serde(default = "default_tail")]
    tail: u32,
}

fn default_tail() -> u32 {
    100
}

async fn logs(State(app): State<Shared>, Query(q): Query<LogsQuery>) -> Response {
    let tasks = app.tasks.read().await;
    let Some(entry) = tasks.get(&q.task_id) else {
        return err(StatusCode::NOT_FOUND, format!("unknown task {}", q.task_id));
    };
    let task = entry.snapshot();
    let lines = tail_lines(Path::new(&task.log_path), q.tail);
    Json(TaskLogs { task, lines }).into_response()
}

#[derive(Deserialize)]
struct KillRequest {
    task_id: String,
}

async fn kill(State(app): State<Shared>, Json(req): Json<KillRequest>) -> Response {
    let tasks = app.tasks.read().await;
    let Some(entry) = tasks.get(&req.task_id) else {
        return err(StatusCode::NOT_FOUND, format!("unknown task {}", req.task_id));
    };
    if entry.summary.status == Status::Running
        && let Some(pid) = entry.summary.pid
    {
        // Negative pid targets the process group created via process_group(0).
        unsafe { libc::kill(-(pid as i32), libc::SIGTERM) };
    }
    Json(entry.snapshot()).into_response()
}

#[derive(Deserialize)]
struct EventsQuery {
    task_id: String,
}

/// Frame of an already finished task, `None` while it still runs.
async fn finished_frame(app: &App, task_id: &str) -> Option<Option<String>> {
    let tasks = app.tasks.read().await;
    let entry = tasks.get(task_id)?;
    Some((entry.summary.status == Status::Exited).then(|| entry.frame()))
}

async fn events(
    State(app): State<Shared>,
    Query(q): Query<EventsQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    // Subscribe before the status check: an exit between the two still lands in rx.
    let rx = app.events.subscribe();
    let Some(done) = finished_frame(&app, &q.task_id).await else {
        return err(StatusCode::NOT_FOUND, format!("unknown task {}", q.task_id));
    };
    ws.on_upgrade(move |socket| pump(socket, rx, app, q.task_id, done))
}

/// One task per socket: deliver its completion frame, then close, so the
/// subscriber's watch ends together with the task.
async fn pump(
    mut socket: WebSocket,
    mut rx: broadcast::Receiver<Event>,
    app: Shared,
    task_id: String,
    done: Option<String>,
) {
    let frame = match done {
        Some(frame) => frame,
        None => match wait_for(&mut socket, &mut rx, &app, &task_id).await {
            Some(frame) => frame,
            None => return,
        },
    };
    if socket.send(Message::Text(frame.into())).await.is_err() {
        return;
    }
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: close_code::NORMAL,
            reason: Utf8Bytes::from_static("task finished"),
        })))
        .await;
}

async fn wait_for(
    socket: &mut WebSocket,
    rx: &mut broadcast::Receiver<Event>,
    app: &App,
    task_id: &str,
) -> Option<String> {
    loop {
        tokio::select! {
            incoming = socket.recv() => match incoming {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return None,
                Some(Ok(_)) => {}
            },
            event = rx.recv() => match event {
                Ok(ev) if ev.task_id == task_id => return Some(ev.text),
                Ok(_) => {}
                Err(broadcast::error::RecvError::Closed) => return None,
                // Our event may be among the skipped ones: ask the registry.
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    if let Some(Some(frame)) = finished_frame(app, task_id).await {
                        return Some(frame);
                    }
                }
            },
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = std::env::var("TASKD_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PORT);
    let (events_tx, _) = broadcast::channel(1024);
    let app = Arc::new(App {
        tasks: RwLock::new(HashMap::new()),
        events: events_tx,
        log_dir: state_dir().join("logs"),
    });

    let router = Router::new()
        .route("/health", get(health))
        .route("/run", post(run))
        .route("/status", get(status))
        .route("/logs", get(logs))
        .route("/kill", post(kill))
        .route("/events", get(events))
        .with_state(app);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    // A second instance simply fails to bind — that is the single-instance lock.
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("taskd listening on http://{addr}");
    axum::serve(listener, router).await?;
    Ok(())
}
