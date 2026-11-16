# taskd — long-running tasks without `until` loops

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

The daemon owns the process, so it owns the exit code. The agent polls
nothing: completion arrives as a push over WebSocket.

```
claude session ──stdio MCP──> mcp-taskd ──HTTP──> taskd (one per machine)
      ▲                                              │ fork/exec, log to file
      └── Monitor({ws: …/events?task_id=X}) ◄─────────┘ on exit, then close
```

**Why WebSocket and not MCP notifications:** an MCP server cannot wake the
agent in Claude Code — server→client messages are not shown to the model
(anthropics/claude-code#36665), and `notifications/progress` is bound to an
active request by the spec. The only working push channel is `Monitor({ws})`.

## Layout

| Crate | Binary | What it does |
|---|---|---|
| `crates/taskd-core` | — | shared types, completion frame format |
| `crates/taskd` | `taskd` (1.05 MiB) | axum daemon: HTTP `/run` `/status` `/logs` `/kill` `/health` + WS `/events?task_id=` |
| `crates/mcp-taskd` | `mcp-taskd` (1.57 MiB) | stdio MCP server on rmcp 3.1.4: `task_run`, `task_status`, `task_logs`, `task_kill` |

Put both binaries in one directory on `PATH` (for example `~/.local/bin/`).
The daemon starts by itself on the first `task_run` call: `mcp-taskd` looks
for `taskd` next to its own binary. Port — `TASKD_PORT` (default 8787). Task
logs — `~/.local/state/taskd/logs/`.

## Memory

| | RSS | Binary |
|---|---|---|
| `taskd` (after 12 tasks) | **4.1 MiB** | 1.05 MiB |
| `mcp-taskd` (after initialize) | **3.9 MiB** | 1.57 MiB |
| *previous Python version* | *42.8 / 211 MiB* | — |

## Build and register

```sh
cargo build --release
ln -s "$PWD/target/release/taskd" "$PWD/target/release/mcp-taskd" ~/.local/bin/
claude mcp add --scope user taskd ~/.local/bin/mcp-taskd
```

## Agent usage

```
task_run(cmd="just parity test", label="parity312")
  → {task_id, ws_url, log_path, instruction}
Monitor({ws: {url: ws_url}, timeout_ms: 1800000})   # right away, one per task
```

The agent keeps working. On exit an event arrives and the daemon closes the
socket (code 1000), so the monitor ends by itself:

```
[taskd] parity312 FAILED rc=1 in 5m12s (task 6f660dc7) log=~/.local/state/taskd/logs/6f660dc7-parity312.log
--- last 20 lines ---
...
```

## MVP limits (deliberately not done)

- **One socket — one task.** Connecting to a finished task returns its frame
  at once and closes the socket. A late `Monitor`, or a retry after
  `timeout_ms` expires (Monitor caps at 30 minutes), does not lose the event.
- **No persistence.** A daemon restart drops the task registry: `/events`
  for an old `task_id` returns 404.
- **Isolation by `session_id`** (`CLAUDE_CODE_SESSION_ID`) applies only to
  `task_status` without `task_id`: a session sees its own tasks,
  `session_id=*` shows all.
- One daemon per machine: a second instance just fails to bind.
- A task killed by a signal reports `rc=?` (the process has no exit code).
  The signal is not shown in the frame.

## Acceptance

`taskd` is checked with 10 parallel tasks with 1–10 s timeouts and mixed exit
codes: event order, session isolation, log tails, `kill`, 404 on an unknown
task.

## License

[MIT](LICENSE) © 2026 potatoespotato
