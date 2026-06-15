// SPDX-License-Identifier: MIT OR Apache-2.0
//! SQLite-backed persistence layer (M1).
//!
//! The daemon's in-memory `AppStateInner` is the hot path for reads and
//! writes; this module is the durable mirror that survives restarts.
//! `Store` is an `Arc<Mutex<Connection>>` that the daemon's state
//! layer takes a clone of and uses for write-through persistence
//! alongside the in-memory HashMaps.
//!
//! Design choices:
//!
//! - **`rusqlite` with `bundled`.** The SQLite amalgamation is built
//!   from source, so the daemon has no system-level `libsqlite3`
//!   dependency and works on macOS, Linux, and CI containers without
//!   any extra setup.
//! - **Plain `Connection`, no pool.** The daemon is a single
//!   multi-threaded process; one `Connection` behind a `Mutex` is
//!   sufficient and avoids pulling in `r2d2` / `deadpool-sqlite`.
//! - **Schema migrations via `PRAGMA user_version`.** Idempotent
//!   `apply_migrations(&conn)` runs at every startup and brings the
//!   file up to the current schema in O(small) SQL.
//! - **WAL journal mode.** Enables concurrent readers (the daemon
//!   itself, plus external tools like `sqlite3` for debugging) without
//!   blocking writers.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use tracing::{debug, info};

use teamcomm_protocol::{
    InboxMessage, LiveState, MessageType, Priority, Reservation, ReservationMode, Session, Thread,
    ThreadStatus,
};

use crate::error::TeamcommError;

/// Current schema version. Bump whenever [`apply_migrations`] adds or
/// changes a migration step.
pub const SCHEMA_VERSION: i32 = 1;

/// Shared, cheaply-clonable handle to the SQLite store.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Mutex<Connection>>,
    path: PathBuf,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl Store {
    /// Open an in-memory SQLite store. Useful for tests.
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().context("failed to open in-memory SQLite")?;
        Self::from_connection(conn, PathBuf::from(":memory:"))
    }

    /// Open a file-backed store at `path`, creating the parent
    /// directory if necessary. Runs migrations on the fresh
    /// connection.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create store parent {}", parent.display())
                })?;
            }
        }
        let conn = Connection::open(path)
            .with_context(|| format!("failed to open SQLite store at {}", path.display()))?;
        let store = Self::from_connection(conn, path.to_path_buf())?;
        info!(path = %store.path.display(), "opened SQLite store");
        Ok(store)
    }

    fn from_connection(conn: Connection, path: PathBuf) -> Result<Self> {
        // WAL: concurrent readers + a single writer, no `SQLITE_BUSY`
        // round-trips for our read-heavy workload.
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("failed to set journal_mode=WAL")?;
        // Foreign keys are off by default in SQLite; we want them on so
        // the schema's FK annotations are enforced.
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("failed to enable foreign_keys")?;
        // Synchronous=NORMAL is the WAL-recommended setting: durable
        // across application crashes, faster than FULL.
        conn.pragma_update(None, "synchronous", "NORMAL")
            .context("failed to set synchronous=NORMAL")?;

        let store = Self {
            inner: Arc::new(Mutex::new(conn)),
            path,
        };
        {
            let mut guard = store.lock();
            apply_migrations(&mut guard)?;
        }
        Ok(store)
    }

    /// Path of the backing file (or `:memory:` for the in-memory store).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Acquire the underlying connection. Panics if the mutex is
    /// poisoned — this should never happen in practice because the
    /// only operations we perform are small, transactional SQL.
    pub(crate) fn lock(&self) -> MutexGuard<'_, Connection> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    // ===== Sessions =====

    pub(crate) fn upsert_session(&self, session: &Session) -> Result<()> {
        let capabilities_json = serde_json::to_string(&session.capabilities)?;
        let started_at = session.started_at.to_rfc3339();
        let last_heartbeat = session.last_heartbeat.to_rfc3339();
        let working_dir = session.working_dir.to_string_lossy().to_string();
        let agent_type_str = agent_type_to_string(&session.agent_type);

        let mut guard = self.lock();
        let tx = guard.transaction()?;
        tx.execute(
            "INSERT INTO sessions (session_id, agent_type, pid, started_at, working_dir, capabilities, last_heartbeat)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(session_id) DO UPDATE SET
                 agent_type = excluded.agent_type,
                 pid = excluded.pid,
                 started_at = excluded.started_at,
                 working_dir = excluded.working_dir,
                 capabilities = excluded.capabilities,
                 last_heartbeat = excluded.last_heartbeat",
            params![
                session.session_id,
                agent_type_str,
                session.pid,
                started_at,
                working_dir,
                capabilities_json,
                last_heartbeat,
            ],
        )?;
        tx.execute(
            "INSERT INTO sessions_by_pid (pid, session_id) VALUES (?1, ?2)
             ON CONFLICT(pid) DO UPDATE SET session_id = excluded.session_id",
            params![session.pid, session.session_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn delete_session(&self, session_id: &str) -> Result<()> {
        let mut guard = self.lock();
        let tx = guard.transaction()?;
        // Look up the pid so we can keep `sessions_by_pid` consistent.
        let pid: Option<i64> = tx
            .query_row(
                "SELECT pid FROM sessions WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .optional()?;
        tx.execute(
            "DELETE FROM sessions WHERE session_id = ?1",
            params![session_id],
        )?;
        if let Some(pid) = pid {
            tx.execute(
                "DELETE FROM sessions_by_pid WHERE pid = ?1",
                params![pid],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn get_session(&self, session_id: &str) -> Result<Option<Session>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(
            "SELECT session_id, agent_type, pid, started_at, working_dir, capabilities, last_heartbeat
             FROM sessions WHERE session_id = ?1",
        )?;
        let row = stmt
            .query_row(params![session_id], row_to_session)
            .optional()?;
        Ok(row)
    }

    pub(crate) fn get_session_by_pid(&self, pid: u32) -> Result<Option<Session>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(
            "SELECT s.session_id, s.agent_type, s.pid, s.started_at, s.working_dir, s.capabilities, s.last_heartbeat
             FROM sessions s
             JOIN sessions_by_pid sp ON sp.session_id = s.session_id
             WHERE sp.pid = ?1",
        )?;
        let row = stmt
            .query_row(params![pid], row_to_session)
            .optional()?;
        Ok(row)
    }

    pub(crate) fn list_sessions(&self) -> Result<Vec<Session>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(
            "SELECT session_id, agent_type, pid, started_at, working_dir, capabilities, last_heartbeat
             FROM sessions",
        )?;
        let rows = stmt
            .query_map([], row_to_session)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    // ===== Reservations =====

    pub(crate) fn upsert_reservation(&self, r: &Reservation) -> Result<()> {
        let mode_str = reservation_mode_to_string(r.mode);
        let acquired_at = r.acquired_at.to_rfc3339();
        let expires_at = r.expires_at.to_rfc3339();
        let path = r.path.to_string_lossy().to_string();

        let guard = self.lock();
        guard.execute(
            "INSERT INTO reservations (reservation_id, session_id, path, mode, acquired_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(reservation_id) DO UPDATE SET
                 session_id = excluded.session_id,
                 path = excluded.path,
                 mode = excluded.mode,
                 acquired_at = excluded.acquired_at,
                 expires_at = excluded.expires_at",
            params![
                r.reservation_id,
                r.session_id,
                path,
                mode_str,
                acquired_at,
                expires_at,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn delete_reservation(&self, id: &str) -> Result<()> {
        let guard = self.lock();
        guard.execute(
            "DELETE FROM reservations WHERE reservation_id = ?1",
            params![id],
        )?;
        Ok(())
    }

    pub(crate) fn list_reservations(&self) -> Result<Vec<Reservation>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(
            "SELECT reservation_id, session_id, path, mode, acquired_at, expires_at
             FROM reservations",
        )?;
        let rows = stmt
            .query_map([], row_to_reservation)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    // ===== Live state =====

    pub(crate) fn upsert_live_state(&self, s: &LiveState) -> Result<()> {
        let focus_file = s.focus_file.as_ref().map(|p| p.to_string_lossy().to_string());
        let worktree = s.worktree.as_ref().map(|p| p.to_string_lossy().to_string());
        let last_heartbeat = s.last_heartbeat.to_rfc3339();
        let status_str = s.status.as_str();

        let guard = self.lock();
        guard.execute(
            "INSERT INTO live_state
                 (session_id, focus_file, focus_branch, worktree, status, last_heartbeat)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(session_id) DO UPDATE SET
                 focus_file = excluded.focus_file,
                 focus_branch = excluded.focus_branch,
                 worktree = excluded.worktree,
                 status = excluded.status,
                 last_heartbeat = excluded.last_heartbeat",
            params![
                s.session_id,
                focus_file,
                s.focus_branch,
                worktree,
                status_str,
                last_heartbeat,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn get_live_state(&self, session_id: &str) -> Result<Option<LiveState>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(
            "SELECT session_id, focus_file, focus_branch, worktree, status, last_heartbeat
             FROM live_state WHERE session_id = ?1",
        )?;
        let row = stmt
            .query_row(params![session_id], row_to_live_state)
            .optional()?;
        Ok(row)
    }

    pub(crate) fn list_live_state(&self) -> Result<Vec<LiveState>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(
            "SELECT session_id, focus_file, focus_branch, worktree, status, last_heartbeat
             FROM live_state",
        )?;
        let rows = stmt
            .query_map([], row_to_live_state)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    // ===== Inbox =====

    pub(crate) fn insert_inbox_message(&self, m: &InboxMessage) -> Result<()> {
        let priority_str = m.priority.as_str();
        let message_type_str = m.message_type.as_str();
        let ts = m.ts.to_rfc3339();
        let references_json = m
            .references
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;

        let guard = self.lock();
        guard.execute(
            "INSERT INTO inbox_messages
                 (message_id, from_session, to_session, subject, body, priority,
                  message_type, thread_id, references, ts, read)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(message_id) DO UPDATE SET
                 read = excluded.read",
            params![
                m.message_id,
                m.from_session,
                m.to_session,
                m.subject,
                m.body,
                priority_str,
                message_type_str,
                m.thread_id,
                references_json,
                ts,
                m.read as i32,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn list_inbox(&self, session_id: &str) -> Result<Vec<InboxMessage>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(
            "SELECT message_id, from_session, to_session, subject, body, priority,
                    message_type, thread_id, references, ts, read
             FROM inbox_messages
             WHERE to_session = ?1
             ORDER BY ts ASC",
        )?;
        let rows = stmt
            .query_map(params![session_id], row_to_inbox)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub(crate) fn get_inbox_message(&self, id: &str) -> Result<Option<InboxMessage>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(
            "SELECT message_id, from_session, to_session, subject, body, priority,
                    message_type, thread_id, references, ts, read
             FROM inbox_messages
             WHERE message_id = ?1",
        )?;
        let row = stmt
            .query_row(params![id], row_to_inbox)
            .optional()?;
        Ok(row)
    }

    pub(crate) fn mark_inbox_read(&self, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let guard = self.lock();
        let mut stmt = guard
            .prepare("UPDATE inbox_messages SET read = 1 WHERE message_id = ?1")?;
        for id in ids {
            stmt.execute(params![id])?;
        }
        Ok(())
    }

    // ===== Threads =====

    pub(crate) fn upsert_thread(&self, t: &Thread) -> Result<()> {
        let created_at = t.created_at.to_rfc3339();
        let status_str = t.status.as_str();
        let guard = self.lock();
        guard.execute(
            "INSERT INTO threads (thread_id, title, topic, created_by, created_at, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(thread_id) DO UPDATE SET
                 title = excluded.title,
                 topic = excluded.topic,
                 status = excluded.status",
            params![
                t.thread_id,
                t.title,
                t.topic,
                t.created_by,
                created_at,
                status_str,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn get_thread(&self, id: &str) -> Result<Option<Thread>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(
            "SELECT thread_id, title, topic, created_by, created_at, status
             FROM threads WHERE thread_id = ?1",
        )?;
        let row = stmt
            .query_row(params![id], row_to_thread)
            .optional()?;
        Ok(row)
    }

    pub(crate) fn list_threads(&self, include_archived: bool) -> Result<Vec<Thread>> {
        let guard = self.lock();
        let sql = if include_archived {
            "SELECT thread_id, title, topic, created_by, created_at, status
             FROM threads ORDER BY created_at ASC"
        } else {
            "SELECT thread_id, title, topic, created_by, created_at, status
             FROM threads WHERE status = 'active' ORDER BY created_at ASC"
        };
        let mut stmt = guard.prepare(sql)?;
        let rows = stmt
            .query_map([], row_to_thread)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub(crate) fn delete_thread(&self, id: &str) -> Result<()> {
        let guard = self.lock();
        let tx = guard.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM thread_participants WHERE thread_id = ?1",
            params![id],
        )?;
        tx.execute("DELETE FROM threads WHERE thread_id = ?1", params![id])?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn add_thread_participant(
        &self,
        thread_id: &str,
        session_id: &str,
    ) -> Result<()> {
        let guard = self.lock();
        guard.execute(
            "INSERT OR IGNORE INTO thread_participants (thread_id, session_id, joined_at)
             VALUES (?1, ?2, ?3)",
            params![thread_id, session_id, Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    pub(crate) fn remove_thread_participant(
        &self,
        thread_id: &str,
        session_id: &str,
    ) -> Result<()> {
        let guard = self.lock();
        guard.execute(
            "DELETE FROM thread_participants WHERE thread_id = ?1 AND session_id = ?2",
            params![thread_id, session_id],
        )?;
        Ok(())
    }

    pub(crate) fn list_thread_participants(&self, thread_id: &str) -> Result<Vec<String>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(
            "SELECT session_id FROM thread_participants
             WHERE thread_id = ?1
             ORDER BY joined_at ASC",
        )?;
        let rows = stmt
            .query_map(params![thread_id], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Count inbox messages tagged with `thread_id`. Used to populate
    /// [`crate::state::ThreadDetails::message_count`].
    pub(crate) fn count_inbox_for_thread(&self, thread_id: &str) -> Result<u32> {
        let guard = self.lock();
        let n: i64 = guard.query_row(
            "SELECT COUNT(*) FROM inbox_messages WHERE thread_id = ?1",
            params![thread_id],
            |row| row.get(0),
        )?;
        Ok(n as u32)
    }
}

// ===== Migrations =====

/// Apply pending migrations to `conn`. Idempotent: re-running on a
/// schema that is already at the current version is a no-op.
fn apply_migrations(conn: &mut Connection) -> Result<()> {
    let current: i32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    debug!(current, target = SCHEMA_VERSION, "checking schema version");
    let tx = conn.transaction()?;

    if current < 1 {
        tx.execute_batch(SCHEMA_V1)?;
    }

    // Bump user_version to the current target. `user_version` is a
    // single integer PRAGMA that we treat as the schema version.
    tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    tx.commit()?;
    info!(version = SCHEMA_VERSION, "schema migrations applied");
    Ok(())
}

const SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS sessions (
    session_id     TEXT PRIMARY KEY,
    agent_type     TEXT NOT NULL,
    pid            INTEGER NOT NULL,
    started_at     TEXT NOT NULL,
    working_dir    TEXT NOT NULL,
    capabilities   TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions_by_pid (
    pid        INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS reservations (
    reservation_id TEXT PRIMARY KEY,
    session_id     TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    path           TEXT NOT NULL,
    mode           TEXT NOT NULL,
    acquired_at    TEXT NOT NULL,
    expires_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reservations_path ON reservations(path);
CREATE INDEX IF NOT EXISTS idx_reservations_session ON reservations(session_id);

CREATE TABLE IF NOT EXISTS live_state (
    session_id     TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
    focus_file     TEXT,
    focus_branch   TEXT,
    worktree       TEXT,
    status         TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS inbox_messages (
    message_id   TEXT PRIMARY KEY,
    from_session TEXT NOT NULL,
    to_session   TEXT NOT NULL,
    subject      TEXT NOT NULL,
    body         TEXT NOT NULL,
    priority     TEXT NOT NULL,
    message_type TEXT NOT NULL,
    thread_id    TEXT,
    references   TEXT,
    ts           TEXT NOT NULL,
    read         INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_inbox_to_session ON inbox_messages(to_session);
CREATE INDEX IF NOT EXISTS idx_inbox_thread_id ON inbox_messages(thread_id);
CREATE INDEX IF NOT EXISTS idx_inbox_from_session ON inbox_messages(from_session);

CREATE TABLE IF NOT EXISTS threads (
    thread_id  TEXT PRIMARY KEY,
    title      TEXT NOT NULL,
    topic      TEXT,
    created_by TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'active'
);
CREATE INDEX IF NOT EXISTS idx_threads_status ON threads(status);
CREATE INDEX IF NOT EXISTS idx_threads_created_by ON threads(created_by);

CREATE TABLE IF NOT EXISTS thread_participants (
    thread_id  TEXT NOT NULL REFERENCES threads(thread_id) ON DELETE CASCADE,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    joined_at  TEXT NOT NULL,
    PRIMARY KEY (thread_id, session_id)
);
CREATE INDEX IF NOT EXISTS idx_participants_session ON thread_participants(session_id);
"#;

// ===== Row decoders =====

fn row_to_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<Session> {
    let session_id: String = row.get(0)?;
    let agent_type_str: String = row.get(1)?;
    let pid: i64 = row.get(2)?;
    let started_at: String = row.get(3)?;
    let working_dir: String = row.get(4)?;
    let capabilities_json: String = row.get(5)?;
    let last_heartbeat: String = row.get(6)?;

    Ok(Session {
        session_id,
        agent_type: agent_type_from_string(&agent_type_str),
        pid: pid as u32,
        started_at: parse_rfc3339(&started_at, "sessions.started_at")?,
        working_dir: PathBuf::from(working_dir),
        capabilities: serde_json::from_str(&capabilities_json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?,
        last_heartbeat: parse_rfc3339(&last_heartbeat, "sessions.last_heartbeat")?,
    })
}

fn row_to_reservation(row: &rusqlite::Row<'_>) -> rusqlite::Result<Reservation> {
    let reservation_id: String = row.get(0)?;
    let session_id: String = row.get(1)?;
    let path: String = row.get(2)?;
    let mode_str: String = row.get(3)?;
    let acquired_at: String = row.get(4)?;
    let expires_at: String = row.get(5)?;

    Ok(Reservation {
        reservation_id,
        session_id,
        path: PathBuf::from(path),
        mode: reservation_mode_from_string(&mode_str)
            .ok_or_else(|| rusqlite_to_sql_decode_error("reservation.mode", mode_str))?,
        acquired_at: parse_rfc3339(&acquired_at, "reservations.acquired_at")?,
        expires_at: parse_rfc3339(&expires_at, "reservations.expires_at")?,
    })
}

fn row_to_live_state(row: &rusqlite::Row<'_>) -> rusqlite::Result<LiveState> {
    let session_id: String = row.get(0)?;
    let focus_file: Option<String> = row.get(1)?;
    let focus_branch: Option<String> = row.get(2)?;
    let worktree: Option<String> = row.get(3)?;
    let status_str: String = row.get(4)?;
    let last_heartbeat: String = row.get(5)?;

    let status = teamcomm_protocol::AgentStatus::parse(&status_str)
        .ok_or_else(|| rusqlite_to_sql_decode_error("live_state.status", status_str))?;
    Ok(LiveState {
        session_id,
        focus_file: focus_file.map(PathBuf::from),
        focus_branch,
        worktree: worktree.map(PathBuf::from),
        status,
        last_heartbeat: parse_rfc3339(&last_heartbeat, "live_state.last_heartbeat")?,
    })
}

fn row_to_inbox(row: &rusqlite::Row<'_>) -> rusqlite::Result<InboxMessage> {
    let message_id: String = row.get(0)?;
    let from_session: String = row.get(1)?;
    let to_session: String = row.get(2)?;
    let subject: String = row.get(3)?;
    let body: String = row.get(4)?;
    let priority_str: String = row.get(5)?;
    let message_type_str: String = row.get(6)?;
    let thread_id: Option<String> = row.get(7)?;
    let references_json: Option<String> = row.get(8)?;
    let ts: String = row.get(9)?;
    let read_i: i64 = row.get(10)?;

    let priority = Priority::parse(&priority_str)
        .ok_or_else(|| rusqlite_to_sql_decode_error("inbox.priority", priority_str))?;
    let message_type = MessageType::parse(&message_type_str)
        .ok_or_else(|| rusqlite_to_sql_decode_error("inbox.message_type", message_type_str))?;
    let references: Option<Vec<String>> = references_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?;

    Ok(InboxMessage {
        message_id,
        from_session,
        to_session,
        subject,
        body,
        priority,
        message_type,
        thread_id,
        references,
        ts: parse_rfc3339(&ts, "inbox.ts")?,
        read: read_i != 0,
    })
}

fn row_to_thread(row: &rusqlite::Row<'_>) -> rusqlite::Result<Thread> {
    let thread_id: String = row.get(0)?;
    let title: String = row.get(1)?;
    let topic: Option<String> = row.get(2)?;
    let created_by: String = row.get(3)?;
    let created_at: String = row.get(4)?;
    let status_str: String = row.get(5)?;
    let status = ThreadStatus::parse(&status_str)
        .ok_or_else(|| rusqlite_to_sql_decode_error("threads.status", status_str))?;
    Ok(Thread {
        thread_id,
        title,
        topic,
        created_by,
        created_at: parse_rfc3339(&created_at, "threads.created_at")?,
        status,
    })
}

fn parse_rfc3339(s: &str, ctx: &'static str) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(SqlDecodeError(format!("{ctx}: {e}"))),
            )
        })
}

fn rusqlite_to_sql_decode_error(ctx: &str, raw: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(SqlDecodeError(format!("{ctx}: unknown variant {raw}"))),
    )
}

/// Newtype so we can stuff any string into a `Box<dyn Error>` for
/// rusqlite's `FromSqlConversionFailure`.
#[derive(Debug)]
struct SqlDecodeError(String);
impl std::fmt::Display for SqlDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for SqlDecodeError {}

// ===== Wire-shape helpers =====

/// `AgentType` -> stable string for the `sessions.agent_type` column.
pub fn agent_type_to_string(t: &teamcomm_protocol::AgentType) -> String {
    use teamcomm_protocol::AgentType::*;
    match t {
        Forge => "forge".to_string(),
        Codex => "codex".to_string(),
        Claude => "claude".to_string(),
        Copilot => "copilot".to_string(),
        Custom(s) => format!("custom:{s}"),
    }
}

/// Inverse of [`agent_type_to_string`]. Falls back to
/// `AgentType::Custom(raw)` for any unknown tag.
pub fn agent_type_from_string(s: &str) -> teamcomm_protocol::AgentType {
    use teamcomm_protocol::AgentType::*;
    if let Some(rest) = s.strip_prefix("custom:") {
        return Custom(rest.to_string());
    }
    match s {
        "forge" => Forge,
        "codex" => Codex,
        "claude" => Claude,
        "copilot" => Copilot,
        // Backwards-compat: a bare "custom" with no inner string.
        "custom" => Custom(String::new()),
        other => Custom(other.to_string()),
    }
}

fn reservation_mode_to_string(m: ReservationMode) -> &'static str {
    match m {
        ReservationMode::Read => "read",
        ReservationMode::Write => "write",
        ReservationMode::Exclusive => "exclusive",
    }
}

fn reservation_mode_from_string(s: &str) -> Option<ReservationMode> {
    match s {
        "read" => Some(ReservationMode::Read),
        "write" => Some(ReservationMode::Write),
        "exclusive" => Some(ReservationMode::Exclusive),
        _ => None,
    }
}

// ===== Conversions to/from `TeamcommError` =====

/// Convenience: a DB error that should be reported as a 500-class
/// (Internal) JSON-RPC error.
pub fn db_to_teamcomm(e: anyhow::Error) -> TeamcommError {
    TeamcommError::Internal(format!("store error: {e}"))
}

/// Convert a rusqlite error (e.g. FK violation) to a sensible
/// `TeamcommError`. Use when the caller asked for something the store
/// rejected structurally.
pub fn sql_integrity_to_teamcomm(e: rusqlite::Error) -> TeamcommError {
    match e {
        rusqlite::Error::SqliteFailure(err, _)
            if err.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            TeamcommError::Conflict(format!("constraint violation: {err}"))
        }
        other => TeamcommError::Internal(format!("store error: {other}")),
    }
}

// Ensure unused imports don't sneak in if we add new helpers later.
#[allow(dead_code)]
fn _assert_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Store>();
    let _ = Utc.timestamp_opt(0, 0);
    let _ = anyhow!("unused");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_store_opens_and_initializes_schema() {
        let store = Store::in_memory().unwrap();
        let path = store.path();
        assert_eq!(path, Path::new(":memory:"));

        // user_version must be bumped to SCHEMA_VERSION.
        let guard = store.lock();
        let v: i32 = guard
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
    }

    #[test]
    fn in_memory_store_creates_all_expected_tables() {
        let store = Store::in_memory().unwrap();
        let guard = store.lock();
        let names: Vec<String> = guard
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        for required in [
            "sessions",
            "sessions_by_pid",
            "reservations",
            "live_state",
            "inbox_messages",
            "threads",
            "thread_participants",
        ] {
            assert!(
                names.iter().any(|n| n == required),
                "expected table `{required}`, got: {names:?}"
            );
        }
    }

    #[test]
    fn migrations_are_idempotent() {
        let store = Store::in_memory().unwrap();
        // Running lock-and-bump a second time must be a no-op.
        {
            let mut guard = store.lock();
            apply_migrations(&mut guard).unwrap();
        }
        let guard = store.lock();
        let v: i32 = guard
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
    }

    #[test]
    fn open_creates_parent_dir_for_file_backed_store() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/store.sqlite");
        let _store = Store::open(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn agent_type_round_trip_through_string_helpers() {
        use teamcomm_protocol::AgentType::*;
        let cases = [
            Forge,
            Codex,
            Claude,
            Copilot,
            Custom("aider".to_string()),
        ];
        for t in &cases {
            let s = agent_type_to_string(t);
            let back = agent_type_from_string(&s);
            assert_eq!(&back, t, "roundtrip failed for {s}");
        }
    }
}
