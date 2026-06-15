// SPDX-License-Identifier: MIT OR Apache-2.0
//! SQLite-backed persistence layer.
//!
//! The daemon's in-memory `AppStateInner` is the hot path for reads and
//! writes; this module is the durable mirror that survives restarts.
//! [`Store`] is an `Arc<Mutex<Connection>>` that the daemon's state
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
//!
//! M2 scope: the durable store covers **reservations**. Sessions,
//! inbox, threads, and live-state are tracked in memory only at the
//! M2 milestone and will be wired into the store in the next milestone.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use tracing::{debug, info};

use teamcomm_protocol::{Reservation, ReservationMode};

use crate::error::TeamcommError;

/// Process-global default [`Store`].
///
/// The daemon's `main` installs a file-backed store at startup; handlers
/// call [`Store::persist_reservation`] which delegates to this global.
/// When the global is unset (e.g. in unit tests that never install
/// one), the helper falls back to an in-memory store so that calls
/// never panic — the persistence is best-effort.
static GLOBAL_STORE: OnceLock<Store> = OnceLock::new();

/// Current schema version. Bump whenever [`apply_migrations`] adds or
/// changes a migration step.
pub const SCHEMA_VERSION: i32 = 2;

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
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    // ===== Reservations =====

    /// Persist a reservation via the process-global [`Store`].
    ///
    /// Handlers call this after a successful in-memory `claim`. If the
    /// daemon's `main` has not installed a global store (e.g. inside a
    /// unit test), this falls back to an in-memory store so the call
    /// is infallible in practice — the worst case is a silent
    /// no-persist (the in-memory store is dropped at process exit).
    /// Errors are returned as `anyhow::Error` for the caller to log.
    pub fn persist_reservation(r: &Reservation) -> Result<()> {
        let store = Self::global();
        store.upsert_reservation(r)
    }

    /// Install `store` as the process-global default. Idempotent: a
    /// second call with the same store is a no-op; a different store
    /// after one is installed is logged and ignored.
    pub fn install_global(store: Store) {
        // First install wins. If a test or main tries to overwrite, we
        // keep the first — the daemon must own its persistence target.
        if GLOBAL_STORE.set(store.clone()).is_err() {
            debug!("process-global Store already installed; ignoring second install");
        }
    }

    /// Borrow the process-global store. Initialises an in-memory one
    /// on first call so handlers never panic. The fallback store lives
    /// for the rest of the process (intentional: it is a `&'static`),
    /// which is fine for both unit tests and any code path that calls
    /// `persist_reservation` before `main` has installed a file-
    /// backed store.
    pub fn global() -> &'static Store {
        GLOBAL_STORE.get_or_init(Self::fallback_in_memory)
    }

    fn fallback_in_memory() -> Store {
        let conn = Connection::open_in_memory().expect("in-memory SQLite is always available");
        Self::from_connection(conn, PathBuf::from(":memory:")).expect("fresh in-memory store OK")
    }

    /// Persist (or update) a single reservation.
    pub(crate) fn upsert_reservation(&self, r: &Reservation) -> Result<()> {
        let mode_str = reservation_mode_to_string(r.mode);
        let acquired_at = r.acquired_at.to_rfc3339();
        let expires_at = r.expires_at.to_rfc3339();
        let path = r.path.to_string_lossy().to_string();
        // M2: a `is_pattern` flag lets the conflict detector recognise
        // glob-shaped reservations without re-parsing the path on
        // every query.
        let is_pattern = is_glob_path(r.path.as_path());

        let guard = self.lock();
        guard.execute(
            "INSERT INTO reservations (reservation_id, session_id, path, mode, acquired_at, expires_at, is_pattern)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(reservation_id) DO UPDATE SET
                 session_id = excluded.session_id,
                 path = excluded.path,
                 mode = excluded.mode,
                 acquired_at = excluded.acquired_at,
                 expires_at = excluded.expires_at,
                 is_pattern = excluded.is_pattern",
            params![
                r.reservation_id,
                r.session_id,
                path,
                mode_str,
                acquired_at,
                expires_at,
                is_pattern as i32,
            ],
        )?;
        Ok(())
    }

    /// Delete a reservation by id. No-op if the id is unknown.
    pub fn delete_reservation(&self, id: &str) -> Result<()> {
        let guard = self.lock();
        guard.execute(
            "DELETE FROM reservations WHERE reservation_id = ?1",
            params![id],
        )?;
        Ok(())
    }

    /// Look up a single reservation by id.
    pub fn get_reservation(&self, id: &str) -> Result<Option<Reservation>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(
            "SELECT reservation_id, session_id, path, mode, acquired_at, expires_at
             FROM reservations WHERE reservation_id = ?1",
        )?;
        let row = stmt.query_row(params![id], row_to_reservation).ok();
        Ok(row)
    }

    /// List every reservation currently in the store. Callers are
    /// responsible for filtering out expired / released entries if
    /// needed.
    pub fn list_reservations(&self) -> Result<Vec<Reservation>> {
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

    /// List every reservation owned by `session_id`.
    pub fn list_reservations_for_session(&self, session_id: &str) -> Result<Vec<Reservation>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(
            "SELECT reservation_id, session_id, path, mode, acquired_at, expires_at
             FROM reservations WHERE session_id = ?1",
        )?;
        let rows = stmt
            .query_map(params![session_id], row_to_reservation)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Delete every reservation owned by `session_id`. Returns the
    /// number of rows removed. Used on session deregister.
    pub fn delete_reservations_for_session(&self, session_id: &str) -> Result<usize> {
        let guard = self.lock();
        let n = guard.execute(
            "DELETE FROM reservations WHERE session_id = ?1",
            params![session_id],
        )?;
        Ok(n)
    }

    /// Count reservations that have the given exact path string. The
    /// check is naive (path-equality only) and is intended for
    /// debugging and quick probes, not the authoritative conflict
    /// check. The authoritative check is in
    /// [`crate::conflict::detect_conflicts`].
    pub fn count_reservations_for_path(&self, path: &str) -> Result<u32> {
        let guard = self.lock();
        let n: i64 = guard.query_row(
            "SELECT COUNT(*) FROM reservations WHERE path = ?1",
            params![path],
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
    if current < 2 {
        tx.execute_batch(SCHEMA_V2_ADD_PATTERN_FLAG)?;
    }

    // Bump user_version to the current target. `user_version` is a
    // single integer PRAGMA that we treat as the schema version.
    tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    tx.commit()?;
    info!(version = SCHEMA_VERSION, "schema migrations applied");
    Ok(())
}

const SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id TEXT PRIMARY KEY,
    session_id     TEXT NOT NULL,
    path           TEXT NOT NULL,
    mode           TEXT NOT NULL,
    acquired_at    TEXT NOT NULL,
    expires_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reservations_path ON reservations(path);
CREATE INDEX IF NOT EXISTS idx_reservations_session ON reservations(session_id);
"#;

const SCHEMA_V2_ADD_PATTERN_FLAG: &str = r#"
ALTER TABLE reservations ADD COLUMN is_pattern INTEGER NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_reservations_is_pattern ON reservations(is_pattern);
"#;

// ===== Row decoders =====

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
        mode: reservation_mode_from_string(&mode_str).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unknown reservation.mode: {mode_str}"),
                )),
            )
        })?,
        acquired_at: parse_rfc3339(&acquired_at, "reservations.acquired_at")?,
        expires_at: parse_rfc3339(&expires_at, "reservations.expires_at")?,
    })
}

fn parse_rfc3339(s: &str, ctx: &'static str) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{ctx}: {e}"),
                )),
            )
        })
}

// ===== Wire-shape helpers =====

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

/// Cheap glob-detection: `*` / `?` / `[` anywhere in the string. Used
/// only to populate the `is_pattern` column; the authoritative
/// pattern detection is `PathPattern::new(...).is_literal()`.
fn is_glob_path(p: &Path) -> bool {
    p.to_string_lossy().contains(['*', '?', '['])
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
#[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample_reservation(id: &str, path: &str) -> Reservation {
        Reservation {
            reservation_id: id.into(),
            session_id: "sess-1".into(),
            path: PathBuf::from(path),
            mode: ReservationMode::Write,
            acquired_at: Utc.with_ymd_and_hms(2026, 6, 14, 12, 0, 0).unwrap(),
            expires_at: Utc.with_ymd_and_hms(2026, 6, 14, 12, 30, 0).unwrap(),
        }
    }

    #[test]
    fn in_memory_store_opens_and_initializes_schema() {
        let store = Store::in_memory().unwrap();
        let path = store.path();
        assert_eq!(path, Path::new(":memory:"));

        let guard = store.lock();
        let v: i32 = guard
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
    }

    #[test]
    fn in_memory_store_creates_reservations_table() {
        let store = Store::in_memory().unwrap();
        let guard = store.lock();
        let names: Vec<String> = guard
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            names.iter().any(|n| n == "reservations"),
            "expected table `reservations`, got: {names:?}"
        );
    }

    #[test]
    fn migrations_are_idempotent() {
        let store = Store::in_memory().unwrap();
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
    fn reservation_round_trip_via_store() {
        let store = Store::in_memory().unwrap();
        let r = sample_reservation("res-1", "/repo/src/lib.rs");
        store.upsert_reservation(&r).unwrap();
        let back = store.get_reservation("res-1").unwrap().unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn reservation_upsert_updates_existing() {
        let store = Store::in_memory().unwrap();
        let mut r = sample_reservation("res-1", "/repo/src/lib.rs");
        store.upsert_reservation(&r).unwrap();
        r.mode = ReservationMode::Exclusive;
        store.upsert_reservation(&r).unwrap();
        let back = store.get_reservation("res-1").unwrap().unwrap();
        assert_eq!(back.mode, ReservationMode::Exclusive);
    }

    #[test]
    fn reservation_delete_removes_row() {
        let store = Store::in_memory().unwrap();
        let r = sample_reservation("res-1", "/repo/src/lib.rs");
        store.upsert_reservation(&r).unwrap();
        store.delete_reservation("res-1").unwrap();
        assert!(store.get_reservation("res-1").unwrap().is_none());
    }

    #[test]
    fn reservation_list_returns_all_rows() {
        let store = Store::in_memory().unwrap();
        store
            .upsert_reservation(&sample_reservation("res-1", "/a"))
            .unwrap();
        store
            .upsert_reservation(&sample_reservation("res-2", "/b"))
            .unwrap();
        store
            .upsert_reservation(&sample_reservation("res-3", "/c"))
            .unwrap();
        let all = store.list_reservations().unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn list_reservations_for_session_filters_correctly() {
        let store = Store::in_memory().unwrap();
        store
            .upsert_reservation(&sample_reservation("res-1", "/a"))
            .unwrap();
        let mut r2 = sample_reservation("res-2", "/b");
        r2.session_id = "sess-2".into();
        store.upsert_reservation(&r2).unwrap();

        let s1 = store.list_reservations_for_session("sess-1").unwrap();
        assert_eq!(s1.len(), 1);
        assert_eq!(s1[0].reservation_id, "res-1");

        let s2 = store.list_reservations_for_session("sess-2").unwrap();
        assert_eq!(s2.len(), 1);
        assert_eq!(s2[0].reservation_id, "res-2");
    }

    #[test]
    fn delete_reservations_for_session_removes_only_that_session() {
        let store = Store::in_memory().unwrap();
        store
            .upsert_reservation(&sample_reservation("res-1", "/a"))
            .unwrap();
        store
            .upsert_reservation(&sample_reservation("res-2", "/b"))
            .unwrap();
        let n = store.delete_reservations_for_session("sess-1").unwrap();
        assert_eq!(n, 2);
        let all = store.list_reservations().unwrap();
        assert!(all.is_empty());
    }

    #[test]
    fn delete_reservations_for_session_leaves_other_sessions_alone() {
        let store = Store::in_memory().unwrap();
        store
            .upsert_reservation(&sample_reservation("res-1", "/a"))
            .unwrap();
        let mut r2 = sample_reservation("res-2", "/b");
        r2.session_id = "sess-2".into();
        store.upsert_reservation(&r2).unwrap();
        let n = store.delete_reservations_for_session("sess-1").unwrap();
        assert_eq!(n, 1);
        let all = store.list_reservations().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].session_id, "sess-2");
    }

    #[test]
    fn count_reservations_for_path_returns_correct_count() {
        let store = Store::in_memory().unwrap();
        store
            .upsert_reservation(&sample_reservation("res-1", "/a"))
            .unwrap();
        store
            .upsert_reservation(&sample_reservation("res-2", "/a"))
            .unwrap();
        let mut r3 = sample_reservation("res-3", "/b");
        r3.session_id = "sess-2".into();
        store.upsert_reservation(&r3).unwrap();
        assert_eq!(store.count_reservations_for_path("/a").unwrap(), 2);
        assert_eq!(store.count_reservations_for_path("/b").unwrap(), 1);
        assert_eq!(store.count_reservations_for_path("/c").unwrap(), 0);
    }

    #[test]
    fn is_pattern_flag_set_for_glob_paths() {
        let store = Store::in_memory().unwrap();
        store
            .upsert_reservation(&sample_reservation("res-1", "/repo/src/**"))
            .unwrap();
        let guard = store.lock();
        let is_pat: i64 = guard
            .query_row(
                "SELECT is_pattern FROM reservations WHERE reservation_id = ?1",
                params!["res-1"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(is_pat, 1);
    }

    #[test]
    fn is_pattern_flag_unset_for_literal_paths() {
        let store = Store::in_memory().unwrap();
        store
            .upsert_reservation(&sample_reservation("res-1", "/repo/src/lib.rs"))
            .unwrap();
        let guard = store.lock();
        let is_pat: i64 = guard
            .query_row(
                "SELECT is_pattern FROM reservations WHERE reservation_id = ?1",
                params!["res-1"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(is_pat, 0);
    }

    #[test]
    fn is_glob_path_helper_detects_wildcards() {
        assert!(is_glob_path(Path::new("/repo/src/*")));
        assert!(is_glob_path(Path::new("/repo/src/file?.rs")));
        assert!(is_glob_path(Path::new("/repo/src/[abc].rs")));
        assert!(!is_glob_path(Path::new("/repo/src/lib.rs")));
    }

    #[test]
    fn reservation_mode_round_trip_through_string_helpers() {
        for m in [
            ReservationMode::Read,
            ReservationMode::Write,
            ReservationMode::Exclusive,
        ] {
            let s = reservation_mode_to_string(m);
            let back = reservation_mode_from_string(s).unwrap();
            assert_eq!(back, m);
        }
    }

    #[test]
    fn unknown_mode_string_returns_none() {
        assert!(reservation_mode_from_string("nonsense").is_none());
    }
}
