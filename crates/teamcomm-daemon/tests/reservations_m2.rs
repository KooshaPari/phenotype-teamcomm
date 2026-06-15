// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end M2 reservation tests over the Unix-socket JSON-RPC listener.
//!
//! FR-1: a literal-path `reservation.claim` succeeds and persists.
//! FR-2: a duplicate literal-path claim is rejected with a structured
//!        `Conflict` listing the blocking reservation.
//! FR-3: `reservation.claim_many` is atomic — if any path in the request
//!        is blocked, the whole call fails and no reservations are
//!        written.
//! FR-4: `reservation.pattern_claim` registers a glob and blocks future
//!        literal claims that match the pattern.
//! FR-5: `reservation.conflicts_for_path` is a read-only probe that does
//!        not change daemon state.
//! FR-6: `reservation.list_conflicts` returns the live overlap set.
//! FR-7: `reservation.release` removes both the in-memory reservation
//!        and the durable row.

use std::time::Duration;

use anyhow::Result;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::watch;
use tokio::time::timeout;

use teamcomm_daemon::{db, listener, DaemonConfig};

async fn wait_for_socket(path: &std::path::Path) -> Option<UnixStream> {
    for _ in 0..50 {
        match UnixStream::connect(path).await {
            Ok(s) => return Some(s),
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    None
}

struct Client {
    writer: tokio::net::unix::OwnedWriteHalf,
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
}

impl Client {
    fn new(stream: UnixStream) -> Self {
        let (read_half, write_half) = stream.into_split();
        Self {
            writer: write_half,
            reader: BufReader::new(read_half),
        }
    }

    async fn round_trip(&mut self, request: Value) -> Result<Value> {
        let line = format!("{request}\n");
        self.writer.write_all(line.as_bytes()).await?;
        let mut buf = String::new();
        let n = timeout(Duration::from_secs(2), self.reader.read_line(&mut buf))
            .await
            .expect("response within 2s")?;
        assert!(n > 0, "expected at least one byte back, got EOF");
        Ok(serde_json::from_str(buf.trim())?)
    }
}

/// Boot a daemon against `dir` and install a fresh in-memory SQLite
/// store as the process-global default. Returns the configured paths
/// plus a shutdown sender.
async fn boot_daemon(
    dir: &std::path::Path,
) -> Result<(
    DaemonConfig,
    tokio::task::JoinHandle<Result<()>>,
    watch::Sender<bool>,
)> {
    let socket_path = dir.join("resv.sock");
    let pid_file = dir.join("resv.pid");
    let config = DaemonConfig::from_args(Some(socket_path.clone()), Some(pid_file));

    // Install a per-test in-memory store so the handlers' `persist_*`
    // calls don't leak into each other.
    db::Store::install_global(db::Store::in_memory()?);

    let state = teamcomm_daemon::new_state();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let listener_task: tokio::task::JoinHandle<Result<()>> =
        tokio::spawn(listener::run(config.clone(), state.clone(), shutdown_rx));
    // Wait until the socket is connectable.
    let _ = wait_for_socket(&socket_path)
        .await
        .expect("daemon socket should be ready within 1s");
    Ok((config, listener_task, shutdown_tx))
}

/// Convenience: register a session and return its id.
async fn register_session(client: &mut Client, pid: u32) -> Result<String> {
    let resp = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 1, "method": "session.register",
            "params": { "pid": pid, "agent_type": "forge" }
        }))
        .await?;
    let id = resp["result"]["session_id"]
        .as_str()
        .expect("session_id present")
        .to_string();
    assert!(id.starts_with("sess_"));
    Ok(id)
}

// ---------------------------------------------------------------------
// FR-1: a successful literal-path claim.
// ---------------------------------------------------------------------
#[tokio::test]
async fn m2_fr1_literal_claim_succeeds_and_persists() -> Result<()> {
    let dir = TempDir::new()?;
    let (_cfg, listener_task, shutdown_tx) = boot_daemon(dir.path()).await?;

    let stream = UnixStream::connect(dir.path().join("resv.sock")).await?;
    let mut client = Client::new(stream);
    let sid = register_session(&mut client, 7001).await?;

    let resp = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 2, "method": "reservation.claim",
            "params": {
                "session_id": sid,
                "path": "/repo/src/lib.rs",
                "mode": "write",
                "ttl_sec": 60,
            }
        }))
        .await?;
    assert!(resp.get("error").is_none(), "claim must succeed: {resp}");
    let result = &resp["result"];
    let reservation = &result["reservation"];
    assert!(reservation["reservation_id"]
        .as_str()
        .unwrap()
        .starts_with("resv_"));
    assert_eq!(result["conflicts"].as_array().unwrap().len(), 0);

    // Confirm it's persisted in the global store.
    let stored = db::Store::global()
        .get_reservation(reservation["reservation_id"].as_str().unwrap())?
        .expect("reservation must be in the store");
    assert_eq!(stored.path.to_string_lossy(), "/repo/src/lib.rs");
    assert_eq!(stored.session_id, sid);

    shutdown_tx.send(true).ok();
    let _ = timeout(Duration::from_secs(2), listener_task).await;
    Ok(())
}

// ---------------------------------------------------------------------
// FR-2: a duplicate literal claim is rejected with a structured conflict.
// ---------------------------------------------------------------------
#[tokio::test]
async fn m2_fr2_duplicate_literal_claim_rejected_with_conflict() -> Result<()> {
    let dir = TempDir::new()?;
    let (_cfg, listener_task, shutdown_tx) = boot_daemon(dir.path()).await?;
    let stream = UnixStream::connect(dir.path().join("resv.sock")).await?;
    let mut client = Client::new(stream);

    let sid_a = register_session(&mut client, 7010).await?;
    let sid_b = register_session(&mut client, 7011).await?;

    // First claim succeeds.
    let r1 = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 2, "method": "reservation.claim",
            "params": {
                "session_id": sid_a,
                "path": "/repo/src/lib.rs",
                "mode": "write"
            }
        }))
        .await?;
    assert!(r1.get("error").is_none());
    assert_eq!(r1["result"]["conflicts"].as_array().unwrap().len(), 0);

    // Second claim from a different session on the same path is rejected.
    let r2 = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 3, "method": "reservation.claim",
            "params": {
                "session_id": sid_b,
                "path": "/repo/src/lib.rs",
                "mode": "write"
            }
        }))
        .await?;
    assert!(r2.get("error").is_none());
    let result = &r2["result"];
    assert!(result["reservation"].is_null(), "no reservation granted");
    let conflicts = result["conflicts"].as_array().unwrap();
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0]["reason"], "exact_match");
    assert_eq!(
        conflicts[0]["existing"]["session_id"].as_str().unwrap(),
        sid_a
    );

    shutdown_tx.send(true).ok();
    let _ = timeout(Duration::from_secs(2), listener_task).await;
    Ok(())
}

// ---------------------------------------------------------------------
// FR-3: claim_many is atomic.
// ---------------------------------------------------------------------
#[tokio::test]
async fn m2_fr3_claim_many_is_atomic_under_conflict() -> Result<()> {
    let dir = TempDir::new()?;
    let (_cfg, listener_task, shutdown_tx) = boot_daemon(dir.path()).await?;
    let stream = UnixStream::connect(dir.path().join("resv.sock")).await?;
    let mut client = Client::new(stream);

    let sid_a = register_session(&mut client, 7020).await?;
    let sid_b = register_session(&mut client, 7021).await?;

    // Sid_a takes /repo/a.rs
    let _ = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 2, "method": "reservation.claim",
            "params": {
                "session_id": sid_a,
                "path": "/repo/a.rs",
                "mode": "write"
            }
        }))
        .await?;

    // Sid_b claims [a.rs, b.rs, c.rs] — a.rs is blocked.
    let resp = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 3, "method": "reservation.claim_many",
            "params": {
                "session_id": sid_b,
                "paths": ["/repo/a.rs", "/repo/b.rs", "/repo/c.rs"],
                "mode": "write"
            }
        }))
        .await?;
    assert!(resp.get("error").is_none());
    let result = &resp["result"];
    assert_eq!(result["claimed"].as_array().unwrap().len(), 0);
    let rejected = result["rejected"].as_array().unwrap();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0]["path"], "/repo/a.rs");

    // Sid_b then retries with just [b.rs, c.rs] — succeeds atomically.
    let resp2 = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 4, "method": "reservation.claim_many",
            "params": {
                "session_id": sid_b,
                "paths": ["/repo/b.rs", "/repo/c.rs"],
                "mode": "write"
            }
        }))
        .await?;
    assert!(resp2.get("error").is_none());
    let claimed = resp2["result"]["claimed"].as_array().unwrap();
    assert_eq!(claimed.len(), 2);
    assert_eq!(resp2["result"]["rejected"].as_array().unwrap().len(), 0);

    // Both new reservations are persisted.
    for c in claimed {
        let id = c["reservation_id"].as_str().unwrap();
        assert!(db::Store::global().get_reservation(id)?.is_some());
    }

    shutdown_tx.send(true).ok();
    let _ = timeout(Duration::from_secs(2), listener_task).await;
    Ok(())
}

// ---------------------------------------------------------------------
// FR-4: pattern_claim blocks literal claims that match the pattern.
// ---------------------------------------------------------------------
#[tokio::test]
async fn m2_fr4_pattern_claim_blocks_matching_literals() -> Result<()> {
    let dir = TempDir::new()?;
    let (_cfg, listener_task, shutdown_tx) = boot_daemon(dir.path()).await?;
    let stream = UnixStream::connect(dir.path().join("resv.sock")).await?;
    let mut client = Client::new(stream);

    let sid_a = register_session(&mut client, 7030).await?;
    let sid_b = register_session(&mut client, 7031).await?;

    // Sid_a claims the pattern.
    let r1 = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 2, "method": "reservation.pattern_claim",
            "params": {
                "session_id": sid_a,
                "path": "/repo/src/*.rs",
                "mode": "exclusive"
            }
        }))
        .await?;
    assert!(
        r1.get("error").is_none(),
        "pattern_claim must succeed: {r1}"
    );
    assert!(!r1["result"]["reservation"].is_null());
    assert_eq!(r1["result"]["conflicts"].as_array().unwrap().len(), 0);

    // Sid_b tries to claim a literal under the pattern.
    let r2 = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 3, "method": "reservation.claim",
            "params": {
                "session_id": sid_b,
                "path": "/repo/src/lib.rs",
                "mode": "write"
            }
        }))
        .await?;
    let result = &r2["result"];
    assert!(result["reservation"].is_null());
    let conflicts = result["conflicts"].as_array().unwrap();
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0]["reason"], "existing_pattern_covers");

    shutdown_tx.send(true).ok();
    let _ = timeout(Duration::from_secs(2), listener_task).await;
    Ok(())
}

// ---------------------------------------------------------------------
// FR-4b: pattern_claim without a glob is rejected with InvalidParams.
// ---------------------------------------------------------------------
#[tokio::test]
async fn m2_fr4_pattern_claim_requires_glob() -> Result<()> {
    let dir = TempDir::new()?;
    let (_cfg, listener_task, shutdown_tx) = boot_daemon(dir.path()).await?;
    let stream = UnixStream::connect(dir.path().join("resv.sock")).await?;
    let mut client = Client::new(stream);
    let sid = register_session(&mut client, 7040).await?;

    let resp = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 2, "method": "reservation.pattern_claim",
            "params": {
                "session_id": sid,
                "path": "/repo/src/lib.rs",  // no glob
                "mode": "write"
            }
        }))
        .await?;
    let err = resp.get("error").expect("error present");
    assert_eq!(err["code"], -32602);

    shutdown_tx.send(true).ok();
    let _ = timeout(Duration::from_secs(2), listener_task).await;
    Ok(())
}

// ---------------------------------------------------------------------
// FR-5: conflicts_for_path is read-only.
// ---------------------------------------------------------------------
#[tokio::test]
async fn m2_fr5_conflicts_for_path_does_not_modify_state() -> Result<()> {
    let dir = TempDir::new()?;
    let (_cfg, listener_task, shutdown_tx) = boot_daemon(dir.path()).await?;
    let stream = UnixStream::connect(dir.path().join("resv.sock")).await?;
    let mut client = Client::new(stream);

    let sid = register_session(&mut client, 7050).await?;

    // Probe before any claim: empty.
    let r1 = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 2, "method": "reservation.conflicts_for_path",
            "params": { "path": "/repo/x.rs", "mode": "write" }
        }))
        .await?;
    assert!(r1.get("error").is_none());
    assert_eq!(r1["result"]["conflicts"].as_array().unwrap().len(), 0);

    // Claim the path.
    let r2 = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 3, "method": "reservation.claim",
            "params": {
                "session_id": sid, "path": "/repo/x.rs", "mode": "write"
            }
        }))
        .await?;
    assert!(r2.get("error").is_none());

    // Probe again: now sees the reservation.
    let r3 = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 4, "method": "reservation.conflicts_for_path",
            "params": { "path": "/repo/x.rs", "mode": "write" }
        }))
        .await?;
    assert!(r3.get("error").is_none());
    let conflicts = r3["result"]["conflicts"].as_array().unwrap();
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0]["reason"], "exact_match");

    // Probe once more: still just the one conflict (no side effects).
    let r4 = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 5, "method": "reservation.conflicts_for_path",
            "params": { "path": "/repo/x.rs", "mode": "write" }
        }))
        .await?;
    assert_eq!(r4["result"]["conflicts"].as_array().unwrap().len(), 1);

    shutdown_tx.send(true).ok();
    let _ = timeout(Duration::from_secs(2), listener_task).await;
    Ok(())
}

// ---------------------------------------------------------------------
// FR-6: list_conflicts reports the live overlap set.
//
// In normal operation the daemon rejects overlapping claims at insert
// time, so the live store has no overlaps and list_conflicts returns
// `pairs: []`. The interesting diagnostic case (overlaps that survived
// the writer's checks) is exercised as a unit test against
// `teamcomm_daemon::conflict::detect_conflicts`.
// ---------------------------------------------------------------------
#[tokio::test]
async fn m2_fr6_list_conflicts_returns_empty_when_no_overlaps() -> Result<()> {
    let dir = TempDir::new()?;
    let (_cfg, listener_task, shutdown_tx) = boot_daemon(dir.path()).await?;
    let stream = UnixStream::connect(dir.path().join("resv.sock")).await?;
    let mut client = Client::new(stream);

    let sid_a = register_session(&mut client, 7060).await?;
    let sid_b = register_session(&mut client, 7061).await?;

    // Two non-overlapping reservations: different paths, different files.
    let _ = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 2, "method": "reservation.claim",
            "params": {
                "session_id": sid_a, "path": "/repo/src/lib.rs", "mode": "write"
            }
        }))
        .await?;
    let _ = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 3, "method": "reservation.claim",
            "params": {
                "session_id": sid_b, "path": "/repo/tests/lib.rs", "mode": "write"
            }
        }))
        .await?;

    let resp = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 4, "method": "reservation.list_conflicts",
            "params": {}
        }))
        .await?;
    assert!(resp.get("error").is_none());
    let pairs = resp["result"]["pairs"].as_array().unwrap();
    assert!(
        pairs.is_empty(),
        "expected no overlaps in a healthy system; got: {pairs:?}"
    );

    shutdown_tx.send(true).ok();
    let _ = timeout(Duration::from_secs(2), listener_task).await;
    Ok(())
}

// ---------------------------------------------------------------------
// FR-7: release removes both in-memory and durable state.
// ---------------------------------------------------------------------
#[tokio::test]
async fn m2_fr7_release_removes_in_memory_and_durable_state() -> Result<()> {
    let dir = TempDir::new()?;
    let (_cfg, listener_task, shutdown_tx) = boot_daemon(dir.path()).await?;
    let stream = UnixStream::connect(dir.path().join("resv.sock")).await?;
    let mut client = Client::new(stream);

    let sid = register_session(&mut client, 7070).await?;

    // Claim.
    let resp = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 2, "method": "reservation.claim",
            "params": {
                "session_id": sid, "path": "/repo/release_me.rs", "mode": "write"
            }
        }))
        .await?;
    let reservation_id = resp["result"]["reservation"]["reservation_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(db::Store::global()
        .get_reservation(&reservation_id)?
        .is_some());

    // Release.
    let resp = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 3, "method": "reservation.release",
            "params": { "reservation_id": reservation_id }
        }))
        .await?;
    assert!(resp.get("error").is_none());
    assert_eq!(resp["result"]["ok"], json!(true));

    // Durable row removed.
    assert!(db::Store::global()
        .get_reservation(&reservation_id)?
        .is_none());

    // A new claim on the same path succeeds (proving in-memory cleared too).
    let resp = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 4, "method": "reservation.claim",
            "params": {
                "session_id": sid, "path": "/repo/release_me.rs", "mode": "write"
            }
        }))
        .await?;
    assert_eq!(resp["result"]["conflicts"].as_array().unwrap().len(), 0);

    shutdown_tx.send(true).ok();
    let _ = timeout(Duration::from_secs(2), listener_task).await;
    Ok(())
}

// ---------------------------------------------------------------------
// FR-8: claim_many with one path blocked — none of the others are claimed.
// ---------------------------------------------------------------------
#[tokio::test]
async fn m2_fr8_claim_many_rolls_back_under_partial_conflict() -> Result<()> {
    let dir = TempDir::new()?;
    let (_cfg, listener_task, shutdown_tx) = boot_daemon(dir.path()).await?;
    let stream = UnixStream::connect(dir.path().join("resv.sock")).await?;
    let mut client = Client::new(stream);

    let sid_a = register_session(&mut client, 7080).await?;
    let sid_b = register_session(&mut client, 7081).await?;

    // Sid_a takes /repo/x.rs
    let _ = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 2, "method": "reservation.claim",
            "params": {
                "session_id": sid_a, "path": "/repo/x.rs", "mode": "write"
            }
        }))
        .await?;

    // Sid_b asks for [free_a, x.rs, free_b] — must fail entirely.
    let resp = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 3, "method": "reservation.claim_many",
            "params": {
                "session_id": sid_b,
                "paths": ["/repo/free_a.rs", "/repo/x.rs", "/repo/free_b.rs"],
                "mode": "write"
            }
        }))
        .await?;
    assert!(resp.get("error").is_none());
    assert_eq!(resp["result"]["claimed"].as_array().unwrap().len(), 0);
    let rejected = resp["result"]["rejected"].as_array().unwrap();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0]["path"], "/repo/x.rs");

    // Neither /repo/free_a.rs nor /repo/free_b.rs should be reserved.
    for free in ["/repo/free_a.rs", "/repo/free_b.rs"] {
        let r = client
            .round_trip(json!({
                "jsonrpc": "2.0", "id": 4, "method": "reservation.claim",
                "params": {
                    "session_id": sid_b, "path": free, "mode": "write"
                }
            }))
            .await?;
        assert_eq!(
            r["result"]["conflicts"].as_array().unwrap().len(),
            0,
            "expected {free} to be free, got: {r}"
        );
    }

    shutdown_tx.send(true).ok();
    let _ = timeout(Duration::from_secs(2), listener_task).await;
    Ok(())
}

// ---------------------------------------------------------------------
// FR-9: read mode does not block other reads but blocks exclusive.
// ---------------------------------------------------------------------
#[tokio::test]
async fn m2_fr9_mode_isolation() -> Result<()> {
    let dir = TempDir::new()?;
    let (_cfg, listener_task, shutdown_tx) = boot_daemon(dir.path()).await?;
    let stream = UnixStream::connect(dir.path().join("resv.sock")).await?;
    let mut client = Client::new(stream);

    let sid_a = register_session(&mut client, 7090).await?;
    let sid_b = register_session(&mut client, 7091).await?;
    let sid_c = register_session(&mut client, 7092).await?;

    // Two reads on the same path — both succeed.
    for sid in [&sid_a, &sid_b] {
        let r = client
            .round_trip(json!({
                "jsonrpc": "2.0", "id": 2, "method": "reservation.claim",
                "params": {
                    "session_id": sid, "path": "/repo/shared.rs", "mode": "read"
                }
            }))
            .await?;
        assert_eq!(r["result"]["conflicts"].as_array().unwrap().len(), 0);
    }

    // Exclusive claim from sid_c is rejected.
    let r = client
        .round_trip(json!({
            "jsonrpc": "2.0", "id": 3, "method": "reservation.claim",
            "params": {
                "session_id": sid_c, "path": "/repo/shared.rs", "mode": "exclusive"
            }
        }))
        .await?;
    let conflicts = r["result"]["conflicts"].as_array().unwrap();
    assert_eq!(conflicts.len(), 2, "both reads must block exclusive");

    shutdown_tx.send(true).ok();
    let _ = timeout(Duration::from_secs(2), listener_task).await;
    Ok(())
}
